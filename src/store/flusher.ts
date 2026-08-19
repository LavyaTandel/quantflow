import type { NormalizedEvent } from '../ingest/types.js';
import type { TickBuffer } from './buffer.js';
import { toDoc, type TickDoc } from './doc.js';
import type { Sink } from './sink.js';

export interface FlushWhen {
  maxDocs: number;
  maxLatencyMs: number;
}

export const DEFAULT_FLUSH_WHEN: Readonly<FlushWhen> = { maxDocs: 1000, maxLatencyMs: 200 };
export const DEFAULT_MAX_IN_FLIGHT = 2;
const MAX_LATENCY_SAMPLES = 4096;
const CLOSE_MAX_ATTEMPTS_PER_BATCH = 3;

export interface Scheduler {
  setInterval(fn: () => void, ms: number): unknown;
  clearInterval(handle: unknown): void;
  now(): number;
}

// Jitter-free: one repeating interval (not self-scheduling setTimeout chains).
// Works under vitest fake timers because it binds globals at call time.
export const nodeScheduler: Scheduler = {
  setInterval: (fn, ms) => globalThis.setInterval(fn, ms),
  clearInterval: (handle) => globalThis.clearInterval(handle as ReturnType<typeof setInterval>),
  now: () => Date.now(),
};

export interface FlushServiceOptions {
  sink: Sink;
  buffer: TickBuffer<NormalizedEvent>;
  scheduler?: Scheduler;
  flushWhen?: Partial<FlushWhen>;
  maxInFlight?: number;
}

export interface FlushLatencyStats {
  samples: number;
  p50: number;
  p99: number;
  max: number;
}

export interface FlushStats {
  flushesStarted: number;
  flushesSucceeded: number;
  flushesFailed: number;
  docsWritten: number;
  errors: number;
  lastErrorMessage?: string;
  batchesRetried: number;
  unwritten: number;
  droppedOldest: number;
  refusedDepth: number;
  bufferDepth: number;
  inFlight: number;
  closed: boolean;
  latencyMs: FlushLatencyStats;
}

interface RetryEntry {
  seq: number;
  docs: TickDoc[];
  attempts: number;
}

function validatePositiveInt(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer, got ${value}`);
  }
}

function percentile(sorted: readonly number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx] as number;
}

export class FlushService {
  readonly flushWhen: Readonly<FlushWhen>;
  readonly maxInFlight: number;

  private readonly sink: Sink;
  private readonly buffer: TickBuffer<NormalizedEvent>;
  private readonly scheduler: Scheduler;
  private timer: unknown = null;
  private closed = false;
  private closing = false;
  private inflight = 0;
  private seqCounter = 0;
  private readonly retries: RetryEntry[] = [];
  private readonly settleWaiters: Array<() => void> = [];
  private readonly latencySamples: number[] = [];

  private flushesStarted = 0;
  private flushesSucceeded = 0;
  private flushesFailed = 0;
  private docsWritten = 0;
  private errors = 0;
  private lastErrorMessage?: string;
  private batchesRetried = 0;
  private unwrittenDocs = 0;

  constructor(opts: FlushServiceOptions) {
    this.flushWhen = {
      maxDocs: opts.flushWhen?.maxDocs ?? DEFAULT_FLUSH_WHEN.maxDocs,
      maxLatencyMs: opts.flushWhen?.maxLatencyMs ?? DEFAULT_FLUSH_WHEN.maxLatencyMs,
    };
    this.maxInFlight = opts.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT;
    validatePositiveInt('flushWhen.maxDocs', this.flushWhen.maxDocs);
    validatePositiveInt('flushWhen.maxLatencyMs', this.flushWhen.maxLatencyMs);
    validatePositiveInt('maxInFlight', this.maxInFlight);
    this.sink = opts.sink;
    this.buffer = opts.buffer;
    this.scheduler = opts.scheduler ?? nodeScheduler;
  }

  start(): void {
    if (this.closed) throw new Error('FlushService is closed');
    if (this.timer !== null) return;
    this.timer = this.scheduler.setInterval(() => this.pump(), this.flushWhen.maxLatencyMs);
  }

  stop(): void {
    if (this.timer === null) return;
    this.scheduler.clearInterval(this.timer);
    this.timer = null;
  }

  // Interval guarantees the staleness bound (<= maxLatencyMs); completion-chained
  // pumping keeps maxInFlight writes in the air so sustained throughput is
  // maxInFlight * maxDocs per flush round-trip, not per interval.
  pump(): void {
    if (this.closed || this.closing) return;
    while (this.inflight < this.maxInFlight) {
      if (this.retries.length > 0) {
        this.launchRetry();
        continue;
      }
      const events = this.buffer.drainUpTo(this.flushWhen.maxDocs);
      if (events.length === 0) break;
      this.launch(events.map(toDoc), ++this.seqCounter, 1);
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.stop();
    this.closing = true;
    while (this.inflight > 0 || this.buffer.size() > 0 || this.retries.length > 0) {
      if (this.inflight === 0) {
        const retryable = this.retries.find((r) => r.attempts < CLOSE_MAX_ATTEMPTS_PER_BATCH);
        if (retryable) {
          this.retries.splice(this.retries.indexOf(retryable), 1);
          retryable.attempts += 1;
          this.launch(retryable.docs, retryable.seq, retryable.attempts);
        } else if (this.buffer.size() > 0 && this.retries.length === 0) {
          const events = this.buffer.drainUpTo(this.flushWhen.maxDocs);
          this.launch(events.map(toDoc), ++this.seqCounter, 1);
        } else {
          break;
        }
      }
      await new Promise<void>((resolve) => {
        this.settleWaiters.push(resolve);
      });
    }
    for (const r of this.retries) this.unwrittenDocs += r.docs.length;
    this.retries.length = 0;
    this.closed = true;
    if (this.unwrittenDocs > 0) {
      const message = `close(): ${this.unwrittenDocs} docs could not be written after ${CLOSE_MAX_ATTEMPTS_PER_BATCH} attempts each; last error: ${this.lastErrorMessage ?? 'unknown'}`;
      console.error(`[store] ${message}`);
      throw new Error(message);
    }
  }

  stats(): FlushStats {
    const bufferCounters = this.buffer.counters();
    const sorted = [...this.latencySamples].sort((a, b) => a - b);
    return {
      flushesStarted: this.flushesStarted,
      flushesSucceeded: this.flushesSucceeded,
      flushesFailed: this.flushesFailed,
      docsWritten: this.docsWritten,
      errors: this.errors,
      lastErrorMessage: this.lastErrorMessage,
      batchesRetried: this.batchesRetried,
      unwritten: this.unwrittenDocs,
      droppedOldest: bufferCounters.droppedOldest,
      refusedDepth: bufferCounters.refusedDepth,
      bufferDepth: bufferCounters.size,
      inFlight: this.inflight,
      closed: this.closed,
      latencyMs: {
        samples: sorted.length,
        p50: percentile(sorted, 50),
        p99: percentile(sorted, 99),
        max: sorted.length > 0 ? (sorted[sorted.length - 1] as number) : 0,
      },
    };
  }

  bufferSize(): number {
    return this.buffer.size();
  }

  private launchRetry(): void {
    const entry = this.retries.shift();
    if (!entry) return;
    this.batchesRetried += 1;
    entry.attempts += 1;
    this.launch(entry.docs, entry.seq, entry.attempts);
  }

  // attempts = times this batch has been handed to the sink so far; preserved
  // across failures so close() can give up after CLOSE_MAX_ATTEMPTS_PER_BATCH.
  private launch(docs: TickDoc[], seq: number, attempts: number): void {
    this.inflight += 1;
    this.flushesStarted += 1;
    const startedAt = this.scheduler.now();
    void this.sink
      .insertMany(docs)
      .then(
        () => {
          this.docsWritten += docs.length;
          this.flushesSucceeded += 1;
          this.recordLatency(this.scheduler.now() - startedAt);
        },
        (err: unknown) => {
          this.errors += 1;
          this.flushesFailed += 1;
          this.lastErrorMessage = err instanceof Error ? err.message : String(err);
          this.insertRetry({ seq, docs, attempts });
        },
      )
      .finally(() => {
        this.inflight -= 1;
        if (this.closing || this.inflight === 0) this.notifySettled();
        if (!this.closing && !this.closed) this.pump();
      });
  }

  private insertRetry(entry: RetryEntry): void {
    let i = this.retries.findIndex((r) => r.seq > entry.seq);
    if (i === -1) i = this.retries.length;
    this.retries.splice(i, 0, entry);
  }

  private recordLatency(ms: number): void {
    this.latencySamples.push(Math.max(0, ms));
    if (this.latencySamples.length > MAX_LATENCY_SAMPLES) {
      this.latencySamples.splice(0, this.latencySamples.length - MAX_LATENCY_SAMPLES);
    }
  }

  private notifySettled(): void {
    while (this.settleWaiters.length > 0) {
      const wake = this.settleWaiters.shift();
      wake?.();
    }
  }
}
