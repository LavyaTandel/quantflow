import { describe, expect, it, vi } from 'vitest';
import type { NormalizedEvent } from '../../src/ingest/types.js';
import { TickBuffer } from '../../src/store/buffer.js';
import {
  DEFAULT_FLUSH_WHEN,
  DEFAULT_MAX_IN_FLIGHT,
  FlushService,
  nodeScheduler,
  type Scheduler,
} from '../../src/store/flusher.js';
import type { TickDoc } from '../../src/store/doc.js';
import type { Sink } from '../../src/store/sink.js';
import { mkBurst, mkBbo, mkDepth, mkTrade, settle } from './helpers.js';

function pushAll(buffer: TickBuffer<NormalizedEvent>, events: readonly NormalizedEvent[]): void {
  for (const event of events) buffer.push(event);
}

type Outcome = 'ok' | 'fail' | 'hold';

class ScriptedSink implements Sink {
  readonly calls: TickDoc[][] = [];
  maxObservedConcurrency = 0;
  private live = 0;
  private held: Array<{ res: () => void; rej: (err: Error) => void }> = [];

  constructor(private outcomes: Outcome[] = []) {}

  insertMany(docs: readonly TickDoc[]): Promise<void> {
    this.calls.push([...docs]);
    this.live += 1;
    this.maxObservedConcurrency = Math.max(this.maxObservedConcurrency, this.live);
    const outcome = this.outcomes.shift() ?? 'ok';
    return new Promise((resolve, reject) => {
      const finish = (ok: boolean) => {
        this.live -= 1;
        if (ok) resolve();
        else reject(new Error('sink boom'));
      };
      if (outcome === 'hold') {
        this.held.push({ res: () => finish(true), rej: () => finish(false) });
      } else {
        queueMicrotask(() => finish(outcome === 'ok'));
      }
    });
  }

  get heldCount(): number {
    return this.held.length;
  }

  releaseHeld(ok = true): void {
    while (this.held.length > 0) {
      const gate = this.held.shift();
      if (!gate) break;
      if (ok) gate.res();
      else gate.rej(new Error('sink boom'));
    }
  }
}

class ManualScheduler implements Scheduler {
  private tasks: Array<(() => void) | null> = [];
  private clock = 0;

  setInterval(fn: () => void, _ms: number): unknown {
    this.tasks.push(fn);
    return this.tasks.length - 1;
  }

  clearInterval(handle: unknown): void {
    if (typeof handle === 'number') this.tasks[handle] = null;
  }

  now(): number {
    this.clock += 1;
    return this.clock;
  }

  fire(times = 1): void {
    for (let i = 0; i < times; i++) {
      for (const task of [...this.tasks]) task?.();
    }
  }
}

function assertMonotonePerSymbol(docs: readonly TickDoc[]): void {
  const lastTsBySymbol = new Map<string, number>();
  for (const doc of docs) {
    const last = lastTsBySymbol.get(doc.symbol);
    expect(doc.ts.getTime()).toBeGreaterThanOrEqual(last ?? 0);
    lastTsBySymbol.set(doc.symbol, doc.ts.getTime());
  }
}

function makeService(
  sink: Sink,
  buffer: TickBuffer<NormalizedEvent>,
  scheduler: ManualScheduler,
  overrides: Partial<{ maxInFlight: number; maxDocs: number; maxLatencyMs: number }> = {},
): FlushService {
  return new FlushService({
    sink,
    buffer,
    scheduler,
    flushWhen: { maxDocs: overrides.maxDocs ?? 1000, maxLatencyMs: overrides.maxLatencyMs ?? 200 },
    maxInFlight: overrides.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT,
  });
}

describe('FlushService cadence', () => {
  it('flushes at most maxDocs per flush and completion-pumps the remainder', async () => {
    const sink = new ScriptedSink();
    const buffer = new TickBuffer({ capacity: 10_000 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);

    pushAll(buffer, mkBurst(2_500));
    service.start();
    scheduler.fire();
    await settle();

    const sizes = sink.calls.map((c) => c.length);
    expect(sizes).toEqual([1_000, 1_000, 500]);
    expect(service.stats().docsWritten).toBe(2_500);
    expect(buffer.size()).toBe(0);
    for (const call of sink.calls) assertMonotonePerSymbol(call);
  });

  it('caps observable in-flight flushes at maxInFlight', async () => {
    const sink = new ScriptedSink(['hold', 'hold', 'hold']);
    const buffer = new TickBuffer({ capacity: 10_000 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);

    pushAll(buffer, mkBurst(3_000));
    service.start();
    scheduler.fire(2);
    await settle();

    expect(sink.calls).toHaveLength(2);
    expect(sink.maxObservedConcurrency).toBe(DEFAULT_MAX_IN_FLIGHT);

    sink.releaseHeld();
    await settle();
    sink.releaseHeld();
    await settle();
    expect(sink.calls.map((c) => c.length)).toEqual([1_000, 1_000, 1_000]);
    expect(sink.maxObservedConcurrency).toBeLessThanOrEqual(2);
    expect(service.stats().docsWritten).toBe(3_000);
  });

  it('fires a partial flush on the latency timer with fake timers + nodeScheduler', async () => {
    vi.useFakeTimers();
    try {
      const sink = new ScriptedSink();
      const buffer = new TickBuffer({ capacity: 100 });
      const service = new FlushService({ sink, buffer, scheduler: nodeScheduler });
      pushAll(buffer, mkBurst(5));
      service.start();
      await vi.advanceTimersByTimeAsync(DEFAULT_FLUSH_WHEN.maxLatencyMs);
      expect(sink.calls).toHaveLength(1);
      expect(sink.calls[0]?.length).toBe(5);
      service.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps per-flush docs monotone by ts across many bursts', async () => {
    const sink = new ScriptedSink();
    const buffer = new TickBuffer({ capacity: 50_000 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);
    service.start();

    for (let round = 0; round < 4; round++) {
      pushAll(buffer, mkBurst(900));
      scheduler.fire();
      await settle();
    }
    expect(service.stats().docsWritten).toBe(3_600);
    for (const call of sink.calls) {
      expect(call.length).toBeLessThanOrEqual(1_000);
      assertMonotonePerSymbol(call);
    }
  });
});

describe('FlushService failure handling', () => {
  it('counts errors and retries the failed batch before fresh drains, in order', async () => {
    const sink = new ScriptedSink(['fail', 'hold', 'hold']);
    const buffer = new TickBuffer({ capacity: 10_000 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);

    const batchA = [mkTrade(), mkDepth()];
    for (const event of batchA) buffer.push(event);
    service.start();
    scheduler.fire();
    const batchB = mkBurst(2);
    for (const event of batchB) buffer.push(event);
    await settle();

    expect(sink.calls).toHaveLength(3);
    expect(sink.calls[0]).toHaveLength(2);
    expect(sink.calls[1]).toHaveLength(2);
    expect(sink.calls[2]).toHaveLength(2);
    const attemptTs = sink.calls[0]?.map((d) => d.ts.getTime());
    expect(sink.calls[1]?.map((d) => d.ts.getTime())).toEqual(attemptTs);
    assertMonotonePerSymbol(sink.calls.slice(1).flat());

    sink.releaseHeld();
    await settle();
    const stats = service.stats();
    expect(stats.errors).toBe(1);
    expect(stats.batchesRetried).toBeGreaterThanOrEqual(1);
    expect(stats.docsWritten).toBe(4);
    expect(stats.lastErrorMessage).toContain('sink boom');
  });

  it('drains everything on graceful close', async () => {
    const sink = new ScriptedSink(['hold', 'hold']);
    const buffer = new TickBuffer({ capacity: 10_000 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);

    pushAll(buffer, mkBurst(1_200));
    service.start();
    const closing = service.close();
    scheduler.fire();
    sink.releaseHeld();
    await settle();
    sink.releaseHeld();
    await closing;

    const stats = service.stats();
    expect(stats.closed).toBe(true);
    expect(stats.docsWritten).toBe(1_200);
    expect(buffer.size()).toBe(0);
  });

  it('close() without start() still flushes buffered docs', async () => {
    const sink = new ScriptedSink();
    const buffer = new TickBuffer({ capacity: 100 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);

    buffer.push(mkTrade());
    buffer.push(mkBbo());
    await service.close();
    expect(sink.calls.map((c) => c.length)).toEqual([2]);
    expect(service.stats().docsWritten).toBe(2);
    scheduler.fire();
    await settle();
    expect(sink.calls).toHaveLength(1);
  });

  it('surfaces unwritten docs loudly when the sink keeps failing through close()', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const sink = new ScriptedSink(Array.from({ length: 20 }, () => 'fail' as Outcome));
    const buffer = new TickBuffer({ capacity: 100 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);

    buffer.push(mkTrade());
    buffer.push(mkBbo());
    buffer.push(mkDepth());
    await expect(service.close()).rejects.toThrow(/3 docs could not be written/);
    expect(errorSpy).toHaveBeenCalled();
    expect(service.stats().unwritten).toBe(3);
    errorSpy.mockRestore();
  });
});

describe('FlushService config and counters', () => {
  it('rejects invalid configuration', () => {
    const buffer = new TickBuffer({ capacity: 10 });
    const sink = new ScriptedSink();
    const base = { sink, buffer, scheduler: new ManualScheduler() };
    expect(
      () => new FlushService({ ...base, flushWhen: { maxDocs: 0, maxLatencyMs: 200 } }),
    ).toThrow(RangeError);
    expect(
      () => new FlushService({ ...base, flushWhen: { maxDocs: 10, maxLatencyMs: -1 } }),
    ).toThrow(RangeError);
    expect(() => new FlushService({ ...base, maxInFlight: 0 })).toThrow(RangeError);
    expect(() => new FlushService({ ...base, maxInFlight: 2.5 })).toThrow(RangeError);
  });

  it('start() after close throws, double start is idempotent', async () => {
    const sink = new ScriptedSink();
    const buffer = new TickBuffer({ capacity: 10 });
    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);
    service.start();
    service.start();
    await service.close();
    expect(() => service.start()).toThrow('closed');
  });

  it('propagates loud-drop counters from the buffer into stats', () => {
    const sink = new ScriptedSink();
    const buffer = new TickBuffer<NormalizedEvent>({ capacity: 2 });
    buffer.push(mkDepth());
    buffer.push(mkDepth());
    expect(buffer.push(mkTrade())).toBe(false);
    expect(buffer.push(mkDepth())).toBe(false);

    const scheduler = new ManualScheduler();
    const service = makeService(sink, buffer, scheduler);
    const stats = service.stats();
    expect(stats.refusedDepth).toBe(1);
    expect(stats.droppedOldest).toBe(0);
    expect(stats.bufferDepth).toBe(2);
  });
});
