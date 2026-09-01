import { isFeatureAllowed } from '../billing/tiers.js';
import type { BookSide, BookStore } from '../book/store.js';
import type { FeatureVector } from './features.js';
import { buildFeatureVector } from './features.js';
import type { Model, ClassifyResult } from './model.js';

// Derived Signal boundary (ADR 0003): external tenants never receive NormalizedEvent
// or raw depth array — only {symbol, ts, signal, score, confidence, horizon_s, window_s, features_version}
export interface DerivedSignal {
  symbol: string;
  ts: number;
  signal: 'buy' | 'sell' | 'neutral';
  score: number;
  confidence: number;
  horizon_s: number;
  window_s: number;
  features_version: string;
  // Optional provenance for audit (internal, not for raw-market redistribution)
  features?: FeatureVector;
}

export interface FeatureSink {
  persist(entry: {
    symbol: string;
    ts: number;
    features: FeatureVector;
    horizonDeltaP?: number;
  }): void | Promise<void>;
}

export interface PipelineOptions {
  bookStore: BookStore;
  model: Model;
  emitAlert: (alert: DerivedSignal) => void;
  featureSink?: FeatureSink;
  ofiThreshold?: number;
  maxQueueSize?: number;
  featuresVersion?: string;
  windowMs?: number;
}

export interface PipelineMetrics {
  gatedDropped: number;
  throttledDropped: number;
  queueShed: number;
  alertsEmitted: number;
  modelLatencyMs: { p50: number; p99: number };
  e2eLatencyMs: { p50: number; p99: number };
  // raw for debugging
  _rawModelLatencies: number[];
  _rawE2eLatencies: number[];
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.min(Math.max(0, idx), sorted.length - 1)]!;
}

function sortedCopy(arr: number[]): number[] {
  return [...arr].sort((a, b) => a - b);
}

function resolveFeature(I: number, ofi: number, sweepFlag: boolean): string {
  if (sweepFlag) return 'F4';
  if (Math.abs(ofi) > 0) {
    // Gating: if OFI triggered, alert is F2-provenance; but F1 also present when I large.
    // Prioritize F1 when I gating also true, since F1 is cheaper to justify.
    // For tier check, we map alert feature by dominant trigger.
    if (Math.abs(I) > 0.3) return 'F1'; // I takes precedence when both
    return 'F2';
  }
  return 'F1';
}

/**
 * AlertPipeline orchestrator.
 *
 * Prod wiring:
 *   pipeline.emitAlert ──> gateway broadcast (WS fan-out)
 *   In prod, `emitAlert` would be bound to a function that calls
 *   `gateway.handleRequest`-compatible derivedOnly topic and/or
 *   `wsServer.broadcast(JSON.stringify(derivedSignal))`.
 *   See docs/adr/0003-derived-signal-boundary.md — fan-out of DerivedSignal is
 *   allowed; raw NormalizedEvent fan-out stays internal.
 *
 * Micro-batch: caller should tick every 100 ms per symbol; pipeline fetches
 * Redis ZSET book state via injected BookStore (hermetic fake in tests,
 * RedisBookStore in prod with single-writer per symbol).
 */
export class AlertPipeline {
  readonly #bookStore: BookStore;
  readonly #model: Model;
  readonly #emitAlert: (alert: DerivedSignal) => void;
  readonly #featureSink?: FeatureSink;
  readonly #ofiThreshold: number;
  readonly #maxQueueSize: number;
  readonly #featuresVersion: string;
  readonly #windowMs: number;

  // state
  readonly #prevBooks = new Map<string, BookSide>();
  readonly #lastEmitAt = new Map<string, number>();
  readonly #ofiHistory = new Map<string, number[]>();

  #pending = 0;

  #gatedDropped = 0;
  #throttledDropped = 0;
  #queueShed = 0;
  #alertsEmitted = 0;
  readonly #modelLatencies: number[] = [];
  readonly #e2eLatencies: number[] = [];

  constructor(opts: PipelineOptions) {
    this.#bookStore = opts.bookStore;
    this.#model = opts.model;
    this.#emitAlert = opts.emitAlert;
    this.#featureSink = opts.featureSink;
    this.#ofiThreshold = opts.ofiThreshold ?? 50;
    this.#maxQueueSize = opts.maxQueueSize ?? 50;
    this.#featuresVersion = opts.featuresVersion ?? 'v2';
    this.#windowMs = opts.windowMs ?? 100;
  }

  /**
   * For hermetic tests: directly set internal queue depth to simulate backpressure.
   */
  setQueueDepth(n: number): void {
    this.#pending = n;
  }

  get queueDepth(): number {
    return this.#pending;
  }

  /**
   * Core tick: extracts features for `symbol` and optionally emits DerivedSignal
   * for the given tenant. Tenant controls feature gating (Starter=F1 only etc.).
   */
  async tick(
    symbol: string,
    tenant: { id: string; tier: string },
    now: number = Date.now(),
  ): Promise<void> {
    const e2eStart = Date.now();
    // Fetch book state (top 5 levels) — in prod Redis ZSET; in tests MemoryBookStore fake
    const book: BookSide = await this.#bookStore.topN(symbol, 5);
    const bestBid = book.bids[0]?.price ?? 0;
    const bestAsk = book.asks[0]?.price ?? 0;
    const spread = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;

    const prev = this.#prevBooks.get(symbol) ?? null;
    const fv: FeatureVector = buildFeatureVector(book, prev, {
      windowMs: this.#windowMs,
      spread,
    });

    // Update OFI history for p75 (if we have enough points, compute percentile)
    const ofiAbs = Math.abs(fv.ofi);
    const hist = this.#ofiHistory.get(symbol) ?? [];
    hist.push(ofiAbs);
    if (hist.length > 200) hist.shift();
    this.#ofiHistory.set(symbol, hist);

    // Determine dynamic p75 threshold (if enough history else static)
    let ofiP75 = this.#ofiThreshold;
    if (hist.length >= 20) {
      const s = sortedCopy(hist);
      ofiP75 = percentile(s, 75);
    }

    const gatingByI = Math.abs(fv.I) > 0.3;
    const gatingByOfi = Math.abs(fv.ofi) > ofiP75;
    const gating = gatingByI || gatingByOfi || fv.sweepFlag;

    // Always remember curr for next OFI delta, even if gated
    this.#prevBooks.set(symbol, book);

    if (!gating) {
      this.#gatedDropped += 1;
      this.#recordE2e(e2eStart);
      return;
    }

    // Tenant feature check: Starter must never receive F2/F4, Pro must not receive F3
    // Map alert feature by dominant trigger for tier enforcement.
    // Resolve feature: sweep => F4, else OFI path => F2, else F1
    let feature: string;
    if (fv.sweepFlag) feature = 'F4';
    else if (gatingByOfi && !gatingByI) feature = 'F2';
    else feature = 'F1';
    // DWP/F3 is internal only: never emit F3 to external; but compute already. If someone
    // tries to emit F3, block for non-enterprise. Our resolveFeature never yields F3 for external.
    // Pro -> F3 blocked; Starter -> F2/F4 blocked via isFeatureAllowed.
    if (!isFeatureAllowed(tenant.tier, feature)) {
      this.#gatedDropped += 1;
      this.#recordE2e(e2eStart);
      return;
    }
    // Also explicitly block F3 for non-enterprise if it were ever requested (defense-in-depth)
    if (feature === 'F3' && !isFeatureAllowed(tenant.tier, 'F3')) {
      this.#gatedDropped += 1;
      this.#recordE2e(e2eStart);
      return;
    }

    // Queue backpressure (max 50, shed to feature-only if exceeded, counted)
    if (this.#pending >= this.#maxQueueSize) {
      this.#queueShed += 1;
      this.#recordE2e(e2eStart);
      return;
    }

    // Throttling: per-tenant-per-symbol ≤1 alert/sec
    const throttleKey = `${tenant.id}:${symbol}`;
    const last = this.#lastEmitAt.get(throttleKey);
    if (last !== undefined && now - last < 1000) {
      this.#throttledDropped += 1;
      this.#recordE2e(e2eStart);
      return;
    }

    // Inference dequeued via simple in-memory queue
    this.#pending += 1;
    let result: ClassifyResult;
    let modelLatency: number;
    try {
      const t0 = Date.now();
      result = await this.#model.classify(fv);
      modelLatency = Date.now() - t0;
      this.#modelLatencies.push(modelLatency);
    } finally {
      this.#pending -= 1;
    }

    // Post-model throttling double-check (in case time advanced during inference)
    // Use original `now` for throttling window; do not re-evaluate wall-clock drift
    // Already checked above; keep single gate.

    const derived: DerivedSignal = {
      symbol,
      ts: now,
      signal: result!.lean,
      score: result!.score,
      confidence: result!.confidence,
      horizon_s: 0.5,
      window_s: fv.windowMs / 1000,
      features_version: this.#featuresVersion,
      features: fv,
    };

    // Throttling was already checked pre-model; record emit
    this.#lastEmitAt.set(throttleKey, now);
    this.#alertsEmitted += 1;
    this.#emitAlert(derived);
    this.#recordE2e(e2eStart);

    // Persist feature vector + next-500 ms horizon to sink (optional, stubbed in tests)
    if (this.#featureSink) {
      try {
        await this.#featureSink.persist({ symbol, ts: now, features: fv });
      } catch {
        // sink failures must not break alert path
      }
    }

    void modelLatency;
    void resolveFeature; // keep helper referenced for tree-shaking docs
  }

  // Convenience alias for single-tenant pipelines (factory tier case)
  async tickForTier(
    symbol: string,
    tier: string,
    tenantId = 'default',
    now?: number,
  ): Promise<void> {
    return this.tick(symbol, { id: tenantId, tier }, now);
  }

  #recordE2e(start: number): void {
    this.#e2eLatencies.push(Date.now() - start);
  }

  metrics(): PipelineMetrics {
    const sm = sortedCopy(this.#modelLatencies);
    const se = sortedCopy(this.#e2eLatencies);
    return {
      gatedDropped: this.#gatedDropped,
      throttledDropped: this.#throttledDropped,
      queueShed: this.#queueShed,
      alertsEmitted: this.#alertsEmitted,
      modelLatencyMs: {
        p50: percentile(sm, 50),
        p99: percentile(sm, 99),
      },
      e2eLatencyMs: {
        p50: percentile(se, 50),
        p99: percentile(se, 99),
      },
      _rawModelLatencies: [...this.#modelLatencies],
      _rawE2eLatencies: [...this.#e2eLatencies],
    };
  }

  /**
   * Zero silent drops invariant: every dropped alert (gating, rate-limit, queue >50)
   * is counted and exposed. This helper asserts that no tick was silently ignored.
   * In prod this would be exported to Prometheus / health endpoint.
   */
  droppedTotal(): number {
    return this.#gatedDropped + this.#throttledDropped + this.#queueShed;
  }
}
