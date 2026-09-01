export * from './features.js';
export * from './model.js';
export * from './pipeline.js';

import type { BookStore } from '../book/store.js';
import type { Model } from './model.js';
import { FakeModel } from './model.js';
import { AlertPipeline } from './pipeline.js';
import type { DerivedSignal, FeatureSink } from './pipeline.js';

export interface CreateAlertPipelineOptions {
  bookStore: BookStore;
  model?: Model;
  tier?: string;
  emitAlert: (alert: DerivedSignal) => void;
  featureSink?: FeatureSink;
  ofiThreshold?: number;
  maxQueueSize?: number;
  featuresVersion?: string;
  windowMs?: number;
}

/**
 * Factory: createAlertPipeline({ bookStore, model, tier, emitAlert })
 *
 * - bookStore: injected BookStore (MemoryBookStore in tests, RedisBookStore in prod)
 * - model: defaults to FakeModel (hermetic, 5-15 ms) when not provided; prod would inject LlamaCppModel
 * - emitAlert: in tests capture to array; in prod wire to gateway broadcast:
 *     pipeline.emitAlert -> gateway broadcast DerivedSignal JSON
 *     Example prod wiring:
 *       const pipeline = createAlertPipeline({
 *         bookStore: new RedisBookStore(redisClient),
 *         model: new LlamaCppModel("http://127.0.0.1:8080"),
 *         emitAlert: (sig) => wsServer.broadcast(JSON.stringify(sig)),
 *       });
 *     This keeps Derived Signal boundary (ADR 0003): external tenants only
 *     receive DerivedSignal (symbol, ts, signal, score, confidence, horizon_s, window_s,
 *     features_version) throttled ≤1/symbol/sec — never NormalizedEvent or raw depth array.
 * - tier is retained for single-tenant convenience but per-tick tenant.tier is authoritative.
 */
export function createAlertPipeline(opts: CreateAlertPipelineOptions): AlertPipeline {
  const model = opts.model ?? new FakeModel({ minLatencyMs: 5, maxLatencyMs: 15 });
  return new AlertPipeline({
    bookStore: opts.bookStore,
    model,
    emitAlert: opts.emitAlert,
    featureSink: opts.featureSink,
    ofiThreshold: opts.ofiThreshold,
    maxQueueSize: opts.maxQueueSize,
    featuresVersion: opts.featuresVersion,
    windowMs: opts.windowMs,
  });
}
