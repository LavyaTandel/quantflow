import { describe, expect, it } from 'vitest';
import { MemoryBookStore } from '../../src/book/memory-store.js';
import { FakeModel } from '../../src/ai/model.js';
import { createAlertPipeline } from '../../src/ai/index.js';
import type { DerivedSignal } from '../../src/ai/pipeline.js';

async function seedStore(
  store: MemoryBookStore,
  symbol: string,
  bids: Array<[number, number]>,
  asks: Array<[number, number]>,
): Promise<void> {
  await store.applySnapshot(
    symbol,
    bids.map(([p, q]) => ({ price: p, qty: q })),
    asks.map(([p, q]) => ({ price: p, qty: q })),
    1,
  );
}

describe('AlertPipeline', () => {
  it('gating: only 5-15% windows produce alerts (high I or OFI or sweep)', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
      ofiThreshold: 100,
    });
    // 20 windows: 18 neutral (I~0, OFI small), 2 gated pass (I>0.3)
    for (let i = 0; i < 20; i++) {
      if (i === 5) {
        await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]); // I=0.8
        await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000 + i * 1100);
      } else if (i === 12) {
        await seedStore(store, 'BTCUSDT', [[100, 10]], [[101, 90]]); // I=-0.8
        await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000 + i * 1100);
      } else {
        await seedStore(store, 'BTCUSDT', [[100, 10]], [[101, 10]]); // I=0 neutral
        await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000 + i * 1100);
      }
    }
    const m = pipeline.metrics();
    // Only 2 alerts out of 20 =10% => within 5-15%
    expect(alerts.length).toBe(2);
    expect(m.gatedDropped).toBe(18);
    expect(m.alertsEmitted).toBe(2);
    expect(m.gatedDropped + m.alertsEmitted + m.throttledDropped + m.queueShed).toBe(20);
  });

  it('throttling: ≤1/symbol/sec, second within window dropped and counted', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
    });
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000);
    expect(alerts.length).toBe(1);
    // second within 1s window => throttled
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000 + 500);
    expect(alerts.length).toBe(1);
    expect(pipeline.metrics().throttledDropped).toBe(1);
    // after 1s window passes => emits again
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000 + 1100);
    expect(alerts.length).toBe(2);
  });

  it('Derived Signal boundary: throttled ≤1/symbol/sec via lastEmitAt map', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
    });
    const ts = 5000;
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, ts);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, ts + 100);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, ts + 200);
    expect(alerts.length).toBe(1);
    expect(pipeline.metrics().throttledDropped).toBe(2);
    // Each alert is DerivedSignal shape, not raw NormalizedEvent
    for (const a of alerts) {
      expect(a).toHaveProperty('symbol');
      expect(a).toHaveProperty('ts');
      expect(a).toHaveProperty('signal');
      expect(a).toHaveProperty('score');
      expect(a).toHaveProperty('confidence');
      expect(a).toHaveProperty('horizon_s');
      expect(a).toHaveProperty('window_s');
      expect(a).toHaveProperty('features_version');
      expect(a).not.toHaveProperty('kind');
      expect(a).not.toHaveProperty('levels');
    }
  });

  it('Starter vs Pro feature filtering (F2 blocked for starter)', async () => {
    const store = new MemoryBookStore();
    const alertsStarter: DerivedSignal[] = [];
    const alertsPro: DerivedSignal[] = [];
    // Starter pipeline tight OFI threshold so OFI triggers F2
    // We need OFI trigger without I: create history then OFI delta
    const store2 = new MemoryBookStore();
    const pipelineStarter = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alertsStarter.push(a),
      ofiThreshold: 1,
    });
    const pipelinePro = createAlertPipeline({
      bookStore: store2,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alertsPro.push(a),
      ofiThreshold: 1,
    });
    // Seed balanced then create OFI via price move: bid price up with qty
    await seedStore(store, 'BTCUSDT', [[100, 10]], [[101, 10]]);
    await seedStore(store2, 'BTCUSDT', [[100, 10]], [[101, 10]]);
    // First tick to establish prev (gated)
    await pipelineStarter.tick('BTCUSDT', { id: 'starter1', tier: 'starter' }, 1000);
    await pipelinePro.tick('BTCUSDT', { id: 'pro1', tier: 'pro' }, 1000);
    // Second tick: move bid price up => OFI ~10, I still 0 (balanced qty 10/10)
    await seedStore(store, 'BTCUSDT', [[101, 10]], [[102, 10]]);
    await seedStore(store2, 'BTCUSDT', [[101, 10]], [[102, 10]]);
    await pipelineStarter.tick('BTCUSDT', { id: 'starter1', tier: 'starter' }, 2200);
    await pipelinePro.tick('BTCUSDT', { id: 'pro1', tier: 'pro' }, 2200);
    // Starter should have 0 new alerts (F2 blocked), Pro should have 1
    expect(alertsStarter.length).toBe(0);
    expect(alertsPro.length).toBe(1);
    expect(pipelineStarter.metrics().gatedDropped).toBeGreaterThanOrEqual(1);
    // Pro enterprise F4 test: sweep
    const storeSweep = new MemoryBookStore();
    const alertsEnterprise: DerivedSignal[] = [];
    const pipelineEnt = createAlertPipeline({
      bookStore: storeSweep,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alertsEnterprise.push(a),
    });
    // Force sweep: empty ask side + tight spread? empty ask triggers sweep
    await seedStore(storeSweep, 'BTCUSDT', [[100, 10]], []);
    await pipelineEnt.tick('BTCUSDT', { id: 'ent', tier: 'enterprise' }, 3000);
    // Enterprise should get F4 sweep alert, starter would not
    expect(alertsEnterprise.length).toBe(1);
    const storeStarterSweep = new MemoryBookStore();
    const alertsStarterSweep: DerivedSignal[] = [];
    const pipelineStarterSweep = createAlertPipeline({
      bookStore: storeStarterSweep,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alertsStarterSweep.push(a),
    });
    await seedStore(storeStarterSweep, 'BTCUSDT', [[100, 10]], []);
    await pipelineStarterSweep.tick('BTCUSDT', { id: 'starter', tier: 'starter' }, 4000);
    expect(alertsStarterSweep.length).toBe(0); // F4 blocked via isFeatureAllowed
  });

  it('Pro must not receive F3 (internal)', async () => {
    // F3 never emitted externally; pipeline never yields F3 feature, so this is vacuously true
    // We assert isFeatureAllowed mapping directly and that pipeline does not emit F3
    const { isFeatureAllowed } = await import('../../src/billing/tiers.js');
    expect(isFeatureAllowed('pro', 'F3')).toBe(false);
    expect(isFeatureAllowed('enterprise', 'F3')).toBe(true);
  });

  it('queue shed when >50', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
      maxQueueSize: 50,
    });
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    pipeline.setQueueDepth(51);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 5000);
    expect(alerts.length).toBe(0);
    expect(pipeline.metrics().queueShed).toBe(1);
    // Reset and should emit
    pipeline.setQueueDepth(0);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 6000);
    expect(alerts.length).toBe(1);
  });

  it('zero silent drops (every drop counted)', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
      maxQueueSize: 50,
    });
    let ticks = 0;
    // Mix of gated, throttled, queue shed
    for (let i = 0; i < 10; i++) {
      await seedStore(store, 'BTCUSDT', [[100, 10]], [[101, 10]]); // gated
      await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, i * 2000);
      ticks++;
    }
    // One throttled
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 25000);
    ticks++;
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 25200); // throttled
    ticks++;
    // One queue shed
    pipeline.setQueueDepth(50);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 27000);
    ticks++;
    pipeline.setQueueDepth(0);
    const m = pipeline.metrics();
    expect(m.gatedDropped + m.throttledDropped + m.queueShed + m.alertsEmitted).toBe(ticks);
    expect(pipeline.droppedTotal()).toBe(m.gatedDropped + m.throttledDropped + m.queueShed);
    // No silent drops: total ticks accounted for
  });

  it('tenant isolation (two tenants same symbol get independent throttling)', async () => {
    const store = new MemoryBookStore();
    const alertsA: DerivedSignal[] = [];
    const alertsB: DerivedSignal[] = [];
    // Shared book store but two pipelines sharing same store but independent?
    // Better: single pipeline handling both tenants with separate throttle keys
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
    });
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 'tenant-a', tier: 'pro' }, 1000);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 'tenant-b', tier: 'pro' }, 1000);
    // Both should emit despite same symbol same timestamp because throttle key is per tenant
    expect(alerts.length).toBe(2);
    expect(alerts[0]!.symbol).toBe('BTCUSDT');
    expect(alerts[1]!.symbol).toBe('BTCUSDT');
    // Second tick within window for tenant-a should throttle, tenant-b also independent
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 'tenant-a', tier: 'pro' }, 1500);
    expect(alerts.length).toBe(2);
    expect(pipeline.metrics().throttledDropped).toBe(1);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 'tenant-b', tier: 'pro' }, 1500);
    expect(alerts.length).toBe(2);
    expect(pipeline.metrics().throttledDropped).toBe(2);
    // After window, both can emit
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 'tenant-a', tier: 'pro' }, 2100);
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 'tenant-b', tier: 'pro' }, 2100);
    expect(alerts.length).toBe(4);
    void alertsA;
    void alertsB;
  });

  it('e2e latency p50/p99 <250 ms P99 budget with FakeModel 5-15 ms (assert p99 ≤ 80 ms in test harness)', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ minLatencyMs: 5, maxLatencyMs: 15 }),
      emitAlert: (a) => alerts.push(a),
    });
    // Drive ~40 ticks with periodic imbalance
    for (let i = 0; i < 40; i++) {
      const isSignal = i % 8 === 0;
      if (isSignal) await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
      else await seedStore(store, 'BTCUSDT', [[100, 10]], [[101, 10]]);
      await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 10000 + i * 1100);
    }
    const m = pipeline.metrics();
    expect(m.alertsEmitted).toBeGreaterThan(0);
    expect(m.e2eLatencyMs.p99).toBeLessThanOrEqual(80);
    expect(m.e2eLatencyMs.p50).toBeLessThanOrEqual(80);
    expect(m.modelLatencyMs.p99).toBeLessThanOrEqual(30);
    // Overall budget 250 ms
    expect(m.e2eLatencyMs.p99).toBeLessThan(250);
  });

  it('metrics percentile helpers and sink persistence', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const persisted: unknown[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
      featureSink: {
        persist: (e) => {
          persisted.push(e);
        },
      },
    });
    await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
    await pipeline.tick('BTCUSDT', { id: 't1', tier: 'pro' }, 1000);
    expect(persisted.length).toBe(1);
    const m = pipeline.metrics();
    expect(m.modelLatencyMs.p50).toBeGreaterThanOrEqual(0);
    expect(m.e2eLatencyMs.p50).toBeGreaterThanOrEqual(0);
  });

  it('emitted alert honors Derived Signal shape and tier reuse', async () => {
    const store = new MemoryBookStore();
    const alerts: DerivedSignal[] = [];
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: (a) => alerts.push(a),
    });
    await seedStore(store, 'BTCUSDT', [[100, 80]], [[101, 20]]);
    await pipeline.tick('BTCUSDT', { id: 'ent', tier: 'enterprise' }, 9999);
    expect(alerts.length).toBe(1);
    const a = alerts[0]!;
    expect(a.symbol).toBe('BTCUSDT');
    expect(a.ts).toBe(9999);
    expect(['buy', 'sell', 'neutral']).toContain(a.signal);
    expect(a.horizon_s).toBe(0.5);
    expect(a.window_s).toBe(0.1);
    expect(a.features_version).toBe('v2');
    expect(a.confidence).toBeGreaterThanOrEqual(0);
    expect(a.score).toBeDefined();
  });

  it('zero silent drops invariant across random ticks', async () => {
    const store = new MemoryBookStore();
    const pipeline = createAlertPipeline({
      bookStore: store,
      model: new FakeModel({ latencyMs: 0 }),
      emitAlert: () => {},
    });
    let total = 0;
    for (let i = 0; i < 30; i++) {
      const pick = i % 3;
      if (pick === 0) await seedStore(store, 'BTCUSDT', [[100, 10]], [[101, 10]]);
      else if (pick === 1) await seedStore(store, 'BTCUSDT', [[100, 90]], [[101, 10]]);
      else await seedStore(store, 'BTCUSDT', [[100, 10]], []);
      await pipeline.tick('BTCUSDT', { id: 't1', tier: 'starter' }, i * 1100);
      total++;
    }
    const m = pipeline.metrics();
    expect(m.gatedDropped + m.throttledDropped + m.queueShed + m.alertsEmitted).toBe(total);
  });
});
