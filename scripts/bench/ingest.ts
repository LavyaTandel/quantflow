// Row 1 — Ingest: sustained normalized-tick load through EventPipeline +
// RedisBookStore.applyDepthDiff against real (Dockerized) Redis.
//
//   npx tsx scripts/bench/ingest.ts
//
// Phases: 30 s sustained at SUSTAIN_RATE events/s, short pause, then a 30 s
// burst at BURST_RATE msg/s. Zero drops asserted in both phases.
import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { RedisBookStore } from '../../src/book/redis-store.js';
import { EventPipeline } from '../../src/ingest/pipeline.js';
import type { DepthDiff, NormalizedEvent } from '../../src/ingest/types.js';
import {
  LatencyRecorder,
  PacedEmitter,
  PERCENTILE_DEFINITION,
  REDIS_URL,
  collectHardwareLine,
  fmtInt,
  fmtMs,
  hardwareTable,
  makeRng,
  sleep,
  verdict,
  writeResultsFile,
  yieldToLoop,
} from './lib.js';

const SEED = 20260826;
const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT'] as const;
const BASE_PRICE: Record<string, number> = {
  BTCUSDT: 64000,
  ETHUSDT: 3400,
  SOLUSDT: 148,
  XRPUSDT: 0.58,
  DOGEUSDT: 0.115,
  ADAUSDT: 0.44,
};
const SNAPSHOT_LEVELS = 50;
const MIX = { trade: 0.7, bbo: 0.2, depth: 0.1 };
const SUSTAIN_RATE = 6000;
const SUSTAIN_SECS = 30;
const BURST_RATE = 30000;
const BURST_SECS = 30;
const PAUSE_MS = 2000;

interface Percentiles {
  samples: number;
  p50: number;
  p99: number;
  max: number;
}

interface PhaseResult {
  name: string;
  targetRate: number;
  achievedRate: number;
  emitted: number;
  drops: number;
  depthAnomalies: number;
  lostEvents: number;
  latency: Percentiles;
}

function midOf(symbol: string, rng: () => number): number {
  const base = BASE_PRICE[symbol] ?? 100;
  return base * (1 + (rng() - 0.5) * 0.002);
}

async function seedBooks(client: RedisClientType): Promise<RedisBookStore> {
  const store = new RedisBookStore({
    multi: () => client.multi(),
    del: (keys) => client.del(keys),
    hGetAll: (key) => client.hGetAll(key),
    zRangeWithScores: (key, min, max, opts) => client.zRangeWithScores(key, min, max, opts),
  });
  const rng = makeRng(SEED);
  for (const symbol of SYMBOLS) {
    const bids = [];
    const asks = [];
    const mid = midOf(symbol, rng);
    const tick = mid * 1e-4;
    for (let i = 0; i < SNAPSHOT_LEVELS; i++) {
      bids.push({ price: +(mid - tick * (i + 1)).toPrecision(10), qty: rng() * 8 });
      asks.push({ price: +(mid + tick * (i + 1)).toPrecision(10), qty: rng() * 8 });
    }
    await store.applySnapshot(symbol, bids, asks, 1_000_000);
  }
  return store;
}

class IngestBench {
  readonly #store: RedisBookStore;
  readonly #latency = new LatencyRecorder();
  readonly #chains = new Map<string, Promise<void>>();
  readonly #updateIds: Map<string, number>;
  #drops = 0;
  #depthAnomalies = 0;
  #acceptedDroppable = 0;

  constructor(store: RedisBookStore, updateIds: Map<string, number>) {
    this.#store = store;
    this.#updateIds = updateIds;
    for (const symbol of SYMBOLS) {
      this.#chains.set(symbol, Promise.resolve());
    }
  }

  static async fromStore(store: RedisBookStore): Promise<IngestBench> {
    const updateIds = new Map<string, number>();
    for (const symbol of SYMBOLS) {
      const meta = await store.getMeta(symbol);
      updateIds.set(symbol, meta?.lastUpdateId ?? 1_000_000);
    }
    return new IngestBench(store, updateIds);
  }

  makeEvent(kind: string, rng: () => number): NormalizedEvent {
    const symbol = SYMBOLS[Math.floor(rng() * SYMBOLS.length)] as string;
    const mid = midOf(symbol, rng);
    if (kind === 'depth') {
      const next = (this.#updateIds.get(symbol) ?? 0) + 1;
      this.#updateIds.set(symbol, next);
      const levelCount = 1 + Math.floor(rng() * 3);
      const levels = [];
      for (let i = 0; i < levelCount; i++) {
        levels.push({
          side: rng() < 0.5 ? ('bid' as const) : ('ask' as const),
          price: +(mid * (1 + (rng() - 0.55) * 0.001)).toPrecision(10),
          newQty: rng() < 0.05 ? 0 : +(rng() * 5).toPrecision(6),
        });
      }
      return {
        kind: 'depth',
        symbol,
        ts: Date.now(),
        firstUpdateId: next,
        finalUpdateId: next,
        levels,
      };
    }
    const ts = Date.now();
    if (kind === 'bbo') {
      const halfSpread = mid * 2e-5;
      return {
        kind: 'bbo',
        symbol,
        bidPrice: mid - halfSpread,
        bidQty: rng() * 3,
        askPrice: mid + halfSpread,
        askQty: rng() * 3,
        ts,
      };
    }
    return { kind: 'trade', symbol, price: mid, qty: rng() * 0.5, ts, tradeId: `t-${ts}-${rng()}` };
  }

  // Depth diffs must reach the store in per-symbol update-ID order, so each
  // symbol has a serialized promise chain. Measured latency therefore includes
  // queueing behind earlier diffs of the same symbol under load.
  submitDepth(diff: DepthDiff): void {
    const chain = this.#chains.get(diff.symbol)!;
    const next = chain.then(async () => {
      const t0 = performance.now();
      const result = await this.#store.applyDepthDiff(diff);
      const dtMs = performance.now() - t0;
      if (result === 'applied') this.#latency.record(dtMs);
      else this.#depthAnomalies += 1;
    });
    this.#chains.set(diff.symbol, next);
  }

  acceptTradeOrBbo(event: NormalizedEvent, pipeline: EventPipeline): void {
    const t0 = performance.now();
    if (!pipeline.push(event)) {
      this.#drops += 1;
      return;
    }
    this.#acceptedDroppable += 1;
    this.#latency.record(performance.now() - t0);
  }

  async settle(): Promise<void> {
    await Promise.all([...this.#chains.values()]);
  }

  get latency(): Percentiles {
    return this.#latency.summarize();
  }

  get drops(): number {
    return this.#drops;
  }

  get depthAnomalies(): number {
    return this.#depthAnomalies;
  }

  get acceptedDroppable(): number {
    return this.#acceptedDroppable;
  }
}

async function drainPipeline(
  pipeline: EventPipeline,
  consumed: { trade: number; bbo: number },
  isActive: () => boolean,
): Promise<void> {
  for (;;) {
    let taken = 0;
    for (;;) {
      const event = pipeline.take();
      if (!event) break;
      if (event.kind === 'trade') consumed.trade += 1;
      else consumed.bbo += 1;
      taken += 1;
      if (taken >= 512) break;
    }
    if (taken >= 512) continue;
    if (!isActive() && pipeline.counters().currentDepth === 0) return;
    await yieldToLoop();
  }
}

async function runPhase(
  name: string,
  rate: number,
  secs: number,
  bench: IngestBench,
  rng: () => number,
): Promise<PhaseResult> {
  const pipeline = new EventPipeline({ capacity: 20_000 });
  const consumed = { trade: 0, bbo: 0 };
  let draining = true;
  const drainer = drainPipeline(pipeline, consumed, () => draining);

  const emitter = new PacedEmitter(rate, (count) => {
    for (let i = 0; i < count; i++) {
      const roll = rng();
      const kind = roll < MIX.trade ? 'trade' : roll < MIX.trade + MIX.bbo ? 'bbo' : 'depth';
      const event = bench.makeEvent(kind, rng);
      if (event.kind === 'depth') bench.submitDepth(event);
      else bench.acceptTradeOrBbo(event, pipeline);
    }
  });

  emitter.start();
  await sleep(secs * 1000);
  emitter.stop();
  draining = false;
  pipeline.close();
  await drainer;
  await bench.settle();

  const pipelineInternalDrops = Object.values(pipeline.counters().droppedByKind).reduce(
    (sum, n) => sum + n,
    0,
  );
  const lostEvents = Math.max(0, bench.acceptedDroppable - (consumed.trade + consumed.bbo));
  const elapsedSec = emitter.elapsedSec;
  return {
    name,
    targetRate: rate,
    achievedRate: emitter.count / elapsedSec,
    emitted: emitter.count,
    drops: bench.drops + pipelineInternalDrops,
    depthAnomalies: bench.depthAnomalies,
    lostEvents,
    latency: bench.latency,
  };
}

async function main(): Promise<void> {
  console.log('[ingest] connecting to Redis...');
  const client = createClient({ url: REDIS_URL });
  await client.connect();
  await client.flushAll();

  const hardware = await collectHardwareLine({ redis: client });
  const store = await seedBooks(client);

  console.log(`[ingest] phase 1/2: sustained ${SUSTAIN_RATE}/s x ${SUSTAIN_SECS}s`);
  const sustained = await runPhase(
    'sustained',
    SUSTAIN_RATE,
    SUSTAIN_SECS,
    await IngestBench.fromStore(store),
    makeRng(SEED + 1),
  );
  console.log(`[ingest] phase 2/2: burst ${BURST_RATE}/s x ${BURST_SECS}s`);
  await sleep(PAUSE_MS);
  const burst = await runPhase(
    'burst',
    BURST_RATE,
    BURST_SECS,
    await IngestBench.fromStore(store),
    makeRng(SEED + 2),
  );

  await client.quit();

  const zeroDrops =
    sustained.drops === 0 &&
    burst.drops === 0 &&
    sustained.depthAnomalies === 0 &&
    burst.depthAnomalies === 0 &&
    sustained.lostEvents === 0 &&
    burst.lostEvents === 0;
  const p99Ok = sustained.latency.p99 <= 50 && burst.latency.p99 <= 50;
  const sustainedOk = sustained.achievedRate >= 5000;
  const passed = zeroDrops && p99Ok && sustainedOk;

  const row = (label: string, r: PhaseResult): string =>
    `| ${label} | ${fmtInt(r.targetRate)} | ${fmtInt(Math.round(r.achievedRate))} | ${fmtInt(r.emitted)} | ${r.drops} | ${r.depthAnomalies} | ${r.lostEvents} | ${fmtMs(r.latency.p50)} | ${fmtMs(r.latency.p99)} | ${fmtMs(r.latency.max)} |`;

  const markdown = `# Benchmark — Row 1: Ingest

Date: ${new Date().toISOString().slice(0, 10)} · Script: \`scripts/bench/ingest.ts\` · Verdict: **${verdict(passed)}**

## Headline

- Sustained: **${fmtInt(Math.round(sustained.achievedRate))} events/s** over ${SUSTAIN_SECS}s (target ≥5,000)
- Burst: survived **${fmtInt(Math.round(burst.achievedRate))} msg/s** for ${BURST_SECS}s with **${burst.drops} drops**
- End-to-end p99: sustained ${fmtMs(sustained.latency.p99)} / burst ${fmtMs(burst.latency.p99)} (target ≤50 ms)

## Hardware (recorded at run time)

${hardwareTable(hardware)}

## Methodology

- Load is synthetic and generated in-process (no network feed), paced by a drift-corrected timer (\`PacedEmitter\`, 5 ms ticks).
- Mix per event: ${MIX.trade * 100}% trade / ${MIX.bbo * 100}% bbo / ${MIX.depth * 100}% depth across ${SYMBOLS.length} symbols (${SYMBOLS.join(', ')}); seeded RNG (mulberry32, SEED=${SEED}) makes runs reproducible.
- trade/bbo events are measured receipt → \`EventPipeline.push\` acceptance (synchronous in-process path).
- depth events are measured receipt → \`RedisBookStore.applyDepthDiff\` completion against real Dockerized Redis, serialized per symbol to preserve update-ID continuity, so samples include same-symbol queue wait.
- One synthetic event ≙ one raw venue message ("30K msg/s raw input" is interpreted post-normalization because generation bypasses wire decoding).
- Drops are counted from four sources: \`push()\` refusals, \`EventPipeline\` \`droppedByKind\` counters, non-\`applied\` continuity results (stale/gap), and drained-vs-accepted reconciliation of trade/bbo events.
- ${PERCENTILE_DEFINITION}

## Results

| Phase | Target rate/s | Achieved/s | Events | Drops | Depth anomalies | Lost trade/bbo | p50 | p99 | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
${row('Sustained 30s', sustained)}
${row('Burst 30s', burst)}

Latency sample counts: sustained ${fmtInt(sustained.latency.samples)}, burst ${fmtInt(burst.latency.samples)}. Client-observed via \`performance.now()\`.

## Config constants

SEED=${SEED}, SUSTAIN_RATE=${SUSTAIN_RATE}, BURST_RATE=${BURST_RATE}, mix=${JSON.stringify(MIX)}, snapshot=${SNAPSHOT_LEVELS} levels/side/symbol, pipeline capacity 20,000.

## Anomalies

${passed ? 'None observed.' : 'See verdict criteria below.'}

## Verdict criteria

- Sustained ≥5,000 events/s: ${verdict(sustainedOk)} (${fmtInt(Math.round(sustained.achievedRate))})
- Burst completed with zero drops/anomalies: ${verdict(zeroDrops)}
- e2e p99 ≤50 ms in both phases: ${verdict(p99Ok)}

**Overall: ${verdict(passed)}**

Reproduce: \`docker compose up -d\` → \`npx tsx scripts/bench/ingest.ts\`
`;
  const path = await writeResultsFile('ingest', markdown);
  console.log(`[ingest] results written to ${path}`);
  console.log(`[ingest] VERDICT: ${verdict(passed)}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
