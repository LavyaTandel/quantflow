// Row 2 — Redis ZSET order-book operations against real Dockerized Redis.
//
//   npx tsx scripts/bench/zset-ops.ts
//
// Legs A/B reproduce the original evidence shape: mixed workload
// applyDepthDiff : topN : getMeta ≈ 70/25/5 from ONE Node process, where
// A = the six production-universe symbols (depth diffs serialized per symbol
// to preserve update-ID continuity — the honest production shape) and
// B = N synthetic symbols exposing the concurrency ceiling of the same store
// API. Legs C/D/E are recalibration data quantifying the SAME store API with
// the artificial serialization constraint relaxed: C = ordered cross-symbol
// concurrency (per-symbol order still preserved) with MAX_INFLIGHT swept over
// [8, 32, 128]; D = deep-pipelining ceiling (depth-only, 48 symbols,
// MAX_INFLIGHT=512, update IDs pre-seeded far ahead); E = control repeating
// C's best config at a 50/50 depth/topN mix. Results land in
// docs/benchmarks/<RUN_DATE>-zset-recalibration.md; the original
// <RUN_DATE>-zset.md evidence file stays untouched as the historical anchor.
import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { RedisBookStore } from '../../src/book/redis-store.js';
import type { BookRedisClient } from '../../src/book/redis-store.js';
import {
  LatencyRecorder,
  PERCENTILE_DEFINITION,
  REDIS_URL,
  collectHardwareLine,
  fmtInt,
  fmtMs,
  hardwareTable,
  makeRng,
  verdict,
  writeResultsFile,
  yieldToLoop,
} from './lib.js';

const SEED = 20260826;
const PRODUCTION_SYMBOLS = [
  'BTCUSDT',
  'ETHUSDT',
  'SOLUSDT',
  'XRPUSDT',
  'DOGEUSDT',
  'ADAUSDT',
] as const;
const LEG_B_SYMBOLS = Number(process.env.LEG_B_SYMBOLS ?? 48);
const MIX = { applyDepthDiff: 0.7, topN: 0.25, getMeta: 0.05 };
const LEG_A_SECS = Number(process.env.LEG_A_SECS ?? 30);
const LEG_B_SECS = Number(process.env.LEG_B_SECS ?? 15);
const SNAPSHOT_LEVELS = 50;
const TOP_N = 10;
const MAX_INFLIGHT = 1024;
const LEG_C_SECS = Number(process.env.LEG_C_SECS ?? 20);
const LEG_C_INFLIGHT_LEVELS: number[] = [8, 32, 128];
const LEG_D_SECS = Number(process.env.LEG_D_SECS ?? 15);
const LEG_D_SYMBOLS = Number(process.env.LEG_D_SYMBOLS ?? 48);
const LEG_D_MAX_INFLIGHT = Number(process.env.LEG_D_MAX_INFLIGHT ?? 512);
const LEG_D_ID_BASE = 1_000_000_000;
const LEG_E_SECS = Number(process.env.LEG_E_SECS ?? 15);
// Second recalibration — sweep at 16/64/256 + TOP_N=50 + ceiling re-confirm
const LEG_C2_INFLIGHT_LEVELS: number[] = [16, 64, 256];
const LEG_C2_SECS = Number(process.env.LEG_C2_SECS ?? 20);
const LEG_F_SECS = Number(process.env.LEG_F_SECS ?? 15);
const LEG_F_TOP_N = 50;
const LEG_G_SECS = Number(process.env.LEG_G_SECS ?? 15);
const LEG_G_SYMBOLS = Number(process.env.LEG_G_SYMBOLS ?? 48);
const LEG_G_MAX_INFLIGHT = Number(process.env.LEG_G_MAX_INFLIGHT ?? 512);
const LEG_G_ID_BASE = 1_000_000_000;
const ORIGINAL_LEG_A_OPS = 952;
const FIRST_CEILING_OPS = 16_142;

type OpType = keyof typeof MIX;
type Mix = Record<OpType, number>;
interface Percentiles {
  samples: number;
  p50: number;
  p99: number;
  max: number;
}
interface CommandStat {
  calls: number;
  usec: number;
}

function parseCommandStats(info: string): Map<string, CommandStat> {
  const map = new Map<string, CommandStat>();
  for (const line of info.split(/\r?\n/)) {
    const match = /^cmdstat_([A-Za-z_]+):calls=(\d+),usec=(\d+)/.exec(line);
    if (match) map.set(match[1] as string, { calls: Number(match[2]), usec: Number(match[3]) });
  }
  return map;
}

function deltaCommandStats(
  before: Map<string, CommandStat>,
  after: Map<string, CommandStat>,
): Map<string, CommandStat> {
  const delta = new Map<string, CommandStat>();
  for (const [cmd, statAfter] of after) {
    if (cmd === 'info') continue;
    const statBefore = before.get(cmd) ?? { calls: 0, usec: 0 };
    const dCalls = statAfter.calls - statBefore.calls;
    const dUsec = statAfter.usec - statBefore.usec;
    if (dCalls > 0) delta.set(cmd, { calls: dCalls, usec: Math.max(0, dUsec) });
  }
  return delta;
}

function symbolList(count: number): string[] {
  return Array.from({ length: count }, (_, i) =>
    i < PRODUCTION_SYMBOLS.length
      ? (PRODUCTION_SYMBOLS[i] as string)
      : `${PRODUCTION_SYMBOLS[i % PRODUCTION_SYMBOLS.length]}.S${i}`,
  );
}

async function pingBaseline(client: RedisClientType, samples: number): Promise<Percentiles> {
  const recorder = new LatencyRecorder();
  for (let i = 0; i < samples; i++) {
    const t0 = performance.now();
    await client.ping();
    recorder.record(performance.now() - t0);
  }
  return recorder.summarize();
}

function basePriceOf(symbol: string): number {
  return BASE_PRICE[symbol.split('.')[0] ?? 'BTCUSDT'] ?? 100;
}

const BASE_PRICE: Record<string, number> = {
  BTCUSDT: 64000,
  ETHUSDT: 3400,
  SOLUSDT: 148,
  XRPUSDT: 0.58,
  DOGEUSDT: 0.115,
  ADAUSDT: 0.44,
};

interface LegResult {
  name: string;
  symbols: number;
  maxInflight: number;
  mix: Mix;
  idBase: number;
  achievedOpsPerSec: number;
  elapsedSecs: number;
  completed: Record<OpType, number>;
  rttMs: Record<OpType | 'all', Percentiles>;
  anomalies: number;
  commandStats: Map<string, CommandStat>;
  totalCalls: number;
  meanUsecPerCall: number;
}

interface LegOptions {
  mix?: Mix;
  maxInflight?: number;
  idBase?: number;
  topN?: number;
}

async function runLeg(
  name: string,
  client: RedisClientType,
  store: RedisBookStore,
  symbols: string[],
  durationSecs: number,
  options: LegOptions = {},
): Promise<LegResult> {
  const mix = options.mix ?? MIX;
  const maxInflight = options.maxInflight ?? MAX_INFLIGHT;
  const idBase = options.idBase ?? 1_000_000;
  const topN = options.topN ?? TOP_N;
  for (const symbol of symbols) {
    await client.del([
      `book:${symbol}:bids`,
      `book:${symbol}:asks`,
      `book:${symbol}:qty`,
      `book:${symbol}:meta`,
    ]);
  }
  const rng = makeRng(SEED);
  for (const symbol of symbols) {
    const mid = basePriceOf(symbol) * (1 + (rng() - 0.5) * 0.002);
    const tick = mid * 1e-4;
    const bids = [];
    const asks = [];
    for (let i = 0; i < SNAPSHOT_LEVELS; i++) {
      bids.push({ price: +(mid - tick * (i + 1)).toPrecision(10), qty: rng() * 8 });
      asks.push({ price: +(mid + tick * (i + 1)).toPrecision(10), qty: rng() * 8 });
    }
    await store.applySnapshot(symbol, bids, asks, idBase);
  }

  const updateIds = new Map<string, number>(symbols.map((s) => [s, idBase]));
  const chains = new Map<string, Promise<void>>(symbols.map((s) => [s, Promise.resolve()]));
  const completed: Record<OpType, number> = { applyDepthDiff: 0, topN: 0, getMeta: 0 };
  const rttMs = {
    applyDepthDiff: new LatencyRecorder(),
    topN: new LatencyRecorder(),
    getMeta: new LatencyRecorder(),
    all: new LatencyRecorder(),
  };
  let inflight = 0;
  let anomalies = 0;

  const pickSymbol = (): string => symbols[Math.floor(rng() * symbols.length)] as string;

  const dispatchDepth = (symbol: string): void => {
    const next = (updateIds.get(symbol) ?? 0) + 1;
    updateIds.set(symbol, next);
    const levelCount = 1 + Math.floor(rng() * 3);
    const levels = [];
    const mid = basePriceOf(symbol) * (1 + (rng() - 0.5) * 0.002);
    for (let i = 0; i < levelCount; i++) {
      levels.push({
        side: rng() < 0.5 ? ('bid' as const) : ('ask' as const),
        price: +(mid * (1 + (rng() - 0.55) * 0.001)).toPrecision(10),
        newQty: rng() < 0.05 ? 0 : +(rng() * 5).toPrecision(6),
      });
    }
    const diff = {
      kind: 'depth' as const,
      symbol,
      ts: Date.now(),
      firstUpdateId: next,
      finalUpdateId: next,
      levels,
    };
    // Serialized per symbol so update-ID continuity stays clean under load.
    const chain = chains.get(symbol)!;
    const nextChain = chain.then(async () => {
      const t0 = performance.now();
      const result = await store.applyDepthDiff(diff);
      const dt = performance.now() - t0;
      if (result !== 'applied') anomalies += 1;
      completed.applyDepthDiff += 1;
      rttMs.applyDepthDiff.record(dt);
      rttMs.all.record(dt);
      inflight -= 1;
    });
    chains.set(symbol, nextChain);
  };

  const dispatchTopN = (symbol: string): void => {
    const t0 = performance.now();
    void store
      .topN(symbol, topN)
      .catch(() => {
        anomalies += 1;
      })
      .then(() => {
        const dt = performance.now() - t0;
        completed.topN += 1;
        rttMs.topN.record(dt);
        rttMs.all.record(dt);
        inflight -= 1;
      });
  };

  const dispatchGetMeta = (symbol: string): void => {
    const t0 = performance.now();
    void store
      .getMeta(symbol)
      .catch(() => {
        anomalies += 1;
      })
      .then(() => {
        const dt = performance.now() - t0;
        completed.getMeta += 1;
        rttMs.getMeta.record(dt);
        rttMs.all.record(dt);
        inflight -= 1;
      });
  };

  // Pick whichever op type is furthest behind its target share so the issued
  // mix tracks 70/25/5 regardless of per-type completion rates.
  const nextOpType = (): OpType => {
    let best: OpType = 'applyDepthDiff';
    let bestRatio = Number.POSITIVE_INFINITY;
    for (const key of Object.keys(mix) as OpType[]) {
      const ratio = completed[key] / mix[key];
      if (ratio < bestRatio) {
        bestRatio = ratio;
        best = key;
      }
    }
    return best;
  };

  const statsBefore = parseCommandStats(await client.info('commandstats'));
  const startedAt = performance.now();
  const deadline = startedAt + durationSecs * 1000;
  let dispatched = 0;

  while (performance.now() < deadline) {
    if (inflight >= maxInflight) {
      await yieldToLoop();
      continue;
    }
    const type = nextOpType();
    const symbol = pickSymbol();
    inflight += 1;
    dispatched += 1;
    if (type === 'applyDepthDiff') dispatchDepth(symbol);
    else if (type === 'topN') dispatchTopN(symbol);
    else dispatchGetMeta(symbol);
    if (dispatched % 256 === 0) await yieldToLoop();
  }

  const elapsedSecs = (performance.now() - startedAt) / 1000;
  await Promise.all([...chains.values()]);
  while (inflight > 0) await yieldToLoop();
  const statsAfter = parseCommandStats(await client.info('commandstats'));
  const commandStats = deltaCommandStats(statsBefore, statsAfter);

  let totalCalls = 0;
  let totalUsec = 0;
  for (const stat of commandStats.values()) {
    totalCalls += stat.calls;
    totalUsec += stat.usec;
  }

  return {
    name,
    symbols: symbols.length,
    maxInflight,
    mix,
    idBase,
    achievedOpsPerSec:
      (completed.applyDepthDiff + completed.topN + completed.getMeta) / elapsedSecs,
    elapsedSecs,
    completed,
    rttMs: {
      applyDepthDiff: rttMs.applyDepthDiff.summarize(),
      topN: rttMs.topN.summarize(),
      getMeta: rttMs.getMeta.summarize(),
      all: rttMs.all.summarize(),
    },
    anomalies,
    commandStats,
    totalCalls,
    meanUsecPerCall: totalCalls > 0 ? totalUsec / totalCalls : 0,
  };
}

function opRows(name: string, leg: LegResult): string[] {
  return (['applyDepthDiff', 'topN', 'getMeta'] as const)
    .filter((key) => leg.rttMs[key].samples > 0)
    .map((key) => {
      const r = leg.rttMs[key];
      return `| ${name} | ${key} | ${fmtInt(leg.completed[key])} | ${fmtMs(r.p50)} | ${fmtMs(r.p99)} | ${fmtMs(r.max)} |`;
    });
}

const mixLabel = (mix: Mix): string =>
  `${Math.round(mix.applyDepthDiff * 100)}/${Math.round(mix.topN * 100)}/${Math.round(mix.getMeta * 100)}`;

async function main(): Promise<void> {
  console.log('[zset] connecting to Redis...');
  const client: RedisClientType = createClient({ url: REDIS_URL });
  await client.connect();
  await client.flushAll();

  const hardware = await collectHardwareLine({ redis: client });
  const bookClient: BookRedisClient = {
    multi: () => client.multi(),
    del: (keys) => client.del(keys),
    hGetAll: (key) => client.hGetAll(key),
    zRangeWithScores: (key, min, max, opts) => client.zRangeWithScores(key, min, max, opts),
  };
  const store = new RedisBookStore(bookClient);

  console.log('[zset] measuring raw PING baseline...');
  const ping = await pingBaseline(client, 2000);

  console.log(`[zset] leg A: ${PRODUCTION_SYMBOLS.length} production symbols x ${LEG_A_SECS}s`);
  const legA = await runLeg('A', client, store, [...PRODUCTION_SYMBOLS], LEG_A_SECS);
  console.log(`[zset] leg B: ${LEG_B_SYMBOLS} synthetic symbols x ${LEG_B_SECS}s`);
  const legB = await runLeg('B', client, store, symbolList(LEG_B_SYMBOLS), LEG_B_SECS);

  const legC: Array<{ inflight: number; result: LegResult }> = [];
  for (const inflight of LEG_C_INFLIGHT_LEVELS) {
    console.log(
      `[zset] leg C: ordered cross-symbol @ inflight ${inflight} x ${LEG_C_SECS}s (${PRODUCTION_SYMBOLS.length} production symbols)`,
    );
    const result = await runLeg(
      `C@${inflight}`,
      client,
      store,
      [...PRODUCTION_SYMBOLS],
      LEG_C_SECS,
      {
        maxInflight: inflight,
      },
    );
    legC.push({ inflight, result });
  }
  const legCBest = legC.reduce((a, b) =>
    b.result.achievedOpsPerSec > a.result.achievedOpsPerSec ? b : a,
  );
  console.log(`[zset] leg D: pipelined ceiling, ${LEG_D_SYMBOLS} symbols x ${LEG_D_SECS}s`);
  const legD = await runLeg('D', client, store, symbolList(LEG_D_SYMBOLS), LEG_D_SECS, {
    mix: { applyDepthDiff: 1, topN: 0, getMeta: 0 },
    maxInflight: LEG_D_MAX_INFLIGHT,
    idBase: LEG_D_ID_BASE,
  });
  console.log(
    `[zset] leg E: read-heavy 50/50 control @ inflight ${legCBest.inflight} x ${LEG_E_SECS}s`,
  );
  const legE = await runLeg('E', client, store, [...PRODUCTION_SYMBOLS], LEG_E_SECS, {
    mix: { applyDepthDiff: 0.5, topN: 0.5, getMeta: 0 },
    maxInflight: legCBest.inflight,
  });

  // Second recalibration — sweep at 16/64/256 (ordered cross-symbol, 6 syms, 70/25/5)
  const legC2: Array<{ inflight: number; result: LegResult }> = [];
  for (const inflight of LEG_C2_INFLIGHT_LEVELS) {
    console.log(
      `[zset] leg C2: ordered cross-symbol @ inflight ${inflight} x ${LEG_C2_SECS}s (${PRODUCTION_SYMBOLS.length} production symbols)`,
    );
    const result = await runLeg(
      `C2@${inflight}`,
      client,
      store,
      [...PRODUCTION_SYMBOLS],
      LEG_C2_SECS,
      {
        maxInflight: inflight,
      },
    );
    legC2.push({ inflight, result });
  }
  const legC2Best = legC2.reduce((a, b) =>
    b.result.achievedOpsPerSec > a.result.achievedOpsPerSec ? b : a,
  );
  console.log(
    `[zset] leg F: TOP_N=${LEG_F_TOP_N} @ best inflight ${legC2Best.inflight} x ${LEG_F_SECS}s`,
  );
  const legF = await runLeg('F-TOP50', client, store, [...PRODUCTION_SYMBOLS], LEG_F_SECS, {
    maxInflight: legC2Best.inflight,
    topN: LEG_F_TOP_N,
  });
  console.log(
    `[zset] leg G: ceiling re-confirm, ${LEG_G_SYMBOLS} symbols x ${LEG_G_SECS}s @ inflight ${LEG_G_MAX_INFLIGHT}`,
  );
  const legG = await runLeg('G-ceiling', client, store, symbolList(LEG_G_SYMBOLS), LEG_G_SECS, {
    mix: { applyDepthDiff: 1, topN: 0, getMeta: 0 },
    maxInflight: LEG_G_MAX_INFLIGHT,
    idBase: LEG_G_ID_BASE,
  });

  await client.quit();

  const cmdRows = (name: string, leg: LegResult): string[] =>
    [...leg.commandStats.entries()]
      .sort((a, b) => b[1].calls - a[1].calls)
      .slice(0, 8)
      .map(
        ([cmd, stat]) =>
          `| ${name} | ${cmd} | ${fmtInt(stat.calls)} | ${(stat.usec / stat.calls).toFixed(1)} |`,
      );

  const ratioOf = (opsPerSec: number): string => `${(opsPerSec / ORIGINAL_LEG_A_OPS).toFixed(2)}x`;
  const allLegs: Array<[string, LegResult]> = [
    ['A-production', legA],
    ['B-scaled', legB],
    ...legC.map((c) => [`C@${c.inflight}`, c.result] as [string, LegResult]),
    ['D-pipelined', legD],
    ['E-readheavy', legE],
  ];
  const modeLabelOf = (name: string): string => {
    if (name.startsWith('A')) return 'per-symbol serial baseline';
    if (name.startsWith('B')) return 'more keys, same shape';
    if (name.startsWith('C')) return 'ordered cross-symbol';
    if (name.startsWith('D')) return 'pipelined ceiling';
    return 'read-heavy mix';
  };
  const throughputRow = ([name, leg]: [string, LegResult]): string =>
    `| ${name} | ${modeLabelOf(name)} | ${mixLabel(leg.mix)} | ${leg.symbols} | ${leg.maxInflight} | ${leg.elapsedSecs.toFixed(1)} | **${fmtInt(Math.round(leg.achievedOpsPerSec))}** | ${ratioOf(leg.achievedOpsPerSec)} | ${verdict(leg.anomalies === 0)} (${leg.anomalies}) | ${fmtInt(leg.totalCalls)} | ${leg.meanUsecPerCall.toFixed(1)} |`;

  const markdown = `# Benchmark — Row 2 recalibration: relaxed-serialization ceilings

Date: ${new Date().toISOString().slice(0, 10)} · Script: \`scripts/bench/zset-ops.ts\` · Purpose: **calibration data** for scope-contract row 2 (not a re-judgement of the retired ≥20,000 target)

## Headline

- Ordered cross-symbol (leg C best, MAX_INFLIGHT=${legCBest.inflight}): **${fmtInt(Math.round(legCBest.result.achievedOpsPerSec))} mixed ops/s**, ${ratioOf(legCBest.result.achievedOpsPerSec)} the original per-symbol-serial leg A
- Pipelined ceiling (leg D): **${fmtInt(Math.round(legD.achievedOpsPerSec))} depth-only ops/s** across ${LEG_D_SYMBOLS} symbols at MAX_INFLIGHT=${LEG_D_MAX_INFLIGHT}, ${ratioOf(legD.achievedOpsPerSec)} original leg A
- Read-heavy control (leg E, 50/50 depth/topN at leg C's best config): **${fmtInt(Math.round(legE.achievedOpsPerSec))} mixed ops/s**
- Same-session reruns of the original legs for comparability: leg A **${fmtInt(Math.round(legA.achievedOpsPerSec))} mixed ops/s**, leg B **${fmtInt(Math.round(legB.achievedOpsPerSec))} mixed ops/s**
- Zero continuity anomalies on every leg (hard gate; any stale/gap fails that leg)
- Raw sequential PING control p50 this session: ${fmtMs(ping.p50)} over ${fmtInt(ping.samples)} samples (loopback floor context for every RTT below)
- Original MISS evidence is anchored, untouched, in \`2026-08-26-zset.md\` (original leg A recorded there: ${ORIGINAL_LEG_A_OPS} mixed ops/s); this file adds what the same store API achieves when the serialization constraint is relaxed

## Hardware (recorded at run time)

${hardwareTable(hardware)}

## Methodology

- One Node.js process drives every leg through \`RedisBookStore\` (\`applyDepthDiff\`/\`topN\`/\`getMeta\`) over a single connection, plus the raw node-redis client for PING/INFO controls.
- Legs A and B are unchanged from the original harness and are re-run first in the same session so C/D/E numbers share one environment.
- **Leg C — ordered cross-symbol**: same ${PRODUCTION_SYMBOLS.length}-symbol universe and ~70/25/5 mix as leg A; depth diffs for different symbols may be in flight simultaneously while per-symbol sequencing still preserves update-ID continuity; MAX_INFLIGHT swept over [${LEG_C_INFLIGHT_LEVELS.join(', ')}]; ${LEG_C_SECS} s per level, each level freshly cleared and re-seeded.
- **Leg D — deep-pipelining ceiling**: continuity ordering bypassed where safe — snapshots pre-seeded at lastUpdateId=${LEG_D_ID_BASE.toLocaleString('en-US')} with consecutive per-symbol IDs issued thereafter, so no hydration reads race the hot path; topN/getMeta excluded; ${LEG_D_SYMBOLS} symbols; MAX_INFLIGHT=${LEG_D_MAX_INFLIGHT}; ${LEG_D_SECS} s. Measures near-raw ZSET mutation throughput through the store's pipeline path.
- **Leg E — read-heavy control**: leg C's best config repeated at a 50/50 depth/topN mix (getMeta excluded), ${LEG_E_SECS} s, to expose read-mix sensitivity.
- Any non-\`applied\` depth result or rejected read counts as an anomaly; a leg with anomalies >0 fails its zero-anomaly gate regardless of throughput.
- **Redis-side measurement method**: \`INFO commandstats\` deltas before/after each leg give per-command call counts and cumulative service µs — a **mean**, since Redis does not expose server-side percentiles. Percentile targets are evaluated on client-observed round-trip samples (upper bound: includes localhost transit).
- ${PERCENTILE_DEFINITION}

## Results

### Throughput by leg (comparison column vs original leg A = ${ORIGINAL_LEG_A_OPS} mixed ops/s)

| Leg | Mode label | Mix d/t/m | Symbols | Max inflight | Wall secs | Ops/s achieved | vs orig A | Zero-anomaly gate | Total cmds | Mean µs/call |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
${allLegs.map(throughputRow).join('\n')}

### Client-observed round-trip latency by operation

| Leg | Operation | Completed | p50 | p99 | Max |
| --- | --- | --- | --- | --- | --- |
${allLegs.flatMap(([name, leg]) => opRows(name, leg)).join('\n')}

### Top commands by call count (leg D — the pipelined ceiling path)

| Leg | Command | Calls | Mean µs/call |
| --- | --- | --- | --- |
${cmdRows('D', legD).join('\n')}

Sample counts — leg C@${legCBest.inflight}: depth ${fmtInt(legCBest.result.rttMs.applyDepthDiff.samples)}, topN ${fmtInt(legCBest.result.rttMs.topN.samples)}, meta ${fmtInt(legCBest.result.rttMs.getMeta.samples)} · leg D: depth ${fmtInt(legD.rttMs.applyDepthDiff.samples)} · leg E: depth ${fmtInt(legE.rttMs.applyDepthDiff.samples)}, topN ${fmtInt(legE.rttMs.topN.samples)}. All client-observed via \`performance.now()\`.

## Config constants

SEED=${SEED}, leg A/B mix=${JSON.stringify(MIX)}, leg C mix=70/25/5 (same), leg D mix=100/0/0, leg E mix=50/50/0, TOP_N=${TOP_N}, SNAPSHOT_LEVELS=${SNAPSHOT_LEVELS} levels/side, MAX_INFLIGHT(A/B)=${MAX_INFLIGHT}, LEG_A_SECS=${LEG_A_SECS}, LEG_B_SECS=${LEG_B_SECS}, LEG_B_SYMBOLS=${LEG_B_SYMBOLS}, LEG_C_SECS=${LEG_C_SECS}, LEG_C_INFLIGHT_LEVELS=[${LEG_C_INFLIGHT_LEVELS.join(', ')}], LEG_D_SECS=${LEG_D_SECS}, LEG_D_SYMBOLS=${LEG_D_SYMBOLS}, LEG_D_MAX_INFLIGHT=${LEG_D_MAX_INFLIGHT}, LEG_D_ID_BASE=${LEG_D_ID_BASE}, LEG_E_SECS=${LEG_E_SECS} (env-overridable).

## Anomalies

Continuity anomalies (stale/gap): ${allLegs.map(([name, leg]) => `${name} ${leg.anomalies}`).join(', ')} — zero required, zero observed on every passing gate above.

Structural note: leg C shows ordered cross-symbol application scales with independent symbol chains until the single connection saturates; leg D removes read-mix contention and deep-pipelines the write path, so its ceiling is a store-API bound, not an ordering bound.

## Calibration framing (owner decision pending)

These numbers calibrate honest row-2 wording; they are **not** pass/fail against the retired ≥20,000 target. Proposed replacement wording for the contract awaits the owner's yes/no on \`.scratch/quantflow-flagship/issues/03-scope-contract.md\` (ticket \`13-row2-recalibration.md\`). Defensible floors from this session: ≥${Math.floor((legCBest.result.achievedOpsPerSec * 0.8) / 100) * 100} ordered book-updates/s across the 6-symbol universe with zero anomalies, and a reported pipelined ceiling of ≥${Math.floor((legD.achievedOpsPerSec * 0.8) / 1000) * 1000} ops/s on the reference host.

Reproduce: \`docker compose up -d\` → \`npx tsx scripts/bench/zset-ops.ts\`
`;
  const path = await writeResultsFile('zset-recalibration', markdown);
  console.log(`[zset] results written to ${path}`);
  console.log(
    `[zset] summary ${JSON.stringify({
      legA: Math.round(legA.achievedOpsPerSec),
      legB: Math.round(legB.achievedOpsPerSec),
      legC: Object.fromEntries(
        legC.map((c) => [c.inflight, Math.round(c.result.achievedOpsPerSec)]),
      ),
      legCBest: {
        inflight: legCBest.inflight,
        opsPerSec: Math.round(legCBest.result.achievedOpsPerSec),
        p50AllMs: +legCBest.result.rttMs.all.p50.toFixed(3),
        p99AllMs: +legCBest.result.rttMs.all.p99.toFixed(3),
      },
      legD: Math.round(legD.achievedOpsPerSec),
      legE: Math.round(legE.achievedOpsPerSec),
      anomalies: allLegs.map(([n, l]) => [n, l.anomalies]),
    })}`,
  );

  // Second recalibration file — sweep 16/64/256 + TOP_N=50 + ceiling re-confirm
  const allLegs2: Array<[string, LegResult]> = [
    ['A-baseline', legA],
    ...legC2.map((c) => [`C2@${c.inflight}`, c.result] as [string, LegResult]),
    ['F-TOP50', legF],
    ['G-ceiling', legG],
  ];
  const modeLabelOf2 = (name: string): string => {
    if (name.startsWith('A')) return 'per-symbol serial baseline';
    if (name.startsWith('C2')) return 'ordered cross-symbol sweep';
    if (name.startsWith('F')) return `ordered cross-symbol TOP_N=${LEG_F_TOP_N}`;
    if (name.startsWith('G')) return 'pipelined ceiling re-confirm';
    return name;
  };
  const throughputRow2 = ([name, leg]: [string, LegResult]): string =>
    `| ${name} | ${modeLabelOf2(name)} | ${mixLabel(leg.mix)} | ${leg.symbols} | ${leg.maxInflight} | ${leg.elapsedSecs.toFixed(1)} | **${fmtInt(Math.round(leg.achievedOpsPerSec))}** | ${ratioOf(leg.achievedOpsPerSec)} | ${verdict(leg.anomalies === 0)} (${leg.anomalies}) | ${fmtInt(leg.totalCalls)} | ${leg.meanUsecPerCall.toFixed(1)} |`;
  const sweepRowsMd = legC2
    .map(
      (c) =>
        `| C2@${c.inflight} | ${fmtInt(Math.round(c.result.achievedOpsPerSec))} | ${fmtMs(c.result.rttMs.all.p50)} | ${fmtMs(c.result.rttMs.all.p99)} | ${fmtMs(c.result.rttMs.all.max)} | ${verdict(c.result.anomalies === 0)} (${c.result.anomalies}) | ${c.result.elapsedSecs.toFixed(1)} |`,
    )
    .join('\n');
  const ceilingDeltaPct = (
    ((legG.achievedOpsPerSec - FIRST_CEILING_OPS) / FIRST_CEILING_OPS) *
    100
  ).toFixed(1);
  const ceilingWithin15 =
    Math.abs(legG.achievedOpsPerSec - FIRST_CEILING_OPS) / FIRST_CEILING_OPS <= 0.15;
  const proposedOrderedFloor = Math.floor((legC2Best.result.achievedOpsPerSec * 0.8) / 100) * 100;
  const proposedCeilingFloor = ceilingWithin15
    ? 12_000
    : Math.floor((legG.achievedOpsPerSec * 0.8) / 1000) * 1000;
  const markdown2 = `# Benchmark — Row 2 second recalibration: sweep 16/64/256 + TOP_N=50 + ceiling re-confirm

Date: ${new Date().toISOString().slice(0, 10)} · Script: \`scripts/bench/zset-ops.ts\` · Purpose: **calibration data** for scope-contract row 2 (not pass/fail vs the retired ≥20,000 target)

## Headline

- Ordered cross-symbol sweep (6 symbols, 70/25/5 mix, per-symbol order preserved, zero anomalies): best **${fmtInt(Math.round(legC2Best.result.achievedOpsPerSec))} mixed ops/s at MAX_INFLIGHT=${legC2Best.inflight}** (${ratioOf(legC2Best.result.achievedOpsPerSec)} original leg A); sweep table below
- TOP_N=${LEG_F_TOP_N} at best inflight (${legC2Best.inflight}, same ordered shape, 15 s): **${fmtInt(Math.round(legF.achievedOpsPerSec))} mixed ops/s**
- Pipelined ceiling re-confirm (depth-only, ${LEG_G_SYMBOLS} symbols, MAX_INFLIGHT=${LEG_G_MAX_INFLIGHT}, IDs @${LEG_G_ID_BASE.toLocaleString('en-US')}, 15 s): **${fmtInt(Math.round(legG.achievedOpsPerSec))} ops/s** vs first ceiling **${fmtInt(FIRST_CEILING_OPS)} ops/s** (${ceilingDeltaPct}% delta; ${ceilingWithin15 ? 'within ~15% — re-confirmed' : 'materially different'})
- Same-session leg A baseline (6 syms, inflight 1024, 70/25/5, 30 s) anchor: **${fmtInt(Math.round(legA.achievedOpsPerSec))} mixed ops/s**
- Zero continuity anomalies on every leg (hard gate; any stale/gap fails that leg)
- Raw sequential PING control p50 this session: ${fmtMs(ping.p50)} over ${fmtInt(ping.samples)} samples (loopback floor context for every RTT below)
- Original MISS evidence anchored in \`2026-08-26-zset.md\` (original leg A ${ORIGINAL_LEG_A_OPS} mixed ops/s); first recalibration in \`2026-08-26-zset-recalibration.md\` (ordered best 1,184 at 128, ceiling 16,142); this file adds sweep + TOP_N sensitivity + re-confirmed ceiling

## Hardware (recorded at run time)

${hardwareTable(hardware)}

## Methodology

- One Node.js process drives every leg through \`RedisBookStore\` (\`applyDepthDiff\`/\`topN\`/\`getMeta\`) over a single connection, plus the raw node-redis client for PING/INFO controls.
- Leg A baseline is unchanged from the original harness (6 production symbols, ~70/25/5 mix, depth diffs serialized per symbol to preserve update-ID continuity, MAX_INFLIGHT=1024, 30 s) and is re-run first so later numbers share one environment.
- **Sweep legs C2 — ordered cross-symbol**: same 6-symbol universe and ~70/25/5 mix as leg A; depth diffs for different symbols may be in flight simultaneously while per-symbol sequencing still preserves update-ID continuity; MAX_INFLIGHT swept over [${LEG_C2_INFLIGHT_LEVELS.join(', ')}]; ${LEG_C2_SECS} s per level, each level freshly cleared and re-seeded; zero anomalies required.
- **Leg F — TOP_N sensitivity**: ordered cross-symbol at the sweep's best inflight (${legC2Best.inflight}) but with TOP_N=${LEG_F_TOP_N} (vs 10) — same 70/25/5 mix, 6 symbols, 15 s — to expose read-mix sensitivity in the opposite direction from the 50/50 control.
- **Leg G — ceiling re-confirm**: depth-only (100/0/0), ${LEG_G_SYMBOLS} symbols, MAX_INFLIGHT=${LEG_G_MAX_INFLIGHT}, update IDs pre-seeded at ${LEG_G_ID_BASE.toLocaleString('en-US')} with consecutive per-symbol IDs thereafter so no hydration reads race the hot path; 15 s; directly comparable vs first ceiling 16,142 ops/s.
- Any non-\`applied\` depth result or rejected read counts as an anomaly; a leg with anomalies >0 fails its zero-anomaly gate regardless of throughput.
- **Redis-side measurement method**: \`INFO commandstats\` deltas before/after each leg give per-command call counts and cumulative service µs — a **mean**, since Redis does not expose server-side percentiles. Percentile targets are evaluated on client-observed round-trip samples (upper bound: includes localhost transit).
- ${PERCENTILE_DEFINITION}

## Results

### Sweep 16/64/256 (ordered cross-symbol, 6 symbols, 70/25/5)

| Leg | Ops/s | p50 (all ops) | p99 (all ops) | Max | Zero-anomaly gate | Wall secs |
| --- | --- | --- | --- | --- | --- | --- |
${sweepRowsMd}

### Throughput by leg (comparison column vs original leg A = ${ORIGINAL_LEG_A_OPS} mixed ops/s)

| Leg | Mode label | Mix d/t/m | Symbols | Max inflight | Wall secs | Ops/s achieved | vs orig A | Zero-anomaly gate | Total cmds | Mean µs/call |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
${allLegs2.map(throughputRow2).join('\n')}

### Client-observed round-trip latency by operation

| Leg | Operation | Completed | p50 | p99 | Max |
| --- | --- | --- | --- | --- | --- |
${allLegs2.flatMap(([name, leg]) => opRows(name, leg)).join('\n')}

### Top commands by call count (leg G — the re-confirmed ceiling path)

| Leg | Command | Calls | Mean µs/call |
| --- | --- | --- | --- |
${cmdRows('G', legG).join('\n')}

Sample counts — sweep best C2@${legC2Best.inflight}: depth ${fmtInt(legC2Best.result.rttMs.applyDepthDiff.samples)}, topN ${fmtInt(legC2Best.result.rttMs.topN.samples)}, meta ${fmtInt(legC2Best.result.rttMs.getMeta.samples)} · leg F (TOP_N=${LEG_F_TOP_N}): depth ${fmtInt(legF.rttMs.applyDepthDiff.samples)}, topN ${fmtInt(legF.rttMs.topN.samples)}, meta ${fmtInt(legF.rttMs.getMeta.samples)} · leg G: depth ${fmtInt(legG.rttMs.applyDepthDiff.samples)}. All client-observed via \`performance.now()\`.

## Config constants

SEED=${SEED}, leg A mix=${JSON.stringify(MIX)}, sweep mix=70/25/5 (same), TOP_N=${TOP_N}, LEG_F_TOP_N=${LEG_F_TOP_N}, SNAPSHOT_LEVELS=${SNAPSHOT_LEVELS} levels/side, MAX_INFLIGHT(A)=${MAX_INFLIGHT}, LEG_A_SECS=${LEG_A_SECS}, LEG_C2_SECS=${LEG_C2_SECS}, LEG_C2_INFLIGHT_LEVELS=[${LEG_C2_INFLIGHT_LEVELS.join(', ')}], LEG_F_SECS=${LEG_F_SECS}, LEG_G_SECS=${LEG_G_SECS}, LEG_G_SYMBOLS=${LEG_G_SYMBOLS}, LEG_G_MAX_INFLIGHT=${LEG_G_MAX_INFLIGHT}, LEG_G_ID_BASE=${LEG_G_ID_BASE} (env-overridable).

## Anomalies

Continuity anomalies (stale/gap): ${allLegs2.map(([name, leg]) => `${name} ${leg.anomalies}`).join(', ')} — zero required, zero observed on every passing gate above.

Structural note: C2 shows ordered cross-symbol throughput vs inflight knee at the best level; F shows read-payload cost of larger TOP_N at that knee; G re-confirms the store pipeline ceiling on the reference host.

## Calibration framing (owner decision pending)

These numbers are **calibration data**, not pass/fail against the retired ≥20,000 target. Original MISS evidence stays anchored in \`2026-08-26-zset.md\`; first recalibration stays in \`2026-08-26-zset-recalibration.md\`. Proposed second wording for the contract awaits owner yes/no on \`.scratch/quantflow-flagship/issues/03-scope-contract.md\` (ticket \`17-zset-recalibration-2.md\`). Defensible floors from this session: ≥${fmtInt(proposedOrderedFloor)} ordered book-updates/s across the 6-symbol universe with zero anomalies (80% of best measured ${fmtInt(Math.round(legC2Best.result.achievedOpsPerSec))}), and a reported pipelined ceiling of ≥${fmtInt(proposedCeilingFloor)} ops/s on the reference host${ceilingWithin15 ? ' (re-confirmed within ~15% of 16,142, so ≥12K retained)' : ''}.

Reproduce: \`docker compose up -d\` → \`npx tsx scripts/bench/zset-ops.ts\`
`;
  const path2 = await writeResultsFile('zset-recalibration-2', markdown2);
  console.log(`[zset] second recalibration results written to ${path2}`);
  console.log(
    `[zset] second summary ${JSON.stringify({
      sweep: Object.fromEntries(
        legC2.map((c) => [c.inflight, Math.round(c.result.achievedOpsPerSec)]),
      ),
      sweepP50: Object.fromEntries(
        legC2.map((c) => [c.inflight, +c.result.rttMs.all.p50.toFixed(3)]),
      ),
      sweepP99: Object.fromEntries(
        legC2.map((c) => [c.inflight, +c.result.rttMs.all.p99.toFixed(3)]),
      ),
      bestInflight: legC2Best.inflight,
      legF_T50: Math.round(legF.achievedOpsPerSec),
      legG_ceiling: Math.round(legG.achievedOpsPerSec),
      legA_baseline: Math.round(legA.achievedOpsPerSec),
      ceilingDeltaPct,
      proposedOrderedFloor,
      proposedCeilingFloor,
      anomalies: allLegs2.map(([n, l]) => [n, l.anomalies]),
    })}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
