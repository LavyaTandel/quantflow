// Row 4 — MongoDB time-series batch writer: FlushService + real mongoSink
// against quantflow.ticks.
//
//   npx tsx scripts/bench/mongo-writer.ts          # 120 s run
//   SOAK=1 npx tsx scripts/bench/mongo-writer.ts   # 600 s soak variant
//
// Sustained synthetic-event feed through TickBuffer + FlushService; reports
// docs/s and flush latency percentiles; asserts zero silent drops.
import { MongoClient } from 'mongodb';
import { FlushService, createMongoClient, mongoSink } from '../../src/store/index.js';
import { TickBuffer } from '../../src/store/buffer.js';
import type { NormalizedEvent } from '../../src/ingest/types.js';
import {
  BENCH_DB,
  PacedEmitter,
  PERCENTILE_DEFINITION,
  TICKS_COLLECTION,
  collectHardwareLine,
  fmtInt,
  fmtMs,
  hardwareTable,
  makeRng,
  verdict,
  writeResultsFile,
} from './lib.js';

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017';
const SEED = 20260826;
const FEED_RATE = Number(process.env.FEED_RATE ?? 24_000); // 20% headroom over target
const DURATION_SECS = process.env.SOAK === '1' ? 600 : 120;
const WARMUP_MS = 5000;
const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT'] as const;
const BUFFER_CAPACITY = 50_000;
const MAX_IN_FLIGHT = 2;

function makeEvent(rng: () => number, seq: number): NormalizedEvent {
  const symbol = SYMBOLS[seq % SYMBOLS.length] as string;
  const roll = rng();
  if (roll < 0.7) {
    return {
      kind: 'trade',
      symbol,
      price: 64000 * (1 + (rng() - 0.5) * 0.01),
      qty: rng(),
      ts: Date.now(),
      tradeId: `t-${seq}`,
    };
  }
  if (roll < 0.9) {
    return {
      kind: 'bbo',
      symbol,
      bidPrice: 63999,
      bidQty: rng() * 3,
      askPrice: 64001,
      askQty: rng() * 3,
      ts: Date.now(),
    };
  }
  return {
    kind: 'depth',
    symbol,
    ts: Date.now(),
    firstUpdateId: seq,
    finalUpdateId: seq,
    levels: [{ side: 'bid' as const, price: 63998, newQty: rng() * 5 }],
  };
}

async function main(): Promise<void> {
  console.log('[mongo] connecting...');
  const client: MongoClient = createMongoClient(MONGO_URL);
  await client.connect();

  const db = client.db(BENCH_DB);
  const collections = await db.listCollections({ name: TICKS_COLLECTION }).toArray();
  if (collections.length === 0) {
    throw new Error(
      `collection ${BENCH_DB}.${TICKS_COLLECTION} does not exist — run \`npx tsx scripts/init-mongo.ts\` first`,
    );
  }

  const hardware = await collectHardwareLine({ mongo: client });
  const buffer = new TickBuffer<NormalizedEvent>({ capacity: BUFFER_CAPACITY });
  const service = new FlushService({
    sink: mongoSink(client, BENCH_DB, TICKS_COLLECTION),
    buffer,
    flushWhen: { maxDocs: 1000, maxLatencyMs: 200 },
    maxInFlight: MAX_IN_FLIGHT,
  });

  const rng = makeRng(SEED);
  let seq = 0;
  const emitter = new PacedEmitter(
    FEED_RATE,
    (count) => {
      for (let i = 0; i < count; i++) buffer.push(makeEvent(rng, seq++));
    },
    5,
  );

  service.start();
  emitter.start();
  console.log(`[mongo] feeding ${FEED_RATE} events/s for ${DURATION_SECS}s...`);
  await new Promise((resolve) => setTimeout(resolve, DURATION_SECS * 1000));
  emitter.stop();

  // Stop the feed, then drain everything still buffered/in-flight.
  // NOTE: stats() returns a fresh snapshot each call — never cache it for a
  // liveness condition, or inFlight freezes at its pre-loop value.
  let drainMs = 0;
  while ((service.bufferSize() > 0 || service.stats().inFlight > 0) && drainMs < 60_000) {
    service.pump();
    await new Promise((resolve) => setTimeout(resolve, 50));
    drainMs += 50;
  }
  await service.close();
  const finalStats = service.stats();

  await client.close();

  const warmupDocs = Math.ceil((WARMUP_MS / 1000) * FEED_RATE);
  const measuredDocs = finalStats.docsWritten - Math.min(warmupDocs, finalStats.docsWritten);
  const measuredRate = measuredDocs / (DURATION_SECS - WARMUP_MS / 1000);
  const silentDrops =
    finalStats.droppedOldest +
    finalStats.refusedDepth +
    buffer.counters().refusedDroppable +
    finalStats.unwritten;

  const throughputOk = measuredRate >= 20_000;
  const p50Ok = finalStats.latencyMs.p50 <= 25;
  const p99Ok = finalStats.latencyMs.p99 <= 250;
  const dropsOk = silentDrops === 0 && finalStats.errors === 0;
  const passed = throughputOk && p50Ok && p99Ok && dropsOk;

  const markdown = `# Benchmark — Row 4: MongoDB batch writer

Date: ${new Date().toISOString().slice(0, 10)} · Script: \`scripts/bench/mongo-writer.ts\` · Verdict: **${verdict(passed)}**${process.env.SOAK === '1' ? ' · **SOAK VARIANT**' : ''}

## Headline

- Sustained write rate: **${fmtInt(Math.round(measuredRate))} docs/s** over ${DURATION_SECS}s (target ≥20,000)
- Flush latency: **p50 ${fmtMs(finalStats.latencyMs.p50)} / p99 ${fmtMs(finalStats.latencyMs.p99)} / max ${fmtMs(finalStats.latencyMs.max)}** per ~1,000-doc unordered insertMany (targets ≤25 ms / ≤250 ms)
- Silent drops: **${silentDrops}** (must be 0); driver errors: ${finalStats.errors}; batches retried: ${finalStats.batchesRetried}

## Hardware (recorded at run time)

${hardwareTable(hardware)}

## Methodology

- Synthetic normalized events (${SYMBOLS.length} symbols, mix ≈70/20/10 trade/bbo/depth, seeded RNG SEED=${SEED}) are paced into a bounded \`TickBuffer\` (capacity ${fmtInt(BUFFER_CAPACITY)}) by a drift-corrected timer at ${fmtInt(FEED_RATE)} events/s — 20% headroom over the 20K target so the writer, not the feeder, is the bottleneck.
- \`FlushService\` drains the buffer in ~1,000-doc chunks into MongoDB time-series collection \`${BENCH_DB}.${TICKS_COLLECTION}\` via unordered \`insertMany\`, write concern w:1, at most ${MAX_IN_FLIGHT} flushes in flight (bounded backpressure per findings/06 P1). Collection created by \`scripts/init-mongo.ts\` (timeField ts, metaField symbol, granularity seconds → buckets close at 1,000 measurements / 125 KB server-side).
- Throughput is computed from \`docsWritten\` delta after a ${WARMUP_MS / 1000}s warmup, divided by measured wall seconds; the tail of the run includes full buffer drain before \`close()\`.
- Flush latency percentiles come from FlushService's own instrumentation (\`scheduler.now()\` around each \`insertMany\`), which keeps a rolling window of the most recent 4,096 completed flushes; on a run of this length that window covers the steady state but not the earliest warmup flushes.
- Zero-silent-drops assertion sums four counters that must all be zero: \`droppedOldest\`, \`refusedDepth\`, \`refusedDroppable\`, \`unwritten\` (docs abandoned after 3 failed attempts), plus zero driver errors.
- ${PERCENTILE_DEFINITION}${process.env.SOAK === '1' ? '\n- SOAK=1 variant: 600 s sustained run to check for drift (checkpoint/journal spikes) over time.' : ''}
- Feed events ≙ measurement docs one-to-one; depth events store level counts only (matching \`toDoc\`), so doc sizes (~150–250 B) match production shapes.

## Results

| Metric | Value | Target | Verdict |
| --- | --- | --- | --- |
| Docs written | ${fmtInt(finalStats.docsWritten)} | — | — |
| Measured write rate | ${fmtInt(Math.round(measuredRate))} docs/s | ≥20,000 | ${verdict(throughputOk)} |
| Flush p50 | ${fmtMs(finalStats.latencyMs.p50)} | ≤25 ms | ${verdict(p50Ok)} |
| Flush p99 | ${fmtMs(finalStats.latencyMs.p99)} | ≤250 ms | ${verdict(p99Ok)} |
| Flush max | ${fmtMs(finalStats.latencyMs.max)} | — | — |
| Flushes started / succeeded / failed | ${finalStats.flushesStarted} / ${finalStats.flushesSucceeded} / ${finalStats.flushesFailed} | — | — |
| Silent drops + unwritten | ${silentDrops} | 0 | ${verdict(dropsOk)} |
| Buffer depth at end | ${finalStats.bufferDepth} | 0 | — |

Sample count for latency percentiles: ${fmtInt(finalStats.latencyMs.samples)} flushes.

## Config constants

SEED=${SEED}, FEED_RATE=${fmtInt(FEED_RATE)}, DURATION_SECS=${DURATION_SECS}, WARMUP_MS=${WARMUP_MS}, BUFFER_CAPACITY=${fmtInt(BUFFER_CAPACITY)}, MAX_IN_FLIGHT=${MAX_IN_FLIGHT}, flushWhen={maxDocs:1000, maxLatencyMs:200}, writeConcern w:1 unordered.

## Anomalies

${passed ? 'None observed.' : 'See verdict table.'}

Reproduce: \`docker compose up -d\` → \`npx tsx scripts/init-mongo.ts\` → \`npx tsx scripts/bench/mongo-writer.ts\`
`;
  const path = await writeResultsFile('mongo-writer', markdown);
  console.log(`[mongo] results written to ${path}`);
  console.log(`[mongo] VERDICT: ${verdict(passed)}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
