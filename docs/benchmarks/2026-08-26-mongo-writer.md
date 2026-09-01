# Benchmark — Row 4: MongoDB batch writer

Date: 2026-08-26 · Script: `scripts/bench/mongo-writer.ts` · Verdict: **MISS** · **SOAK VARIANT**

## Headline

- Sustained write rate: **24,001 docs/s** over 600s (target ≥20,000)
- Flush latency: **p50 45.00 ms / p99 57.00 ms / max 65.00 ms** per ~1,000-doc unordered insertMany (targets ≤25 ms / ≤250 ms)
- Silent drops: **0** (must be 0); driver errors: 0; batches retried: 0

## Hardware (recorded at run time)

| Component      | Value                                            |
| -------------- | ------------------------------------------------ |
| CPU            | Intel(R) Core(TM) Ultra 5 125H x18 logical cores |
| RAM            | 15.7 GB total                                    |
| OS             | Windows_NT 10.0.26200 (win32)                    |
| Node.js        | v26.7.0                                          |
| Docker engine  | 29.6.2                                           |
| MongoDB server | 8.2.12                                           |

## Methodology

- Synthetic normalized events (6 symbols, mix ≈70/20/10 trade/bbo/depth, seeded RNG SEED=20260826) are paced into a bounded `TickBuffer` (capacity 50,000) by a drift-corrected timer at 24,000 events/s — 20% headroom over the 20K target so the writer, not the feeder, is the bottleneck.
- `FlushService` drains the buffer in ~1,000-doc chunks into MongoDB time-series collection `quantflow.ticks` via unordered `insertMany`, write concern w:1, at most 2 flushes in flight (bounded backpressure per findings/06 P1). Collection created by `scripts/init-mongo.ts` (timeField ts, metaField symbol, granularity seconds → buckets close at 1,000 measurements / 125 KB server-side).
- Throughput is computed from `docsWritten` delta after a 5s warmup, divided by measured wall seconds; the tail of the run includes full buffer drain before `close()`.
- Flush latency percentiles come from FlushService's own instrumentation (`scheduler.now()` around each `insertMany`), which keeps a rolling window of the most recent 4,096 completed flushes; on a run of this length that window covers the steady state but not the earliest warmup flushes.
- Zero-silent-drops assertion sums four counters that must all be zero: `droppedOldest`, `refusedDepth`, `refusedDroppable`, `unwritten` (docs abandoned after 3 failed attempts), plus zero driver errors.
- Percentile definition: nearest-rank on the ascending-sorted sample — index = ceil(p/100 × n) − 1, clamped to [0, n−1]; p50/p99/max computed over all recorded samples of the phase unless stated otherwise.
- SOAK=1 variant: 600 s sustained run to check for drift (checkpoint/journal spikes) over time.
- Feed events ≙ measurement docs one-to-one; depth events store level counts only (matching `toDoc`), so doc sizes (~150–250 B) match production shapes.

## Results

| Metric                               | Value             | Target  | Verdict |
| ------------------------------------ | ----------------- | ------- | ------- |
| Docs written                         | 14,400,394        | —       | —       |
| Measured write rate                  | 24,001 docs/s     | ≥20,000 | PASS    |
| Flush p50                            | 45.00 ms          | ≤25 ms  | MISS    |
| Flush p99                            | 57.00 ms          | ≤250 ms | PASS    |
| Flush max                            | 65.00 ms          | —       | —       |
| Flushes started / succeeded / failed | 28781 / 28781 / 0 | —       | —       |
| Silent drops + unwritten             | 0                 | 0       | PASS    |
| Buffer depth at end                  | 0                 | 0       | —       |

Sample count for latency percentiles: 4,096 flushes.

## Config constants

SEED=20260826, FEED_RATE=24,000, DURATION_SECS=600, WARMUP_MS=5000, BUFFER_CAPACITY=50,000, MAX_IN_FLIGHT=2, flushWhen={maxDocs:1000, maxLatencyMs:200}, writeConcern w:1 unordered.

## Anomalies

See verdict table.

Reproduce: `docker compose up -d` → `npx tsx scripts/init-mongo.ts` → `npx tsx scripts/bench/mongo-writer.ts`
