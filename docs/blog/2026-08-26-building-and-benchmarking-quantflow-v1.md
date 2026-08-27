# Building and benchmarking QuantFlow v1: four locked rows, one honest miss, and what it taught us

_2026-08-26 · All numbers below come from committed benchmark records under `docs/benchmarks/` and the project's issue tracker. Hardware, percentile definitions, and reproduce commands are in the methodology box in section 3._

QuantFlow.io is a real-time crypto market-data pipeline with an unusual second output: career evidence. Every subsystem that ships has to produce claims an interviewer can drill, which means every claim needs a number, a method, and a date. This post is the v1 build report.

## 1. What v1 is, and what it refuses to be

Three subsystems carry claims:

- **Ingestion cluster.** A Binance Spot WebSocket service (six symbols: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, ADAUSDT across `aggTrade`, `bookTicker`, `depth@100ms`) that honours venue connection budgets, reconnects with exponential backoff plus jitter, and normalises every frame into a venue-neutral event union through a bounded queue.
- **Order book as maintained L2 state.** Redis sorted sets holding price-level state per symbol, rebuilt from REST snapshot bootstrap, updated via depth diffs with U/u update-ID continuity checking, gap detection, and resync, exposed as top-N queries.
- **Async MongoDB time-series batch writer.** A bounded 50,000-doc buffer drained as roughly 1,000-doc unordered `insertMany` batches, cadence designed against MongoDB's bucket-close semantics, with zero silent drops as a hard invariant.

Two things are deliberately absent. First, there is no matching engine: ADR-0002 records that "order book" here means maintained L2 state only, because an execution domain (orders, balances, fills) would add correctness risk that no v1 claim uses. Second, there is no AI layer; nothing in this system pretends to be intelligent, and no roadmap item was sacrificed to bolt one on. Scope-cutting is the engineering act here: each exclusion is written down with a reason, so nobody "completes" the project into something unclaimable by accident.

## 2. Hermetic first

The shared contract is `NormalizedEvent`, a venue-neutral union of trade ticks, best-quote changes, and depth diffs. Downstream modules (book store, writer, fan-out) never see venue bytes; they see normalised events. That single decision makes everything else possible: recorded fixtures replay through the same normaliser and pipeline that live sockets feed, tests run offline and deterministically, and CI never touches a live socket.

The fixture policy is legal before it is technical. ADR-0001 records why raw exchange captures never enter the repository: Coinbase's Market Data Terms of Use (§3.2) prohibit redistributing data or derived works, OKX's API Agreement (§9.4) extends its redistribution ban even to unauthenticated public-endpoint data, and Binance publishes bulk archives without a clean explicit redistribution licence. So the repo carries only venue-neutral normalised events or synthetic generators; real captures go to a gitignored `.local-fixtures/` directory. The consequence stated in the ADR: any clone reproduces the tests without legal exposure, because reversing the policy after raw bytes enter git history is practically impossible.

Hermetic does not mean fake-forever. Unit suites use injected fakes (an exact-semantics `FakeRedis`, fake-timed schedulers), and the acceptance criteria for ingestion included a flag-gated live smoke (`LIVE_SMOKE=1`) that exists precisely so hermetic confidence eventually collides with reality on purpose.

## 3. The benchmark suite: targets before results

The four rows were locked in a scope-contract ticket before any harness existed, with numbers set from a research pass against primary sources rather than from what we hoped to achieve:

- Redis capacity came from the redis-performance organisation's own ZSET harness (`redis-zbench-go`): about 55K ZADD/s sustained server-side under pipelining. These are third-party published figures recorded in internal research notes for calibration only; they say nothing about our client-side deployment.
- WebSocket latency expectations came from Evil Martians' June 2026 five-stack Node.js study: true server-side p99 of 7 to 11 ms at 10K subscribers, and the cautionary finding that a load generator sharing a saturated event loop inflated apparent p99 roughly 20-fold. That lesson directly shaped our requirement for a decoupled generator process.
- Writer cadence came from MongoDB's documented time-series bucket-close semantics (buckets close at 1,000 measurements or 125 KB, one open bucket per series), which constrain how flush ordering interacts with bucket fragmentation.

The locked targets: ingest at least 5,000 normalized ticks/s sustained, surviving a 30-second burst near 30,000 msg/s raw input with zero drops and end-to-end p99 within 50 ms; at least 20,000 mixed ZSET ops/s round-trip from Node on localhost; fan-out to 1,000–5,000 concurrent subscribers at 15 ms p99 or better; and a writer sustaining at least 20,000 docs/s with flush p50 within 25 ms and p99 within 250 ms.

### Results, 2026-08-26

| Row            | Locked target                                                             | Measured                                                                                                          | Verdict            |
| -------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------ |
| 1 Ingest       | ≥5,000 ticks/s sustained; ~30K msg/s burst ×30 s, 0 drops; e2e p99 ≤50 ms | 5,999 events/s sustained; 29,988 msg/s burst, 0 drops, 0 anomalies; p99 1.765 ms sustained / 1.510 ms burst       | PASS               |
| 3 Fan-out      | 1,000–5,000 subscribers ≤15 ms p99, decoupled load generator              | Verdict leg (2,000 subs): p99 3.000 ms, 0 connect failures, 0 sequence gaps; probe leg (5,000 subs): p99 5.000 ms | PASS               |
| 4 Mongo writer | ≥20,000 docs/s; flush p50 ≤25 ms / p99 ≤250 ms; 0 silent drops            | 24,001 docs/s over 600 s soak (14.4M docs); flush p50 45.00 ms / p99 57.00 ms / max 65.00 ms; 0 drops, 0 errors   | PASS, one sub-miss |
| 2 ZSET ops     | ≥20,000 mixed ops/s round-trip; Redis-side p50 ≤1 ms                      | Leg A 952 ops/s (first run ≈1,030); client RTT p50 4.632 ms vs raw PING control 0.640 ms                          | MISS               |

Two measurement caveats belong next to that table. Fan-out latency is what the decoupled load generator measures: a timestamp echo covering server-send to client-receive on one host clock, with no TCP ACK involved despite the row definition's "send-to-client-ack" shorthand; the server separately records broadcast-wave completion (34.10 ms p50 / 68.96 ms p99 over 722 waves at 2,000 subscribers), which measures kernel-buffer flush, not application acknowledgement. And the 5,000-subscriber leg is a capability probe, not a dossier claim; the contractual verdict leg is 2,000 subscribers. The writer's sub-miss is attributed in the record to the cost of a 1,000-doc unordered `insertMany` on this host's virtualised disk; throughput headroom was designed in (feeder paced at 24,000 events/s, 20% above target, so the writer and not the feeder is the bottleneck).

### Methodology box

**Hardware** (recorded at run time, all rows): Intel Core Ultra 5 125H, 18 logical cores; 15.7 GB RAM; Windows_NT 10.0.26200; Node.js v26.7.0; Docker engine 29.6.2; Redis server 8.10.1; MongoDB server 8.2.12 (Docker/Redis/Mongo absent from the fan-out row, which needs none).

**Percentile definition** (identical in every record): nearest-rank on the ascending-sorted sample, index = ceil(p/100 × n) − 1, clamped to [0, n−1]; computed over all recorded samples of the phase unless stated otherwise. Ingest and ZSET latencies are client-observed via `performance.now()`; ZSET additionally reports Redis `INFO commandstats` means (a mean is the only server-side statistic Redis exposes).

**Reproduce:**

```text
docker compose up -d
npx tsx scripts/bench/ingest.ts
npx tsx scripts/bench/zset-ops.ts
npx tsx scripts/init-mongo.ts
npx tsx scripts/bench/mongo-writer.ts
npx tsx scripts/bench/fanout.ts   # no Docker needed for this row
```

## 4. The two honest failures

The passes are fine. These two failures are the best part of the record.

### Row 2: when a calibrated target meets deployment shape

The production-universe ZSET leg (six symbols, depth serialised per symbol, the real deployment shape) measured about 1,030 ordered mixed ops/s against the 20,000 target. Hypothesis one: the continuity logic read metadata with `hgetall` before each diff, a correctness-first choice flagged during the store build. We fixed it with a client-side continuity cache: hot-path `hgetall` calls fell 76%, `getMeta` p50 dropped to 0.014 ms. Re-measured: throughput unchanged. The committed evidence doc records leg A at 952 mixed ops/s.

By elimination, the residual bound is ordered-application times loopback RTT: depth must apply strictly in update-ID order per symbol, so each diff costs roughly two sequential round trips inside six symbol chains, and the raw sequential PING control itself drifted from 0.274 to 0.640 ms p50 between runs on the same host (virtualised Docker loopback on Windows). At six chains that arithmetic lands near 1–2K ops/s, exactly where we are. The structural note in the record says it plainly: per-symbol serialisation bounds leg-A depth throughput at about two sequential round trips per diff times six symbol chains, the cost of ordered application, reported rather than hidden behind unordered writes.

The lesson generalises beyond Redis. Our 20,000 figure was calibrated against pipelined server-side capacity (the zbench harness feeding 50 clients at pipeline depth 100). But the deployment shape serialises writes per symbol chain, so the binding constraint is round-trip count times chains, not server capacity. A target derived from what a server can absorb says little about what one ordered client can complete. Ticket 13 now proposes a pipelined-ceiling leg and revised contract wording; the original MISS evidence stays published untouched, because it anchors the analysis.

### The harness race that only real infrastructure found

During row 4, the harness hung in an infinite wait. The drain loop compared against a frozen `stats()` snapshot, so `feedEndStats.inFlight` could never reach zero; the fix re-checks fresh stats each iteration behind a 60-second safety cap. Every hermetic test had passed, correctly: the schedulers were fake-timed and the bug lived in the interaction between real async flushes and a snapshot taken at the wrong moment. Hermetic-first catches most defects cheaply; the residue is exactly the class you find only by running against real infrastructure, which is why the suite ends with real Dockerised services rather than stopping at the mocks.

## 5. Live reality check

After the benchmarks, the flag-gated smoke ran for 60 seconds against `data-stream.binance.vision`: 19,314 frames received (1,014 trades, 15,696 best-quote changes, 2,601 depth updates), with zero parse errors, zero unknown-frame drops, zero reconnects, and zero pipeline drops of any kind. One minute proves nothing statistical, but it confirms the reconnect budget, watchdog, and bounded queue behave outside fixtures.

## 6. What's next

Row-2 recalibration (ticket 13) lands a pipelined-ceiling measurement and revised contract wording for owner sign-off; then the tranche-2 dossier upgrade turns these records into tagged claims, and the scripts and benchmark docs move to a public repo.

## Prior art, evaluated

The build started with a survey of what already exists, checked against primary sources rather than reputation. The category anchor is ccxt: a unified REST and WebSocket API over more than a hundred exchanges, around 43,700 GitHub stars and roughly 14,700 releases since 2017, with exchange-breakage fixes shipping weekly — one August release carried two Kraken patches. Hummingbot (about 19,600 stars, Apache-2.0) sits nearby as a Python market-making bot framework: a consumer of feeds like ours, not an embeddable library. NautilusTrader (about 27,800 stars) is the serious-engine end of the spectrum — best-in-class L2 modelling behind a Rust core under LGPL-3.0, neither embeddable in a TypeScript pipeline nor safe to copy into an MIT repository.

For ingestion, the adopt-vs-own call was made honestly, because adoption was genuinely attractive: ccxt delivers speed-to-correctness across many venues. It was declined for v1 on boundary grounds, not capability grounds. The expensive half of an adapter — reconnect budget, jittered backoff, bounded queue — already existed in-repo and tested, so adopting ccxt would chiefly have meant inheriting its symbol model and ArrayCache buffering at exactly the boundary where v1's claims live: normalization, continuity, and drop accounting. None of this is defended forever. The adoption trigger is written down alongside the analysis: past roughly three venues, per-venue hand-rolling stops being cheap and ccxt — MIT-clean — becomes the rational runtime choice.

_Gates at close of session: lint/format/typecheck clean, 172/172 tests passing (suite grew post-draft: hardening pass and the Kraken v2 fallback adapter landed after these benchmarks were recorded)._
