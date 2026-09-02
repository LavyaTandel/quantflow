# Interview talking points — QuantFlow.io flagship (promoted from local dossier)

> Promoted sanitized snapshot of `.scratch/quantflow-flagship/dossier/interview-talking-points.md:1` — private notes stripped, `[measured]`/`[cited]`/`[spec]` tags preserved. Full local history stays in `.scratch/` (gitignored).

Eleven questions an interviewer will actually drill, with speakable first-person answer sketches. Every number is tagged `[measured]` (committed benchmark record under `docs/benchmarks/`, 2026-08-26) or `[cited]`/`[spec]`. Context sentence to use verbatim if asked "measured how?": *"All numbers come from reproducible benchmark scripts run on a laptop-class machine — Intel Core Ultra 5 125H, Node v26.7.0, Dockerised Redis and MongoDB — with hardware, percentile definitions, and reproduce commands committed next to every result."*

Honesty anchors: no matching engine exists in v1 (ADR-0002 — the order book is maintained L2 state only); worker-thread risk math and Kafka exist as designed-next interfaces only, with zero performance claims attached.

---

## 1. Why sorted sets for an order book? What are the complexity bounds?

**Answer sketch:** Each price level lives in a Redis sorted set scored by price, with a companion hash holding quantity per level, so writes are ZADD at O(log N), top-N reads are ZRANGE at O(log N + M) where M is the levels returned, and metadata lookups are O(1) [spec]. One depth diff applies inside a single MULTI/EXEC pipeline — hash updates plus zadd/zrem pairs committing atomically. I chose Redis because the book is read-mostly shared state that other consumers can query without owning my process, and the complexity guarantees are hardware-independent. Scope matters here: this is maintained L2 state for querying and persistence — there is deliberately no matching engine in v1, recorded in ADR-0002.

**Depth hook:** Offer the commandstats breakdown from the ZSET benchmark — mean service time per command (zadd ~5 µs, exec ~16 µs) [measured] — and ask what changes at thousands of symbols.

## 2. How do U/u update continuity and snapshot bootstrap actually work?

**Answer sketch:** Binance depth diffs carry U, the first update ID they apply to, and u, the last. I buffer incoming diffs while fetching a REST snapshot; once it lands, I discard buffered events whose u falls at or before the snapshot's lastUpdateId, then apply the remainder strictly in ID order. Any stale or gapped sequence forces resynchronisation rather than silent application — in my benchmarks any non-applied result counts as an anomaly, and there were zero across every leg [measured]. Kraken Spot v2 is the shipped fallback: no U/u, CRC32 per frame plus an internal per-symbol sequence stamped into DepthDiff updateId fields [measured: 31 Kraken tests, 2026-08-26]. The continuity logic itself is pure functions covered by an exhaustive boundary test matrix, and stores sit behind one BookStore interface with memory and Redis implementations.

**Depth hook:** Invite them to pick a boundary case — overlapping U/u ranges, or the snapshot landing mid-buffer — and walk the decision table.

## 3. Why is depth never dropped while trades are shed under overload?

**Answer sketch:** The ingest pipeline holds a bounded queue of 20,000 events and, when overloaded, sheds oldest trade and best-quote events before ever refusing a depth diff. The reasoning: a trade is a point-in-time fact whose value decays once superseded, but a missing depth diff breaks the update-ID chain and would force an expensive full resync — so depth has priority by construction. Drops are never silent: four independent counters must all read zero — push refusals, per-kind drop counters, non-applied continuity results, and drained-versus-accepted reconciliation — and they did, across roughly 900,000 burst events [measured].

**Depth hook:** Ask me what happens if the depth path itself saturates — that's the `refusedDepth` counter plus the resync cost model.

## 4. MongoDB closes time-series buckets at 1,000 measurements or 125 KB — how does your flush design respect that?

**Answer sketch:** Exactly one bucket per series is open at a time, so bucket alignment constrains flush design more than raw batch size does. At roughly 150–250 bytes per tick document, size closure bites near 500 points, and the server re-groups each batch by symbol and sorts time-ascending itself [spec] — so oversized batches buy nothing per bucket. My FlushService drains a bounded 50,000-document buffer in ~1,000-doc unordered `insertMany` chunks with write concern w:1, at most two flushes in flight, cadence capped at maxDocs 1000 / maxLatencyMs 200. Measured over 600 seconds (SOAK=1): 24,001 docs/s sustained, flush p50 45 ms / p99 57 ms, zero silent drops [measured] (120 s run was 23,999 docs/s, p50 44 ms — superseded); the flush-p50 sub-target miss is attributed in the record to this host's virtualised disk.

**Depth hook:** Cross-flush ordering and cardinality — why grouping ticks per symbol inside each flush matters as the symbol universe grows.

## 5. How did you handle backpressure? What does the driver's waitQueueTimeoutMS have to do with it?

**Answer sketch:** The MongoDB Node driver queues connection waiters without a cap, and waitQueueTimeoutMS defaults to zero — wait forever — so its documentation puts bounding the queue squarely on the application [spec]. I took that seriously at the application layer: a bounded 50,000-doc buffer whose shedding counters are loud rather than silent, at most two flushes in flight so the pool never accumulates hidden waiters, and an unwritten-after-three-attempts counter that must stay zero. The same pattern repeats upstream in the 20,000-capacity ingest pipeline. End-of-run assertions verify the buffer drains to exactly zero, which makes the claim checkable by construction.

**Depth hook:** Contrast with silent-drop designs — ask why I consider "zero silent drops" the cheapest strong claim in the whole suite.

## 6. How does reconnection honour Binance's connection budget?

**Answer sketch:** Binance caps connection attempts at 300 per five minutes per IP, forces a recycle within 24 hours, and limits control messages to five per second per connection [cited]. Kraken posture is 150 attempts per 10 minutes per IP (Cloudflare ban) enforced by the same ConnectBudget/BackoffScheduler core [measured]. My socket layer uses exponential backoff with jitter plus a token-bucket guard, so even a crash-loop cannot burn either IP budget, and a watchdog detects silent stalls that a clean close would never signal. The flag-gated live smoke then ran 60 seconds against the production stream: 19,314 frames received with zero parse errors, unknown-frame drops, reconnects, or pipeline drops [measured].

**Depth hook:** Why jitter specifically — thundering-herd behaviour after shared-infrastructure outages — and how the 24-hour forced recycle gets scheduled safely.

## 7. Walk me through your fan-out measurement. Why a separate load generator?

**Answer sketch:** The load generator runs as a separate OS process with its own event loop, because Evil Martians' June 2026 five-stack study showed a generator sharing the server's saturated event loop inflated apparent p99 roughly twenty-fold — 223 ms reported where true server-side p99 was 11 ms [cited]. Clients compute receive-minus-send timestamp echoes on one host clock; a monitored subset of 64 connections carries latency instrumentation while every connection counts deliveries, and sequence numbers detect dropped frames. Result: 2,000 concurrent subscribers at end-to-end p99 3.000 ms, with a capability probe at 5,000 subscribers reaching p99 5.000 ms, zero connect failures or gaps [measured]. The server separately records broadcast-wave completion — kernel-buffer flush, explicitly not application-level acknowledgement.

**Depth hook:** Why the echo is not a TCP ACK, and what breaks when sender and receiver clocks split across hosts.

## 8. Tell me about a performance investigation that changed your mind.

**Answer sketch:** My first store version read continuity metadata via hgetall before each depth diff — correctness-first, flagged at build time. When the production-universe benchmark landed near 952 mixed ops/s against a 20,000 target, hypothesis one was that read: a client-side continuity cache cut hot-path hgetall calls by 76% and getMeta p50 to 0.014 ms — and re-measurement moved throughput not at all [measured]. By elimination the bound was structural: ordered per-symbol application costs roughly two sequential round trips per diff across six symbol chains, and the raw sequential PING control itself ran 0.274–0.640 ms p50 between sessions on virtualised loopback — arithmetic landing exactly at the observed 1–2K ops/s. Follow-up calibration legs measured a 16,142 ops/s pipelined store-path ceiling and 1,184 ordered cross-symbol mixed ops/s, zero anomalies everywhere [measured], and I renegotiated the contract wording with that data rather than quietly retiring the target. The transferable lesson: a figure calibrated on what a server can absorb says little about what one ordered client can complete.

**Depth hook:** The leg C/D/E design that separated ordering cost from store capacity — and why the original MISS evidence stays published untouched, anchoring the analysis.

## 9. Your harness hung during benchmarking. What happened?

**Answer sketch:** During the writer benchmark the drain loop compared against a frozen stats() snapshot, so its inFlight count could never reach zero and the harness waited forever. The fix re-checks fresh stats each iteration behind a sixty-second safety cap. What interests me most is that every hermetic test had passed legitimately — fake-timed schedulers meant the defect lived in the interaction between real asynchronous flushes and a snapshot taken at the wrong moment. That's exactly why the suite ends against real Dockerised services instead of stopping at mocks: hermetic-first catches most defects cheaply, and the residue is the class only infrastructure finds. Unified runtime — `npm run dev` (`src/app.ts`) wires ingest→RedisBookStore→FlushService→FanoutHub behind `STAGES`/`REPLAY_FILE` with graceful SIGINT drain [measured].

**Depth hook:** Where else frozen-snapshot-versus-live comparisons bite — health checks, readiness probes, queue-depth-based autoscaling.

## 10. Why do recorded fixtures never enter the repository?

**Answer sketch:** Exchange terms make committing raw market data legally hazardous: Coinbase's Market Data Terms §3.2 prohibit redistribution and derived works, OKX's API Agreement §9.4 extends its ban even to unauthenticated public-endpoint data, and Binance publishes bulk archives without a clean explicit redistribution licence [cited]. So ADR-0001 keeps the repo to venue-neutral normalised events or synthetic generators only, with live captures going to a gitignored `.local-fixtures/` directory. The consequences I like: CI never touches a live socket, any clone reproduces the tests without legal exposure, and the policy is reversible today but practically impossible to reverse once raw bytes enter public git history.

**Depth hook:** Replay fixtures flow through the identical normaliser and pipeline that live sockets feed — ask about the FakeRedis and fake-timed-scheduler test strategy that made hermetic CI possible.

## 11. Why didn't you just build on ccxt?

**Answer sketch:** I evaluated it seriously and with real respect — ccxt is the category anchor, around 43,000 GitHub stars, a unified REST+WS API across 100-plus exchanges, with Kraken fixes shipping weekly [cited]. But my destination was ownership plus drillability: every claim I make about reconnection budgets, continuity, and zero silent drops lives at the ingestion boundary, and adopting ccxt hands that boundary to its symbol model and ArrayCache buffering. Hand-building kept that interview surface mine — and it wasn't wasted effort, because the socket core (token-bucket budget, jittered backoff, rate limiting) is venue-neutral and gets reused wholesale, which is exactly why venue #2 — Kraken Spot v2 (book+trades+ticker, CRC32-verified, landed 2026-08-26) — was cheap mapping work rather than new plumbing [measured]. I've documented the trigger where this flips: past roughly three venues, per-venue hand-rolling stops being cheap and ccxt — MIT-clean — becomes the rational runtime choice. That's engineering judgment with a written exit clause, not not-invented-here.

**Depth hook:** Turn the question around and ask me "which parts WOULD you adopt?" — post-v1 worker-pool math is the honest answer, where a thin own-wrapper over `worker_threads` comes first and piscina (MIT) is the documented fallback once pool-lifecycle complexity exceeds the math itself.
