# QuantFlow.io

A real-time crypto market-data pipeline: WebSocket ingestion (Binance Spot primary — `aggTrade`, `depth@100ms`, `bookTicker` on `data-stream.binance.vision` — with Kraken Spot v2 as documented fallback), level-2 order-book state maintained in Redis sorted sets, an asynchronous MongoDB time-series batch writer, and WebSocket fan-out to dashboard subscribers. The secondary output is Evidence: engineering claims an interviewer can drill, where every number is cited, measured, or explicitly tagged `to-be-measured`.

## Status

v1 flagship **implemented and benchmarked**. All four subsystems are built, tested (172 vitest tests across 20 suites, strict TypeScript; Binance ingestion live-proven, Kraken Spot v2 fallback adapter built behind a flag-gated smoke), and measured on the reference host — results with full methodology live in [`docs/benchmarks/`](docs/benchmarks/README.md):

| Suite row          | Result                                                                                                                                                                |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ingest             | **PASS** — ≥5K normalized ticks/s sustained, zero drops through a 30K msg/s burst                                                                                     |
| WS fan-out         | **PASS** — 5,000 concurrent subscribers, p99 5 ms (decoupled load generator)                                                                                          |
| Mongo batch writer | **PASS** — 24,001 docs/s ×600 s soak (14.4M docs), zero drops/errors (flush p50 45 ms misses its 25 ms sub-target; documented, no drift)                              |
| Redis ZSET ops     | Root-caused **MISS**, recalibrated — see [2026-08-26-zset.md](docs/benchmarks/2026-08-26-zset.md) + [recalibration](docs/benchmarks/2026-08-26-zset-recalibration.md) |

Live reality check: a flag-gated smoke run against Binance's production feed ingested 19,314 frames in 60 s with zero errors, drops, or reconnects.

Beyond v1, two additional slices are built, test-proven, and not yet billed: **AI alpha-alert** (F1 queue imbalance + F2 OFI micro-batched 100 ms → Llama 3.2 1B Q4_K_M 60–80 ms, Derived Signal throttled ≤1/symbol/1–5s per ADR 0003) and **LedgerGuard** (Redpanda `ledger-{tenantId}` + strict per-account `SET NX PX 8–10 s` + fencing, idempotent `ledger_accounts`/`ledger_transactions`, `src/ledger/prototype-ledger-gateway.html` demo, 226/226 tests) — see `.scratch/quantflow-v2-ai` and `.scratch/quantflow-ledger`, plus `docs/adr/0003`/`0004`.

v1 maintains book state only. There is no matching engine, no trading, and no AI layer — by design, see [docs/adr/0002](docs/adr/0002-v1-is-book-state-not-a-matching-engine.md). Raw exchange captures never enter the repo ([ADR-0001](docs/adr/0001-recorded-fixtures-stay-local.md)).

## Dev setup

Prerequisites:

- Node.js >= 26 with npm
- Docker — Redis and MongoDB run containerized (`docker compose up -d`)
- Toolchain: TypeScript (strict), vitest, ESLint 10 (flat config), Prettier

| Command                                        | Purpose                                                                                                          |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `npm ci`                                       | Clean install from the lockfile                                                                                  |
| `npm run lint`                                 | Lint with ESLint flat config                                                                                     |
| `npm run format:check`                         | Verify Prettier formatting                                                                                       |
| `npm run typecheck`                            | Strict TypeScript check                                                                                          |
| `npm test`                                     | Run the vitest suite                                                                                             |
| `docker compose up -d`                         | Start Redis + MongoDB                                                                                            |
| `npx tsx scripts/init-mongo.ts`                | Idempotently create the time-series collection                                                                   |
| `LIVE_SMOKE=1 npx tsx scripts/live-smoke.ts`   | 60 s live ingestion against Binance (never in CI)                                                                |
| `npx tsx scripts/bench/ingest.ts`              | Benchmark: ingestion row                                                                                         |
| `npx tsx scripts/bench/zset-ops.ts`            | Benchmark: order-book ops row (+ recalibration legs)                                                             |
| `npx tsx scripts/bench/fanout.ts`              | Benchmark: fan-out row (spawns decoupled load generator)                                                         |
| `SOAK=1 npx tsx scripts/bench/mongo-writer.ts` | Benchmark: 600 s writer drift soak                                                                               |
| `npm run dev`                                  | Unified runtime: ingest → book → store → fan-out (env `STAGES`, `REPLAY_FILE`, `PORT`, `REDIS_URL`, `MONGO_URL`) |

## Repo map

| Path               | Contents                                                                                                                                                                  |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/ingest/`      | Binance socket (budgeted reconnect/backoff), normalizer, bounded pipeline, JSONL replay                                                                                   |
| `src/book/`        | Order-book state: continuity logic, memory + Redis-ZSET stores, snapshot bootstrap                                                                                        |
| `src/store/`       | Tick buffer, flush service, MongoDB time-series sink                                                                                                                      |
| `test/`            | Vitest suites — fully hermetic; no test touches a live socket or database                                                                                                 |
| `scripts/`         | Live smoke + benchmark harness (`scripts/bench/`)                                                                                                                         |
| `docs/benchmarks/` | Locked methodology + measured results (append-only evidence)                                                                                                              |
| `docs/blog/`       | Draft engineering post built from the benchmark arc                                                                                                                       |
| `CONTEXT.md`       | Domain glossary — read before naming concepts                                                                                                                             |
| `docs/adr/`        | Decision records ([0001](docs/adr/0001-recorded-fixtures-stay-local.md): fixture/legal policy; [0002](docs/adr/0002-v1-is-book-state-not-a-matching-engine.md): v1 scope) |
| `DATA_SOURCES.md`  | Per-venue data-source roles and fixture rules                                                                                                                             |

## Issue tracker

Issues live as local markdown under `.scratch/`. That directory is private and gitignored — it is never committed, and public docs refer to it only as "the local issue tracker".

## License

MIT — see [LICENSE](LICENSE).
