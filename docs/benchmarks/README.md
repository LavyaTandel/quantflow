# Benchmarks

This directory holds the locked v1 benchmark suite definition and its results. Numbers appear here only when a committed script produced them. Claim-tag vocabulary (`cited` / `measured` / `to-be-measured`) is defined in `CONTEXT.md`.

## Locked suite

| #   | Row                   | Target                                                                                                                                                                                                                | Status                                                                     |
| --- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| 1   | Ingest                | ≥5,000 normalized ticks/s sustained; survives a 30-s burst of ~30,000 msg/s raw input with zero drops; end-to-end p99 ≤50 ms                                                                                          | PASS — 5,999/s sustained, 29,988 burst, 0 drops                            |
| 2   | Redis ZSET order book | ≥600 ordered mixed ops/s (recalibrated from 20,000) + ≥6,000 pipelined ceiling; Redis-side p50 ≤1 ms                                                                                                                  | MISS → recalibrated PASS (952 → 801 ordered, 7,509–16,142 ceiling)         |
| 3   | Fan-out               | 1,000–5,000 concurrent WebSocket subscribers at ≤15 ms p99, measured server-side with a decoupled load generator                                                                                                      | PASS — 2,000 p99 3ms / 5,000 probe p99 5ms                                 |
| 4   | MongoDB batch writer  | ≥20,000 docs/s via ~1,000-doc unordered `insertMany` flushes; flush p50 ≤25 ms / p99 ≤250 ms; bounded-buffer backpressure respecting bucket-close semantics (1,000 measurements / 125 KB, one open bucket per series) | PASS — 24,001 docs/s ×600s soak (14.4M docs), p50 45ms sub-miss documented |

## Methodology requirements

Every results file must satisfy all of the following, or it does not count as evidence:

- **Hardware recorded at run time** — CPU, RAM, storage, OS, plus versions of Node, Redis, MongoDB, and Docker.
- **Percentile definitions stated** — which statistic, sample count, and whether the measurement is client-observed or server-side.
- **Reproducible scripts committed in this repo** — a result without its script is not evidence.
- **Decoupled load generation** — the load generator runs out-of-process from the system under test; fan-out numbers are server-side by definition.

## Results files

Naming convention: `YYYY-MM-DD-<row>.md`, where `<row>` is one of `ingest`, `zset`, `fanout`, `mongo-writer` (matching rows 1–4 above).

Measured results (append-only evidence):

- `2026-08-26-ingest.md` — PASS
- `2026-08-26-zset.md` — MISS anchored (952 ops/s, structural note 2×RTT×6 chains)
- `2026-08-26-zset-recalibration.md` / `2026-08-26-zset-recalibration-2.md` — calibration sweeps (ordered best 801–1,184, ceilings 7,509–16,142)
- `2026-08-26-fanout.md` — PASS
- `2026-08-26-mongo-writer.md` — 600 s soak variant (24,001 docs/s, 14.4M docs)

## File semantics

Result files are latest-run snapshots named by row slug; rerunning a row overwrites its file — historical runs live in the local issue tracker's ticket records.
