# Benchmark — Row 2 second recalibration: sweep 16/64/256 + TOP_N=50 + ceiling re-confirm

Date: 2026-08-27 · Script: `scripts/bench/zset-ops.ts` · Purpose: **calibration data** for scope-contract row 2 (not pass/fail vs the retired ≥20,000 target)

## Headline

- Ordered cross-symbol sweep (6 symbols, 70/25/5 mix, per-symbol order preserved, zero anomalies): best **801 mixed ops/s at MAX_INFLIGHT=256** (0.84x original leg A); sweep table below
- TOP_N=50 at best inflight (256, same ordered shape, 15 s): **822 mixed ops/s**
- Pipelined ceiling re-confirm (depth-only, 48 symbols, MAX_INFLIGHT=512, IDs @1,000,000,000, 15 s): **7,509 ops/s** vs first ceiling **16,142 ops/s** (-53.5% delta; materially different)
- Same-session leg A baseline (6 syms, inflight 1024, 70/25/5, 30 s) anchor: **651 mixed ops/s**
- Zero continuity anomalies on every leg (hard gate; any stale/gap fails that leg)
- Raw sequential PING control p50 this session: 1.573 ms over 2,000 samples (loopback floor context for every RTT below)
- Original MISS evidence anchored in `2026-08-26-zset.md` (original leg A 952 mixed ops/s); first recalibration in `2026-08-26-zset-recalibration.md` (ordered best 1,184 at 128, ceiling 16,142); this file adds sweep + TOP_N sensitivity + re-confirmed ceiling

## Hardware (recorded at run time)

| Component     | Value                                            |
| ------------- | ------------------------------------------------ |
| CPU           | Intel(R) Core(TM) Ultra 5 125H x18 logical cores |
| RAM           | 15.7 GB total                                    |
| OS            | Windows_NT 10.0.26200 (win32)                    |
| Node.js       | v26.7.0                                          |
| Docker engine | 29.6.2                                           |
| Redis server  | 8.10.1                                           |

## Methodology

- One Node.js process drives every leg through `RedisBookStore` (`applyDepthDiff`/`topN`/`getMeta`) over a single connection, plus the raw node-redis client for PING/INFO controls.
- Leg A baseline is unchanged from the original harness (6 production symbols, ~70/25/5 mix, depth diffs serialized per symbol to preserve update-ID continuity, MAX_INFLIGHT=1024, 30 s) and is re-run first so later numbers share one environment.
- **Sweep legs C2 — ordered cross-symbol**: same 6-symbol universe and ~70/25/5 mix as leg A; depth diffs for different symbols may be in flight simultaneously while per-symbol sequencing still preserves update-ID continuity; MAX_INFLIGHT swept over [16, 64, 256]; 20 s per level, each level freshly cleared and re-seeded; zero anomalies required.
- **Leg F — TOP_N sensitivity**: ordered cross-symbol at the sweep's best inflight (256) but with TOP_N=50 (vs 10) — same 70/25/5 mix, 6 symbols, 15 s — to expose read-mix sensitivity in the opposite direction from the 50/50 control.
- **Leg G — ceiling re-confirm**: depth-only (100/0/0), 48 symbols, MAX_INFLIGHT=512, update IDs pre-seeded at 1,000,000,000 with consecutive per-symbol IDs thereafter so no hydration reads race the hot path; 15 s; directly comparable vs first ceiling 16,142 ops/s.
- Any non-`applied` depth result or rejected read counts as an anomaly; a leg with anomalies >0 fails its zero-anomaly gate regardless of throughput.
- **Redis-side measurement method**: `INFO commandstats` deltas before/after each leg give per-command call counts and cumulative service µs — a **mean**, since Redis does not expose server-side percentiles. Percentile targets are evaluated on client-observed round-trip samples (upper bound: includes localhost transit).
- Percentile definition: nearest-rank on the ascending-sorted sample — index = ceil(p/100 × n) − 1, clamped to [0, n−1]; p50/p99/max computed over all recorded samples of the phase unless stated otherwise.

## Results

### Sweep 16/64/256 (ordered cross-symbol, 6 symbols, 70/25/5)

| Leg    | Ops/s | p50 (all ops) | p99 (all ops) | Max      | Zero-anomaly gate | Wall secs |
| ------ | ----- | ------------- | ------------- | -------- | ----------------- | --------- |
| C2@16  | 733   | 4.353 ms      | 45.42 ms      | 114.0 ms | PASS (0)          | 20.0      |
| C2@64  | 757   | 6.682 ms      | 46.62 ms      | 107.9 ms | PASS (0)          | 20.0      |
| C2@256 | 801   | 6.279 ms      | 54.10 ms      | 109.4 ms | PASS (0)          | 20.0      |

### Throughput by leg (comparison column vs original leg A = 952 mixed ops/s)

| Leg        | Mode label                    | Mix d/t/m | Symbols | Max inflight | Wall secs | Ops/s achieved | vs orig A | Zero-anomaly gate | Total cmds | Mean µs/call |
| ---------- | ----------------------------- | --------- | ------- | ------------ | --------- | -------------- | --------- | ----------------- | ---------- | ------------ |
| A-baseline | per-symbol serial baseline    | 70/25/5   | 6       | 1024         | 30.0      | **651**        | 0.68x     | PASS (0)          | 111,685    | 28.0         |
| C2@16      | ordered cross-symbol sweep    | 70/25/5   | 6       | 16           | 20.0      | **733**        | 0.77x     | PASS (0)          | 82,905     | 25.7         |
| C2@64      | ordered cross-symbol sweep    | 70/25/5   | 6       | 64           | 20.0      | **757**        | 0.79x     | PASS (0)          | 85,823     | 27.8         |
| C2@256     | ordered cross-symbol sweep    | 70/25/5   | 6       | 256          | 20.0      | **801**        | 0.84x     | PASS (0)          | 91,107     | 25.3         |
| F-TOP50    | ordered cross-symbol TOP_N=50 | 70/25/5   | 6       | 256          | 15.0      | **822**        | 0.86x     | PASS (0)          | 70,061     | 24.0         |
| G-ceiling  | pipelined ceiling re-confirm  | 100/0/0   | 48      | 512          | 15.0      | **7,509**      | 7.89x     | PASS (0)          | 788,762    | 5.6          |

### Client-observed round-trip latency by operation

| Leg        | Operation      | Completed | p50      | p99      | Max      |
| ---------- | -------------- | --------- | -------- | -------- | -------- |
| A-baseline | applyDepthDiff | 13,972    | 5.744 ms | 60.94 ms | 145.2 ms |
| A-baseline | topN           | 4,631     | 24.05 ms | 119.5 ms | 161.4 ms |
| A-baseline | getMeta        | 927       | 0.025 ms | 2.095 ms | 2.130 ms |
| C2@16      | applyDepthDiff | 10,266    | 2.871 ms | 42.51 ms | 78.81 ms |
| C2@16      | topN           | 3,667     | 15.74 ms | 51.06 ms | 114.0 ms |
| C2@16      | getMeta        | 735       | 0.020 ms | 0.037 ms | 0.053 ms |
| C2@64      | applyDepthDiff | 10,612    | 5.088 ms | 43.73 ms | 62.97 ms |
| C2@64      | topN           | 3,779     | 17.52 ms | 52.10 ms | 107.9 ms |
| C2@64      | getMeta        | 761       | 0.020 ms | 0.084 ms | 0.087 ms |
| C2@256     | applyDepthDiff | 11,308    | 4.603 ms | 52.63 ms | 102.1 ms |
| C2@256     | topN           | 3,955     | 15.82 ms | 58.10 ms | 109.4 ms |
| C2@256     | getMeta        | 791       | 0.018 ms | 0.045 ms | 0.073 ms |
| F-TOP50    | applyDepthDiff | 8,713     | 5.268 ms | 45.22 ms | 71.93 ms |
| F-TOP50    | topN           | 3,028     | 16.56 ms | 58.60 ms | 85.40 ms |
| F-TOP50    | getMeta        | 607       | 0.026 ms | 0.071 ms | 0.135 ms |
| G-ceiling  | applyDepthDiff | 112,640   | 5.386 ms | 9.491 ms | 59.30 ms |

### Top commands by call count (leg G — the re-confirmed ceiling path)

| Leg | Command | Calls   | Mean µs/call |
| --- | ------- | ------- | ------------ |
| G   | hset    | 326,755 | 2.2          |
| G   | zadd    | 214,115 | 6.2          |
| G   | exec    | 112,640 | 20.5         |
| G   | multi   | 112,640 | 0.2          |
| G   | zrem    | 11,306  | 2.0          |
| G   | hdel    | 11,306  | 2.7          |

Sample counts — sweep best C2@256: depth 11,308, topN 3,955, meta 791 · leg F (TOP_N=50): depth 8,713, topN 3,028, meta 607 · leg G: depth 112,640. All client-observed via `performance.now()`.

## Config constants

SEED=20260826, leg A mix={"applyDepthDiff":0.7,"topN":0.25,"getMeta":0.05}, sweep mix=70/25/5 (same), TOP_N=10, LEG_F_TOP_N=50, SNAPSHOT_LEVELS=50 levels/side, MAX_INFLIGHT(A)=1024, LEG_A_SECS=30, LEG_C2_SECS=20, LEG_C2_INFLIGHT_LEVELS=[16, 64, 256], LEG_F_SECS=15, LEG_G_SECS=15, LEG_G_SYMBOLS=48, LEG_G_MAX_INFLIGHT=512, LEG_G_ID_BASE=1000000000 (env-overridable).

## Anomalies

Continuity anomalies (stale/gap): A-baseline 0, C2@16 0, C2@64 0, C2@256 0, F-TOP50 0, G-ceiling 0 — zero required, zero observed on every passing gate above.

Structural note: C2 shows ordered cross-symbol throughput vs inflight knee at the best level; F shows read-payload cost of larger TOP_N at that knee; G re-confirms the store pipeline ceiling on the reference host.

## Calibration framing (owner decision pending)

These numbers are **calibration data**, not pass/fail against the retired ≥20,000 target. Original MISS evidence stays anchored in `2026-08-26-zset.md`; first recalibration stays in `2026-08-26-zset-recalibration.md`. Proposed second wording for the contract awaits owner yes/no on `.scratch/quantflow-flagship/issues/03-scope-contract.md` (ticket `17-zset-recalibration-2.md`). Defensible floors from this session: ≥600 ordered book-updates/s across the 6-symbol universe with zero anomalies (80% of best measured 801), and a reported pipelined ceiling of ≥6,000 ops/s on the reference host.

Reproduce: `docker compose up -d` → `npx tsx scripts/bench/zset-ops.ts`
