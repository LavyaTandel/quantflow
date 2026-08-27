# Benchmark — Row 2 recalibration: relaxed-serialization ceilings

Date: 2026-08-26 · Script: `scripts/bench/zset-ops.ts` · Purpose: **calibration data** for scope-contract row 2 (not a re-judgement of the retired ≥20,000 target)

## Headline

- Ordered cross-symbol (leg C best, MAX_INFLIGHT=128): **1,184 mixed ops/s**, 1.24x the original per-symbol-serial leg A
- Pipelined ceiling (leg D): **16,142 depth-only ops/s** across 48 symbols at MAX_INFLIGHT=512, 16.96x original leg A
- Read-heavy control (leg E, 50/50 depth/topN at leg C's best config): **1,091 mixed ops/s**
- Same-session reruns of the original legs for comparability: leg A **1,051 mixed ops/s**, leg B **3,669 mixed ops/s**
- Zero continuity anomalies on every leg (hard gate; any stale/gap fails that leg)
- Raw sequential PING control p50 this session: 0.285 ms over 2,000 samples (loopback floor context for every RTT below)
- Original MISS evidence is anchored, untouched, in `2026-08-26-zset.md` (original leg A recorded there: 952 mixed ops/s); this file adds what the same store API achieves when the serialization constraint is relaxed

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
- Legs A and B are unchanged from the original harness and are re-run first in the same session so C/D/E numbers share one environment.
- **Leg C — ordered cross-symbol**: same 6-symbol universe and ~70/25/5 mix as leg A; depth diffs for different symbols may be in flight simultaneously while per-symbol sequencing still preserves update-ID continuity; MAX_INFLIGHT swept over [8, 32, 128]; 20 s per level, each level freshly cleared and re-seeded.
- **Leg D — deep-pipelining ceiling**: continuity ordering bypassed where safe — snapshots pre-seeded at lastUpdateId=1,000,000,000 with consecutive per-symbol IDs issued thereafter, so no hydration reads race the hot path; topN/getMeta excluded; 48 symbols; MAX_INFLIGHT=512; 15 s. Measures near-raw ZSET mutation throughput through the store's pipeline path.
- **Leg E — read-heavy control**: leg C's best config repeated at a 50/50 depth/topN mix (getMeta excluded), 15 s, to expose read-mix sensitivity.
- Any non-`applied` depth result or rejected read counts as an anomaly; a leg with anomalies >0 fails its zero-anomaly gate regardless of throughput.
- **Redis-side measurement method**: `INFO commandstats` deltas before/after each leg give per-command call counts and cumulative service µs — a **mean**, since Redis does not expose server-side percentiles. Percentile targets are evaluated on client-observed round-trip samples (upper bound: includes localhost transit).
- Percentile definition: nearest-rank on the ascending-sorted sample — index = ceil(p/100 × n) − 1, clamped to [0, n−1]; p50/p99/max computed over all recorded samples of the phase unless stated otherwise.

## Results

### Throughput by leg (comparison column vs original leg A = 952 mixed ops/s)

| Leg          | Mode label                 | Mix d/t/m | Symbols | Max inflight | Wall secs | Ops/s achieved | vs orig A | Zero-anomaly gate | Total cmds | Mean µs/call |
| ------------ | -------------------------- | --------- | ------- | ------------ | --------- | -------------- | --------- | ----------------- | ---------- | ------------ |
| A-production | per-symbol serial baseline | 70/25/5   | 6       | 1024         | 30.0      | **1,051**      | 1.10x     | PASS (0)          | 179,653    | 19.5         |
| B-scaled     | more keys, same shape      | 70/25/5   | 48      | 1024         | 15.0      | **3,669**      | 3.85x     | PASS (0)          | 312,743    | 6.6          |
| C@8          | ordered cross-symbol       | 70/25/5   | 6       | 8            | 20.0      | **1,033**      | 1.08x     | PASS (0)          | 116,603    | 17.2         |
| C@32         | ordered cross-symbol       | 70/25/5   | 6       | 32           | 20.0      | **1,133**      | 1.19x     | PASS (0)          | 128,220    | 17.6         |
| C@128        | ordered cross-symbol       | 70/25/5   | 6       | 128          | 20.0      | **1,184**      | 1.24x     | PASS (0)          | 134,331    | 17.6         |
| D-pipelined  | pipelined ceiling          | 100/0/0   | 48      | 512          | 15.0      | **16,142**     | 16.96x    | PASS (0)          | 1,694,692  | 3.3          |
| E-readheavy  | read-heavy mix             | 50/50/0   | 6       | 128          | 15.0      | **1,091**      | 1.15x     | PASS (0)          | 81,922     | 22.3         |

### Client-observed round-trip latency by operation

| Leg          | Operation      | Completed | p50      | p99      | Max      |
| ------------ | -------------- | --------- | -------- | -------- | -------- |
| A-production | applyDepthDiff | 22,378    | 2.945 ms | 46.63 ms | 86.62 ms |
| A-production | topN           | 7,633     | 13.76 ms | 62.12 ms | 94.26 ms |
| A-production | getMeta        | 1,526     | 0.009 ms | 0.040 ms | 0.072 ms |
| B-scaled     | applyDepthDiff | 38,852    | 3.255 ms | 107.0 ms | 192.2 ms |
| B-scaled     | topN           | 13,611    | 38.17 ms | 154.8 ms | 187.2 ms |
| B-scaled     | getMeta        | 2,749     | 0.036 ms | 0.276 ms | 0.309 ms |
| C@8          | applyDepthDiff | 14,461    | 1.053 ms | 21.52 ms | 39.55 ms |
| C@8          | topN           | 5,168     | 9.280 ms | 35.13 ms | 52.43 ms |
| C@8          | getMeta        | 1,034     | 0.009 ms | 0.026 ms | 0.081 ms |
| C@32         | applyDepthDiff | 15,866    | 1.886 ms | 34.80 ms | 72.19 ms |
| C@32         | topN           | 5,656     | 10.93 ms | 49.39 ms | 80.20 ms |
| C@32         | getMeta        | 1,136     | 0.012 ms | 0.029 ms | 0.035 ms |
| C@128        | applyDepthDiff | 16,637    | 2.932 ms | 41.27 ms | 57.66 ms |
| C@128        | topN           | 5,912     | 10.51 ms | 48.00 ms | 74.03 ms |
| C@128        | getMeta        | 1,184     | 0.012 ms | 0.029 ms | 0.045 ms |
| D-pipelined  | applyDepthDiff | 242,136   | 2.385 ms | 4.539 ms | 16.91 ms |
| E-readheavy  | applyDepthDiff | 8,238     | 7.637 ms | 37.58 ms | 51.22 ms |
| E-readheavy  | topN           | 8,130     | 10.17 ms | 42.38 ms | 57.86 ms |

### Top commands by call count (leg D — the pipelined ceiling path)

| Leg | Command | Calls   | Mean µs/call |
| --- | ------- | ------- | ------------ |
| D   | hset    | 701,958 | 0.9          |
| D   | zadd    | 459,822 | 4.5          |
| D   | multi   | 242,136 | 0.1          |
| D   | exec    | 242,136 | 12.0         |
| D   | hdel    | 24,320  | 1.2          |
| D   | zrem    | 24,320  | 1.1          |

Sample counts — leg C@128: depth 16,637, topN 5,912, meta 1,184 · leg D: depth 242,136 · leg E: depth 8,238, topN 8,130. All client-observed via `performance.now()`.

## Config constants

SEED=20260826, leg A/B mix={"applyDepthDiff":0.7,"topN":0.25,"getMeta":0.05}, leg C mix=70/25/5 (same), leg D mix=100/0/0, leg E mix=50/50/0, TOP_N=10, SNAPSHOT_LEVELS=50 levels/side, MAX_INFLIGHT(A/B)=1024, LEG_A_SECS=30, LEG_B_SECS=15, LEG_B_SYMBOLS=48, LEG_C_SECS=20, LEG_C_INFLIGHT_LEVELS=[8, 32, 128], LEG_D_SECS=15, LEG_D_SYMBOLS=48, LEG_D_MAX_INFLIGHT=512, LEG_D_ID_BASE=1000000000, LEG_E_SECS=15 (env-overridable).

## Anomalies

Continuity anomalies (stale/gap): A-production 0, B-scaled 0, C@8 0, C@32 0, C@128 0, D-pipelined 0, E-readheavy 0 — zero required, zero observed on every passing gate above.

Structural note: leg C shows ordered cross-symbol application scales with independent symbol chains until the single connection saturates; leg D removes read-mix contention and deep-pipelines the write path, so its ceiling is a store-API bound, not an ordering bound.

## Calibration framing (owner decision pending)

These numbers calibrate honest row-2 wording; they are **not** pass/fail against the retired ≥20,000 target. Proposed replacement wording for the contract awaits the owner's yes/no on `.scratch/quantflow-flagship/issues/03-scope-contract.md` (ticket `13-row2-recalibration.md`). Defensible floors from this session: ≥900 ordered book-updates/s across the 6-symbol universe with zero anomalies, and a reported pipelined ceiling of ≥12000 ops/s on the reference host.

Reproduce: `docker compose up -d` → `npx tsx scripts/bench/zset-ops.ts`
