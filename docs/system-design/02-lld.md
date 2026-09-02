# LLD — QuantFlow.io Per-Module Design

> Companion to [`01-hld.md`](./01-hld.md) (C4 L1/L2, NFRs, failure modes, 1M scaling). This file is the module-level drill-down: class sketches, invariants, and the measured ceilings each seam actually hits. Read `CONTEXT.md` before naming concepts. ADRs [0001–0005](../adr/) govern the trade-offs echoed here.

---

## 1. Ingest pipeline — WS → normalizer → bounded queue → `drain()`

### Wiring

```
BinanceMarketSocket --frame--> normalizeFrame --NormalizedEvent[]--> EventPipeline --drain()--> { FanoutHub, TickBuffer, SymbolDiffBus }
```

`src/ingest/index.ts:32-57` is the glue: `socket.on('frame', f => pipeline.pushMany(normalizeFrame(f).events))`. Socket never writes directly to downstream consumers — the single `EventPipeline` is the choke point.

### Components

| Component | File | Responsibility |
|---|---|---|
| `BinanceMarketSocket` | `src/ingest/binance-socket.ts:179-404` | WS lifecycle, token-budgeted reconnect, liveness/recycle timers, `ControlRateLimiter` 250 ms |
| `normalizeFrame` / `normalizeFrames` | `src/ingest/normalize.ts:157-176` | Combined-stream unwrap → `tryAggTrade` / `tryDepthUpdate` / `tryBookTicker` dispatch; returns `{events, droppedUnknown}` — never throws on unknown frames |
| `EventPipeline` | `src/ingest/pipeline.ts:26-155` | Bounded doubly-linked FIFO `20K` bench cap (`10K` default at `:14`), `push`/`pushMany` with shed policy, `drain(): AsyncGenerator` |
| `ConnectBudget` / `BackoffScheduler` | `src/ingest/binance-socket.ts:9-80` | 300/5 min bucket + `500×2^attempt` jitter + `budgetWaitMs` |

### NormalizedEvent union (`src/ingest/types.ts:1-39`)

```ts
type NormalizedEvent = TradeTick | BestQuote | DepthDiff;
interface TradeTick  { kind:'trade'; symbol:string; price:number; qty:number; ts:number; tradeId:string }
interface BestQuote  { kind:'bbo';   symbol:string; bidPrice:number; bidQty:number; askPrice:number; askQty:number; ts:number }
interface DepthDiff  { kind:'depth'; symbol:string; ts:number; firstUpdateId:number; finalUpdateId:number; levels:DepthLevelChange[] }
// DepthLevelChange { side:'bid'|'ask'; price:number; newQty:number }
```

All downstream code switches on `kind` — venue shape is erased here (`src/ingest/normalize.ts:157-162`).

### Bounded queue policy — 20K, shed oldest droppable first

Capacity is injected (`DEFAULT_PIPELINE_CAPACITY=10_000` at `src/ingest/pipeline.ts:14`; bench ctor uses `20_000`). `push()` at `src/ingest/pipeline.ts:44-65`:

* **depth** (non-droppable): `while(size>=cap && shedOldestDroppable())` → admit if a `trade`/`bbo` was evicted, else `dropped.depth++`.
* **trade/bbo** (droppable): `if(shedOldestDroppable()) append` else `dropped[kind]++`.
* `shedOldestDroppable():44-65:125-141` scans from `head` (oldest) and evicts the first `trade`/`bbo`; `dropped[evictedKind]++` is counted.

`pipeline.counters():106-113` exposes `{accepted, droppedByKind, currentDepth, capacity}`. No frame is silently lost — it is either `accepted`, `droppedUnknown` (`src/ingest/index.ts:40`), or `droppedByKind[kind]`.

### `pipeline.drain()` — single ordered `AsyncGenerator` (`:85-95`)

```ts
async *drain(): AsyncGenerator<NormalizedEvent> {
  for(;;){
    const event = this.take(); // :75-83
    if(event) { yield event; continue; }
    if(this.closed) return;
    await this.waitForSignal(); // :143-147 waiters[] promise
  }
}
```

`src/app.ts:401-403` fans the same generator to three consumers (`hub.broadcast`, `buffer.push`, `bus.push`). Depth ordering is preserved end-to-end because there is exactly one generator instance.

### Invariant — zero silent drops

```
ticks_in == emitted + gated + throttled + queueShed
```

where `emitted=accepted`, `gated=droppedUnknown`, `throttled` is gateway/fan-out policy elsewhere, `queueShed=droppedByKind.*`. Shutdown logs all legs (`src/app.ts:352-355`).

---

## 2. Book — continuity, `BookStore` seam, Redis ZSET

### Seam: `BookStore` interface (`src/book/store.ts:19-24`)

```ts
interface BookStore {
  applySnapshot(symbol:string, bids:Level[], asks:Level[], lastUpdateId:number): Promise<void>;
  applyDepthDiff(diff:DepthDiff): Promise<'applied'|'stale'|'gap'>;
  topN(symbol:string, n:number): Promise<BookSide>;
  getMeta(symbol:string): Promise<BookMeta|undefined>;
}
```

Two impls: `MemoryBookStore` (`src/book/memory-store.ts:35-81`, `Map<number,number>` per side, hermetic tests without Docker) and `RedisBookStore` (`src/book/redis-store.ts:39-140`).

### Continuity (`src/book/continuity.ts:5-11`)

```ts
function assessContinuity(lastUpdateId:number, diff:DepthDiff): 'applied'|'stale'|'gap' {
  if(diff.finalUpdateId <= lastUpdateId) return 'stale';
  if(diff.firstUpdateId <= lastUpdateId+1 && diff.finalUpdateId >= lastUpdateId+1) return 'applied';
  return 'gap';
}
```

A `gap` forces resync — never patch. Caller in `src/app.ts:184-195,200-204` breaks the per-symbol loop → `bookResyncs++` → re-bootstrap via `bufferDiffsWhileBootstrapping`.

### `RedisBookStore` — ZSET + continuity cache

Keys per symbol (`:26-29`): `book:{symbol}:bids` (ZSET score=price), `book:{symbol}:asks` (ZSET), `book:{symbol}:qty` (hash price→qty), `book:{symbol}:meta` (hash lastUpdateId). `topN` at `src/book/redis-store.ts:116-128` does `zRangeWithScores REV` for bids + `hGetAll(qty)` in parallel.

`continuityCache` (`:39-55,84-95`): caches `lastUpdateId` in `Map<string,number>` → avoids `HGETALL meta` on every diff. Benchmark delta **−76% / 0.014 ms per op** [measured in `2026-08-26-zset.md` §3]. Single-writer-per-symbol assumption is flagged at `:87-89` — multi-instance needs leader election per symbol (fencing token reuse, ADR-0004).

Depth apply (`:98-113`) is a single `MULTI/EXEC`: per `DepthLevelChange` → `ZADD`/`ZREM` + `HSET`/`HDEL`, then `HSET meta lastUpdateId`. Snapshot apply (`:57-82`) does `DEL bids/asks/qty` then bulk `ZADD` + `HSET`.

### Throughput bound — 2×RTT×6 chains → ~1K ops/s ordered, 16K pipelined ceiling

HLD §4.2 arithmetic carries here: each ordered apply is ≈2 round-trips serialized per symbol (meta read + MULTI/EXEC). Six independent symbol chains bound the single-connection ordered path to **952 ops/s leg-A (6 symbols)** and **1,184 ops/s cross-symbol best** [measured]; the pipelined `depth-only` ceiling on one connection is **16,142 ops/s** (re-confirm **7,509 ops/s**) [measured, docs/benchmarks/2026-08-26-zset*.md]. Client RTT p50 leg-A 4.632 ms (PING floor 0.640 ms). Right scaling move is per-shard Redis, not bigger box (HLD §6).

### Bootstrap — `bufferDiffsWhileBootstrapping` (`src/book/bootstrap.ts:72-236`)

Races `fetchSnapshot(symbol)` against buffered diff stream (`Promise.race` at `:134-137`): buffers every `DepthDiff` while the REST fetch is in flight, installs snapshot via `store.applySnapshot`, then replays `pending` with staleness discard (`finalUpdateId <= snapshot.lastUpdateId`) and continuity-checked apply. Up to `maxAttempts=3` (Kraken branch handles CRC32+internal seq variant; Binance branch uses U/u). Gap or fetch failure → full resync (outer `while(attempts<maxAttempts)`).

### `SymbolDiffBus` per-symbol fan-in (`src/app.ts:61-131`)

Per-symbol `queue: DepthDiff[]` + `waiters: PullResult[]`, cap `10_000` (`src/app.ts:68`). Overflow policy (`:89-92`): `queue.length=0; overflowResync.add(symbol)` → next loop `consumeOverflowResync` (`:109-111,181`) breaks to resync. Single puller per symbol (`:96-107 next()` / `:113-122 iterable()`).

---

## 3. Store — `TickBuffer` 50K → `FlushService` 1K/200 ms → Mongo

### `TickBuffer` (`src/store/buffer.ts:30-137`)

`capacity=50_000` (`:3`). Same droppable policy as ingest but with **separate refusal** for depth when no droppable exists (`:58-65: onRefuse + refusedDepth++`) rather than silent discard. Counters (`:96-106`): `{capacity,size,accepted,droppedOldest,droppedOldestByKind,refusedDepth,refusedDroppable}`. `drainUpTo(n)` (`:76-90`) pops up to `n` from `head` and maintains `droppableCount`.

### `FlushService` (`src/store/flusher.ts:80-264`)

Trigger: `flushWhen = {maxDocs:1000, maxLatencyMs:200}` (`:11`) — staleness bound **≤200 ms** (one repeating `setInterval`, not self-scheduling chain, `:119-123`). Concurrency: `maxInFlight=2` (`:12`). Throughput ≈ `2×1000 per flush RTT`, not per interval (`:134-145 pump()` pumps retries first, then drains buffer, then `launch`).

```ts
// src/store/flusher.ts:219-243 — launch
launch(docs:TickDoc[], seq:number, attempts:number): void {
  this.inflight++; this.flushesStarted++;
  void this.sink.insertMany(docs).then(
    () => { this.docsWritten+=docs.length; this.flushesSucceeded++; recordLatency(); },
    (err) => { this.errors++; this.flushesFailed++; lastErrorMessage=...; insertRetry({seq,docs,attempts}); }
  ).finally(()=>{ this.inflight--; notifySettled(); pump(); });
}
```

Retry queue is seq-ordered `insertRetry(:245-249)`. `close()` (`:147-177`) drains `buffer + retries` with `CLOSE_MAX_ATTEMPTS_PER_BATCH=3`; excess becomes `unwrittenDocs` and throws (never silent). Latency ring: `MAX_LATENCY_SAMPLES=4096` + nearest-rank `p50/p99` (`:74-78,251-256`).

### Sink (`src/store/sink.ts:26-37`)

```ts
collection.insertMany(docs, { ordered:false, writeConcern:{w:1} })
```

Pool guard at `:9-16`: `maxPoolSize=100`, `waitQueueTimeoutMS=10s`; `maxInFlight=2` holds ≤2 connections so a single writer cannot exhaust the pool. Measured **24,001 docs/s ×600 s soak = 14.4M docs** [measured], flush p50 **45 ms** / p99 **57 ms** [measured, `2026-08-26-mongo-writer.md`].

### Doc shape (`src/store/doc.ts:31-60`)

`toDoc(event)` maps `TradeTick|BestQuote|DepthDiff` → `TickDoc` with `ts:Date`, `symbol`, `kind`, and kind-specific payload (`price/qty/tradeId`, `bidPrice/bidQty/askPrice/askQty`, or `nLevels`). Mongo time-series bucket close is **1K measurements / 125 KB** per series (`HLD §2, NFR row 4` + `03-data-model.md`).

### Invariant — zero silent drops (again)

`buffer.counters()` + `flushService.stats()` partition every tick: `accepted == docsWritten + unwritten + droppedOldest + refusedDepth + bufferDepth + maxInFlight*batch + retryQueue`. Shutdown summary prints it (`src/app.ts:299-305,352-355`).

---

## 4. Fan-out — WS `hello|trade|bbo|depth`, decoupled load-gen, 200-row tape

### `FanoutHub` (`src/fanout/index.ts:53-187`)

* **Frame encoding** `encodeEvent(:28-51)` → `{t:'trade'|'bbo'|'depth', s, ...}` — bbo uses `bp/bq/ap/aq` short keys; depth ships `n` only (levels stay in Redis/Mongo).
* **Hello** `onConnection(:135-144)` pushes `{t:'hello', stages}` immediately after `handleUpgrade`.
* **Broadcast** `broadcast(:95-100)` iterates `Set<ClientState>` and calls `#send` per client.
* **Slow-client eviction** `#send(:146-154)` → `#recordSendError(:156-168)` — `ws.send(frame, cb)` error increments per-client `sendErrors`; at `≥100` (`DEFAULT_MAX_SEND_ERRORS=100` at `:21`) → `terminate()` + `slowClientsDropped++`. Per-client isolation — one stalled socket never stalls others.

HTTP side (`:70-78,170-186`): `GET /` serves `public/index.html`; `GET /ws` upgrade only; unknown paths `404`.

### Dashboard `public/index.html:179-321`

* **Tape** `MAX_ROWS=200` (`:184`), `tapeRow(:237-253)` prepend + prune `while(childCount>MAX_ROWS) last.remove()`.
* **Rate meter** `recent:number[]` + `setInterval(250 ms) → rateEl = recent.length msgs/s` (`:214-215`).
* **Top-of-book** `topOfBook(:225-235)` derives `mid=(bp+ap)/2`, `spreadBps=(ap-bp)/mid*1e4`, flashes `flash-up/down` (`:219-223`).
* **Reconnect** `reconnect(:293-296)` → `delayMs = min(5000, 250*2^attempt++)` — jitter is server-side (Socket's `BackoffScheduler`); client cap is **5 s**. Bench note in HLD §4.3: fan-out numbers require **decoupled load-gen** — `scripts/bench/fanout.ts` spawns **two OS processes** (`fanout-server.ts` + `fanout-loadgen.ts`); in-process gen inflated p99 ~20×.

Measured: **2K p99 3 ms (64 monitored, 44K samples) · 5K probe p99 5 ms (12.7K samples)** [measured, `2026-08-26-fanout.md`].

---

## 5. Gateway — `x-api-key` / Bearer, `TenantStore qf_*`, 60/300, `derivedOnly:true`

### Auth (`src/gateway/auth.ts:29-50`)

`extractApiKey(req)` checks `x-api-key` first, then `Authorization: Bearer <token>` (case-insensitive). `authenticate(apiKey, tenantStore)` → `Tenant | undefined`. Exported via `src/gateway/index.ts:15`.

### `TenantStore` — `qf_*` keys (`src/billing/tenant.ts:28-30,276-305`)

`generateApiKey() → qf_${hex8}` (16 hex chars). `RedisTenantStore` keys: `tenant:{id}` (hash, `:351`), `tenant:apikey:{apiKey} → id` (`:354`), `tenant:index` (set, `:355`). Hermetic fallback is `MemoryTenantStore` / `FakeRedisTenantClient` (same interface, `:164-239`). TTL-free; rotation is explicit `create`/`updateTier`.

### Rate limiter (`src/gateway/rateLimiter.ts:14-197` + tiers `src/billing/tiers.ts:27-64`)

```
tierLimit: starter 60/min  (wsConnections:5)
           pro     300/min (20)
           enterprise Infinity (100, unlimited)
```

Fixed-window 60 s (`WINDOW_MS=60_000` at `:14`): `windowStart=floor(now/60s)*60s`, `resetAt=ws+60000`, `key=ratelimit:{tenantId}:{ws}` (`:175`), `INCR` on first hit → `EXPIRE 60`. Remaining `max(0, limit-count)`. Enterprise bypasses `INCR`. `MemoryRateLimiter` at `:75-103` mirrors for tests. Every rejection returns `429 {Retry-After, X-RateLimit-Remaining:0}` and is counted — no silent drop (`src/gateway/index.ts:83-92`).

### Gateway HTTP + WS (`src/gateway/index.ts:56-179`)

* **HTTP** `handleRequest(:63-120)`: extract → authenticate → `rateLimiter.check` → branch. `GET /api/market-data/*` and `/api/alerts/*` return `200 {tenant, tier, derivedOnly:true}` (`:98-112`). Unknown routes → `404` with `X-RateLimit-Remaining`. Missing/invalid key → `401` (`:64-76`). Signal boundary: external tenants never see raw ticks — only throttled Derived Signals via these routes (**ADR-0003**).
* **WS** `handleUpgrade(:122-176)`: accepts `?apiKey=` query **or** header (`:123-125`), authenticates, enforces `wsConnections` per tier via `wsCounts: Map<tenantId,number>` (`:61,146-165`) with `close` decrement. Cap comes from `getTier(tier).rateLimits.wsConnections` (`:20-24`). Sends `101 Switching Protocols` + JSON `hello` frame `{t:'hello', derivedOnly:true}` (`:167-172`); over-limit → `429` + `socket.destroy()`. The internal `FanoutHub` path (`/ws` at `src/fanout/index.ts:74-81`) is the dev-local broadcast that this gated path fronts — same `hello` frame taxonomy.

### Fakes for hermetic tests

`FakeRedis` (`src/ledger/service.ts:210-284`, dup pattern in billing/gateway): `set NX/PX`, `incr`, `eval(DELEX)`, `hSet/hGetAll`, `sAdd/sMembers`. Lets ingest/book/store/fanout/ledger/gateway all run under `vitest` without Docker.

---

## 6. Cross-cutting invariants

* **Zero silent drops — tested, not sloganeered.** Every leg that can reject exposes an explicit counter; shutdown summary (`src/app.ts:352-355`) logs them. Ledger and gateway add `X-RateLimit-Remaining` / `slowClientsDropped` to the same ledger. CI asserts `ticks == emitted + gated + throttled + shed` via `pipeline.counters()` / `buffer.counters()` / `fanoutHub.stats()`.
* **Single deployable, many logical contexts.** `src/app.ts` wires `STAGES=ingest,book,store,fanout` (`:41-55`) — HLD L2 containers are logical, not process boundaries; sharding to 1M ticks/s is by symbol hash, not by bigger box (HLD §6).
* **Evidence tagging is load-bearing.** All numbers above marked **[measured]** are from `docs/benchmarks/2026-08-26-*.md` with `scripts/bench/*.ts` scripts; a claim without a script does not count (`docs/benchmarks/README.md` methodology box + HLD §8).

## 7. See also

* HLD + capacity arithmetic + failure modes + 1M sharding: [`01-hld.md`](./01-hld.md) §§2–6.
* System-design index: [`README.md`](./README.md).
* ADRs: [0001 fixtures local](../adr/0001-recorded-fixtures-stay-local.md) · [0002 book not engine](../adr/0002-v1-is-book-state-not-a-matching-engine.md) · [0003 derived-only boundary](../adr/0003-derived-signal-boundary.md) · [0004 ledger serializability](../adr/0004-ledger-strict-serializability.md) · [0005 bounded-context split](../adr/0005-scenariox-bounded-context.md).
* Canonical terms & claim tags: [`CONTEXT.md`](../../CONTEXT.md) · [`CONTEXT-MAP.md`](../../CONTEXT-MAP.md).
