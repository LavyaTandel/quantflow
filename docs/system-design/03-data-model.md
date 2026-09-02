# Data Model — QuantFlow.io

> How the three stores actually lay bytes down — Redis hot-book, Mongo time-series, ledger/Mongo CAS, and tenant/rate state. Companion to [`01-hld.md`](./01-hld.md) (L2 containers, failure modes) and [`02-lld.md`](./02-lld.md) (per-module seams). ADRs [0001–0005](../adr/) anchor the "why not commit raw" and "why fence" decisions.

---

## 1. Redis — hot book + ledger fencing + tenant/rate keys

Redis is **latency-sensitive, not durable**. Single deployable owns one `ioredis`/`node-redis` connection (`src/app.ts:256-259`); book ordering assumes single-writer-per-symbol (`src/book/redis-store.ts:87-89`). Documented sharding trigger when the 1K ordered ceiling is hit is per-shard Redis (HLD §6).

### 1.1 Book ZSET keys (`src/book/redis-store.ts:26-33`)

```ts
const bidsKeyOf = (s:string) => `book:${s}:bids`;   // ZSET score=price member=String(price)
const asksKeyOf = (s:string) => `book:${s}:asks`;   // ZSET score=price member=String(price)
const qtyKeyOf  = (s:string) => `book:${s}:qty`;    // HASH  field String(price) → String(qty)
const metaKeyOf = (s:string) => `book:${s}:meta`;   // HASH  field lastUpdateId → String(int)
```

| Key | Type | Score / field | TTL | Writer |
|---|---|---|---|---|
| `book:{symbol}:bids` | **ZSET** | `score=price (number), member=String(price)` | none | `applySnapshot` / `applyDepthDiff` MULTI/EXEC (`:63-82,98-113`) |
| `book:{symbol}:asks` | **ZSET** | same | none | same |
| `book:{symbol}:qty`  | **HASH** | field `String(price)` → `String(qty)` (qty>0 only; zero → `ZREM`+`HDEL`) | none | same |
| `book:{symbol}:meta` | **HASH** | field `lastUpdateId` → `String(int)` | none | same |

* **Why ZSET + HASH?** ZSET gives `O(log N)` level insert/delete + ordered `zRangeWithScores REV` for `topN` (`:118-128`); quantity is a separate hash so float qty never lives in the score (price is exact as `score`). `topN` does `ZRANGE(withScores) + HGETALL qty` in parallel and joins in the client.
* **Snapshot invariant:** `applySnapshot` first `DEL bids,asks,qty` (`:66`) then bulk `ZADD`+`HSET`; snapshot is **not** incremental — it replaces the side.
* **Depth invariant:** `applyDepthDiff` checks continuity **before** opening MULTI (`:96`). Per `DepthLevelChange`: if `newQty==0` → `ZREM+HDEL`, else `ZADD+HSET` (`:102-108`), then `HSET meta lastUpdateId=finalUpdateId` (`:110`). One `EXEC` per diff.
* **Continuity cache** (`:39-55,84-95`): `#lastUpdateIds: Map<symbol,number>` caches `lastUpdateId` → skips `HGETALL meta` on every diff. Measured **−76% / 0.014 ms** vs uncached [2026-08-26-zset.md §3]. Hydrated lazily; gap when key absent → resync.
* **Pending Kraken branch:** Kraken uses internal `seq` + `CRC32` checksum vs Binance `U/u` (`02-lld.md` §2 + `bootstrap.ts` maxAttempts + `src/ingest/kraken/` when flag-gated).

### 1.2 Ledger fencing — `ledger:lock:*` + `ledger:fence:*` (`src/ledger/redlock.ts`)

```
ledger:lock:{tenantId}:{accountId}:{asset}   STRING  value = randomUUID() + ":" + fencingToken   PX 8000–10000   SET NX
ledger:fence:{tenantId}:{accountId}:{asset}  STRING  integer counter via INCR (monotonic fencing token)
```

* **Acquire** (`src/ledger/redlock.ts:63-104`): `SET lock NX PX ttl` with `randomPart` → `INCR fence` (only holder increments) → CAS `EVAL` updating lock value to `randomPart:fencingToken` with `PX ttl`. If CAS returns `nil` → throw `lock not acquired (fencing race)`.
* **Release** (`DELEX_LUA` at `:17-18`): `if redis.call("get",KEYS[1])==ARGV[1] then del else 0 end` — ownership-checked so a successor's lock is never deleted. `Redlock.acquireWithRetry` (`:107-127`) does **200 ms + ±100 ms jitter ×3**.
* **Check before materialize** (`src/ledger/store.ts:97-99,284-287` + `service.ts:95-99`): `fencingToken > lastFencingToken` — stale holder's produce is fenced and returns `conflict` (→ 409). Topic is `ledger-{tenantId}` keyed by `accountId:asset` (murmur2) for ordered per-account consumption (ADR-0004).

### 1.3 Tenant & rate keys

#### Tenants (`src/billing/tenant.ts:276-355`)

| Key | Type | Value |
|---|---|---|
| `tenant:{id}` | **HASH** | fields `{id,name,apiKey,tier,stripeCustomerId,stripeSubscriptionId,createdAt,venues,features}` serialized via `serializeTenant` (`:241-253`) |
| `tenant:apikey:{apiKey}` | **STRING** | `id` (`:354`, `apiKey` is `qf_${hex8}` per `:28-30`) |
| `tenant:index` | **SET** | `id` members scanned by `sMembers` for `list()` (`:355,393-409`) |
| `tenant:stripe:{customerId}` | **STRING** | `id` (`:451`, reverse index optional) |

`hGetAll` returns `{}` → `deserializeTenant` (`:255-274`) returns `undefined` → caller treats as not-found (404 vs 401 branching in `src/gateway/index.ts`).

#### Rate limiter (`src/gateway/rateLimiter.ts:14-181`)

```
ratelimit:{tenantId}:{windowStart}   STRING  integer counter via INCR, EXPIRE 60
```

* `windowStart = floor(now/60_000)*60_000`, `resetAt = windowStart+60_000` (`:22-24`).
* `check()` at `:160-181`: `tierLimit` (60 starter / 300 pro / `Infinity` enterprise, `src/billing/tiers.ts:34-63`) → `INCR key`; if `count==1` then `EXPIRE 60` (`:176-177`); `allowed = count <= limit`, `remaining = max(0, limit-count)`. Enterprise bypasses `INCR` (`:166-167`).
* Per-tenant WS caps (`src/gateway/index.ts:20-24,61-65,146-165`): `wsCounts: Map<tenantId,number>` with `close` decrement; not persisted — process-local gate that the Redis limiter fronts.
* Fakes: `InMemoryRateLimiterRedis` (`:34-71`) and `FakeRedisRateLimiterClient` mirror `incr/expire/get/flush` with `Date.now()`-checked expirations for hermetic tests.

### 1.4 Continuity cache vs Redis `HGETALL` cost

The only `HGETALL` costs that remain on the hot path are `getMeta()` for a cold symbol and `topN` for dashboard queries; every in-order `applyDepthDiff` hits the `Map` cache (`:49-54`) and skips the round-trip entirely. When the book sharding move lands (HLD §6), place `{book:{symbol}:*}` behind hash-tag `{symbol}` so the `MULTI` stays single-slot; otherwise split reads across ZSET node + qty/meta node need CAS per slot.

---

## 2. Mongo — time-series `ticks` + ledger collections

Mongo is **durable, not hot** — single `MongoClient` (`src/store/sink.ts:18-24`, `maxPoolSize=100, waitQueueTimeoutMS=10s`) fronted by a bounded `FlushService` (`maxInFlight=2`). The only write path is `insertMany unordered w:1` (`src/store/sink.ts:31-34`).

### 2.1 Time-series `ticks`

Logical collection `quantflow.ticks` (`src/app.ts:28-30`, env `MONGO_DB`/`MONGO_COLLECTION`). No explicit `createCollection` in v1 code — the `mongoSink` inserts and Mongo auto-creates; the intended time-series DDL (documented target when infra is provisioned) is:

```js
db.createCollection("ticks", {
  timeseries: {
    timeField: "ts",          // TickDoc.ts: Date
    metaField: "symbol",      // shard / series key
    granularity: "seconds"    // 30-100 ms tick spacing → seconds avoids per-ms buckets
  },
  expireAfterSeconds: 604800  // 7-day retention example — HLD §6 recommends TTL to bound 1M/s growth
});
// Recommended indexes (time-series auto-buckets on {ts,symbol}):
// db.ticks.createIndex({ symbol: 1, ts: 1 })
// db.ticks.createIndex({ kind: 1, ts: 1 })   // optional: filter trade/bbo/depth
```

**TickDoc union** (`src/store/doc.ts:29-60`):

```ts
type TickDoc = TradeTickDoc | BboTickDoc | DepthTickDoc;
interface TickDocBase { ts:Date; symbol:string; kind:'trade'|'bbo'|'depth'; }
interface TradeTickDoc extends TickDocBase { kind:'trade'; price:number; qty:number; tradeId:string; }
interface BboTickDoc    extends TickDocBase { kind:'bbo'; bidPrice:number; bidQty:number; askPrice:number; askQty:number; }
interface DepthTickDoc  extends TickDocBase { kind:'depth'; nLevels:number; }
```

`toDoc(event:NormalizedEvent):TickDoc` (`:31-60`) converts `ts:number` → `ts:Date` (wall-clock at venue `T/E` or `ctx.nowMs`), `symbol` lower→upper preserved as subscribed. `DepthTickDoc` stores `nLevels` only — full level arrays live in Redis; storing them per tick would blow bucket size.

**Bucket-close semantics** (`HLD §2 / benchmarks/README.md` row 4): time-series bucket closes at **1,000 measurements or 125 KB, whichever first** — one open bucket per `(symbol)` series. That is the bound used to justify `maxDocs=1000` in `FlushService` (`src/store/flusher.ts:11`): a flush batch maps to ≈one bucket. The `ordered:false` write lets Mongo interleave batches across symbols without per-doc ordering.

**Flush tuning** measured against this collection (`docs/benchmarks/2026-08-26-mongo-writer.md`): **24,001 docs/s sustained** across 600 s (28,781 flushes, 14.4M docs, 0 failed) at `w:1`; `flush p50 45 ms / p99 57 ms` — the p50 sub-miss vs 25 ms target is documented and attributed to `w:1` ack, not contention. Switching to `w:0` (unacknowledged) or `maxInFlight>2` are the documented scaling levers to 1M ticks/s (HLD §6).

**Invariants:** every `NormalizedEvent` pushed through `TickBuffer→FlushService→mongoSink` is either `docsWritten`, `unwritten` (after 3 `close()` attempts), `droppedOldest`/`refusedDepth`/`bufferDepth`, or in-flight — **zero silent drops** (`src/store/flusher.ts:147-177`, `buffer.ts:96-106`). `ordered:false` means a mid-batch duplicate-key error (if a unique index were added) would not abort the rest of the batch.

### 2.2 Ledger collections

Three Mongo collections underpin ledger (ADR-0004). `MemoryLedgerStore` (`src/ledger/store.ts:51-195`) is the hermetic impl; `MongoLedgerStore` (`:232-441`) delegates to a `MongoDbLike` when a live `db` is injected, otherwise falls back to `Memory`.

#### `ledger_accounts`

One row per `(tenantId, accountId, asset)` — versioned balance:

```ts
// src/ledger/types.ts:6-16
interface LedgerAccount {
  tenantId:string; accountId:string; asset:string;
  balance:number; version:number;               // monotonic CAS version
  createdAt:Date; updatedAt:Date;
  lastFencingToken:number;                      // checked > incoming fencingToken
}
```

```js
db.ledger_accounts.createIndex({ tenantId:1, accountId:1, asset:1 }, { unique:true });
```

**CAS** (`:294-341`): for existing account → `updateOne({tenantId,accountId,asset,version:expectedVersion, lastFencingToken:{$lt:fencingToken}}, {$set:{balance,newBalance, version:expectedVersion+1, lastFencingToken, updatedAt}})` — `matchedCount==0` ⇒ re-read and return `{conflict:{currentVersion,currentBalance}}`. New account path tries `insertOne(...,version:1)` and treats `code 11000 duplicate` as `conflict`. `version` is the `prevVersion`/`version` chain that appears on every `Transaction` and `LedgerStatement` cursor.

#### `ledger_transactions`

Immutable event row per append — tenant-isolated, page-ordered, kafka-aware:

```ts
// src/ledger/types.ts:20-36
interface Transaction {
  id:string; tenantId:string; accountId:string; asset:string;
  type:'debit'|'credit'|'transfer'; amount:number;
  prevVersion:number; kafkaOffset?:number;
  idempotencyKey:string; fencingToken:number; transferId?:string;
  createdAt:Date;           // stored Date, serialized ISO in produce (`service.ts:132-135`)
  // version:number derived as prevVersion+1 (stored field `version` in Mongo)
}
```

```js
db.ledger_transactions.createIndex({ tenantId:1, idempotencyKey:1 }, { unique:true, expireAfterSeconds:86400 });
db.ledger_transactions.createIndex({ tenantId:1, accountId:1, asset:1, version:1 });
```

**Idempotency 24 h** (`:75-89,269-274`): compound key `(tenantId, idempotencyKey)` is unique (24 h TTL via `expireAfterSeconds` is the intended infra setting; hermetic `Memory` stores enforce via `byIdempotency: Map` at `:54` with `dedupKey = tenantId:idempotencyKey` at `:47-49`). `appendTransaction` first checks `findByIdempotencyKey` (`:75-89`/`269`) and returns the existing `{account, transaction}` without mutating balance. Duplicate `insertOne` guarded by catching `code 11000` and re-reading the winner (`:343-357`).

**Fencing token check** (`:97-99,284-287`): `if(fencingToken <= lastFencingToken && current!==undefined) return conflict` — a holder that lost its Redis lock cannot materialize after a successor advanced `lastFencingToken`. Kafka offset (`service.ts:138-146, ledger/store.ts`) is `baseOffset` from `producer.send` stored as `tx.kafkaOffset`.

**Statement & audit:** `listTransactionsFiltered({cursor,limit,fromVersion,toVersion})` (`:158-188,376-433`) is sorted by `txVersion (= prevVersion+1)` asc, cursor is last `version`, paginated `limit` default `100`, windowed export via `fromVersion..toVersion` — always filtered by `tenantId` (gateway isolation, ADR-0004 § "reuses tenant.id as topic suffix…").

#### Kafka note (ledger plane, not v1 hot path)

Topic `ledger-{tenantId}` (`src/ledger/service.ts:22-24,54`) with `key = accountId:asset` (murmur2 partitioner downstream) gives per-account ordering. The idempotent produce path is `producer.send({topic, messages:[{key,value,headers:{fencingToken,idempotencyKey}}]})` at `:138-141`; the `hermetic` fake is `FakeKafkaProducer` (`:286-306`) that records `sent[]` and returns monotonic `baseOffset`.

---

## 3. Key → query → collection map (cheat-sheet)

| Need | Where | How |
|---|---|---|
| Best 20 levels for `BTCUSDT` | `book:{symbol}:bids/asks` ZSET | `RedisBookStore.topN(symbol,20)` → `ZRANGE WITHSCORES REV 0..19` + `HGETALL qty` (`src/book/redis-store.ts:116-128`) |
| Last update id / continuity check | `book:{symbol}:meta` + `Map cache` | `getMeta` / cached `applyDepthDiff` (`:130-139,:84-97`) → `assessContinuity` (`src/book/continuity.ts:5-11`) |
| Replay original ticks for bench | `ticks` time-series | `find({symbol, ts:{$gte,$lte}}).sort({ts:1})` — `toDoc` shape (`src/store/doc.ts`) |
| Bucket-close behavior | `ticks` internals | Close at 1K/125 KB per series — `FlushService maxDocs=1000` aligns one flush → one bucket |
| Acquire per-account lock | `ledger:lock:*` / `ledger:fence:*` | `Redlock.acquire(key)` → `SET NX PX` + `INCR fence` + CAS `EVAL` (`src/ledger/redlock.ts:63-104`) |
| Strip stale fencing holder | `ledger_accounts.lastFencingToken` | `store.appendTransaction` fencing compare (`:97-99`) vs CAS filter `lastFencingToken:{$lt:fencingToken}` |
| Idempotency dedupe (24 h) | `ledger_transactions (tenantId,idempotencyKey)` | `findByIdempotencyKey` pre-check (`:67-73,255-264`) + unique index catch 11000 (`:343-357`) |
| Tenant lookup by API key | `tenant:apikey:{key}→id` + `tenant:{id}` | `RedisTenantStore.findByApiKey → findById` (`src/billing/tenant.ts:366-391`) |
| Rate-window check | `ratelimit:{tenantId}:{ws}` | `INCR+EXPIRE 60` (`src/gateway/rateLimiter.ts:175-177`) — `Infinity` enterprise skips `INCR` |

---

## 4. Idempotency & fencing — the end-to-end guarantee

```
Caller(tier, idempotencyKey) → Gateway rate-check → LedgerService.append
  1. Redlock.acquire("ledger:lock:{tenant}:{account}:{asset}", PX 9000)
  2. store.findByIdempotencyKey(tenant,key)  → hit? return same Transaction (no debit)
  3. getAccount → CAS read (prevVersion, balance, lastFencingToken)
  4. producer.send(ledger-{tenant}, key accountId:asset, value=Transaction JSON, headers fencingToken)
  5. store.appendTransaction(tx) → fencingToken>last? & prevVersion==current? & idempotency unique? → CAS updateOne
  6. release lock via DELEX ownership-checked
```

Miss at any step is a typed `conflict` (409) rather than a silent retry — callers handle the `currentVersion/currentBalance` hint explicitly (ADR-0004 : "correctness over throughput"). The `FakeRedis` at `src/ledger/service.ts:210-284` plus `FakeKafkaProducer` give a full hermetic simulation of this pipeline without Docker.

---

## 5. See also

* [`01-hld.md`](./01-hld.md) — NFRs (24K docs/s, Redis ceilings), failure modes, scaling to 1M.
* [`02-lld.md`](./02-lld.md) — per-module seams, continuity cache −76%, `EventPipeline`/`TickBuffer`/`FlushService` capacities.
* IDL & types: `src/ingest/types.ts` · `src/store/doc.ts` · `src/ledger/types.ts`.
* ADRs: [0001 fixtures](../adr/0001-recorded-fixtures-stay-local.md) · [0003 derived-only → venue entitlement mapped to tenant entitlements](../adr/0003-derived-signal-boundary.md) · [0004 ledger strict serializability](../adr/0004-ledger-strict-serializability.md).
