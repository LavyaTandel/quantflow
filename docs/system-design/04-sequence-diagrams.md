# Sequence Diagrams — QuantFlow.io

> Four Mermaid `sequenceDiagram`s that pin the runtime narratives sketched in [`01-hld.md`](./01-hld.md) (L2 containers + failure modes) and spelled out in [`02-lld.md`](./02-lld.md) (per-module seams) / [`03-data-model.md`](./03-data-model.md) (keys + bucket semantics). All frames are GitHub-native Mermaid — no extra deps.

---

## 1. Live ingest — Binance `aggTrade` → normalizer → pipeline → book continuity → flush → fan-out

The single ordered path: one venue frame becomes one `NormalizedEvent` through the only `drain()` generator (`src/app.ts:401-403` fans to hub/buffer/bus). Drop reasons are partitioned (`src/ingest/pipeline.ts:44-65` + `src/ingest/normalize.ts:157-162`), never silent.

```mermaid
sequenceDiagram
    participant Binance as Binance Spot<br/>data-stream.binance.vision
    participant Socket as BinanceMarketSocket<br/>src/ingest/binance-socket.ts
    participant Norm as normalizeFrame<br/>src/ingest/normalize.ts
    participant Pipe as EventPipeline<br/>cap 20K (bench) / 10K default
    participant Bus as SymbolDiffBus<br/>src/app.ts:61-131
    participant Hub as FanoutHub<br/>src/fanout/index.ts
    participant Buffer as TickBuffer<br/>cap 50K src/store/buffer.ts
    participant Flusher as FlushService<br/>1K docs / 200ms / inFlight=2
    participant Sink as mongoSink<br/>w:1 unordered
    participant Book as RedisBookStore<br/>ZSET + continuity cache

    Binance-->>Socket: WS frame (combined stream JSON)
    Socket->>Socket: JSON.parse + pong/liveness gate<br/>parseErrors++ on throw
    Socket->>Norm: frame + {nowMs}
    Norm->>Norm: tryAggTrade / tryDepthUpdate / tryBookTicker
    alt unknown / malformed
        Norm-->>Pipe: {events:[], droppedUnknown:true}<br/>counted, never thrown
    else aggTrade matched
        Norm-->>Pipe: FrameOutcome{events:[TradeTick{tradeId,price,qty,ts}]}
    end
    Pipe->>Pipe: push(event):<br/>if depth: shedOldestDroppable() else dropped.depth++<br/>if trade/bbo: shedOldestDroppable() else dropped[trade|bbo]++
    Pipe-->>Bus: push(DepthDiff) when kind=='depth'
    Pipe-->>Hub: drain() yield → broadcast(encodeEvent)<br/>encode {t:'trade',s,p,q,ts}
    Pipe-->>Buffer: drain() yield → buffer.push(event)<br/>depth: refusedDepth if no droppable else shedOldestDroppable
    Buffer-->>Flusher: pump() → drainUpTo(1000) when maxDocs or 200ms elapsed
    Flusher->>Sink: insertMany(TickDoc[], {ordered:false, w:1})
    Sink-->>Flusher: ack → recordLatency; on reject → insertRetry(seq-ordered)
    Bus-->>Book: next(symbol) → applyDepthDiff(diff)
    Book->>Book: cached lastUpdateId ?? HGETALL meta<br/>assessContinuity(U,u) → applied|stale|gap
    alt applied
        Book->>Book: MULTI: ZADD/ZREM + HSET qty + HSET meta lastUpdateId → EXEC
        Book-->>Buffer: (no-store side effect; depth flush already queued above)
    else stale
        Book-->>Bus: return 'stale' → bookStale++
    else gap
        Book-->>Bus: return 'gap' → break → resync (Diagram 3)
    end
    Hub-->>Hub: slow-client guard: sendErrors>=100 → terminate → slowClientsDropped++
```

**Notes.** Single `drain()` (`src/ingest/pipeline.ts:85-95`) preserves depth order end-to-end. `ticks==emitted+gated+throttled+queueShed` is observable via `pipeline.counters()` + `buffer.counters()` + `hub.stats()` (shutdown at `src/app.ts:352-355`).

---

## 2. Snapshot bootstrap — `bufferDiffsWhileBootstrapping` · Binance U/u vs Kraken `seq`+CRC32 (buffer → discard → apply)

Races the REST fetch against the live diff stream so no delta is lost while the snapshot is in flight (`src/book/bootstrap.ts:72-236`, `Promise.race` at `:134-137`). Kraken path swaps the continuity predicate to internal `seq` + CRC32 while keeping the same buffer-discard-apply skeleton.

```mermaid
sequenceDiagram
    participant Bus as SymbolDiffBus<br/>per-symbol queue cap 10K
    participant Boot as bufferDiffsWhileBootstrapping<br/>src/book/bootstrap.ts
    participant Fetch as SnapshotFetcher<br/>src/book/rest-snapshot.ts<br/>GET /api/v3/depth?symbol=&limit=5000
    participant Store as BookStore<br/>RedisBookStore / MemoryBookStore
    participant Kraken as (flag-gated) KrakenSocket<br/>seq + CRC32 path

    Bus->>Boot: iterable(symbol) (AsyncIterable<DepthDiff>)
    Boot->>Fetch: fetchSnapshot(symbol)  (no await yet — wraps via wrapFetch)
    Boot->>Boot: install helper: applySnapshot(bids,asks,lastUpdateId)
    loop until snapshot installed or sourceDone
        Boot->>Boot: Promise.race(fetchOutcome, iterator.next())
        alt race = diff arrived first
            Bus-->>Boot: DepthDiff{U,u,levels}<br/>pending.push(diff); bufferedWhileFetching++<br/>onEvent(diff-buffered)
        else race = fetch resolved
            Fetch-->>Boot: {ok:true, snapshot{lastUpdateId,bids,asks}}
            Boot->>Store: applySnapshot(symbol,bids,asks,lastUpdateId)<br/>DEL bids/asks/qty → ZADD batches → HSET meta
            Store-->>Boot: ok → snapshotsFetched++<br/>onEvent(snapshot-fetched)
        else fetch rejected
            Fetch-->>Boot: {ok:false, error}<br/>onEvent(snapshot-fetch-failed) → break → retry outer while
        end
    end
    Boot->>Boot: discard stale pending: while pending[i].finalUpdateId <= snapshot.lastUpdateId<br/>discardedStale++ → onEvent(diff-discarded)
    Boot->>Boot: currentLast = snapshot.lastUpdateId
    loop pending[idx..] in order
        Boot->>Boot: verdict = assessContinuity(currentLast, candidate)  [Binance path]
        alt gap (firstUpdateId > currentLast+1)
            Boot-->>Boot: gapsDetected++ → emitGap(onEvent) → slice pending from gap idx<br/>onEvent(resync-started) → outer while attempts++ → continue
            Note over Boot,Kraken: Kraken variant replaces this check with<br/>candidate.seq == expectedSeq  &&  crc32(levels) == checksum<br/>mismatch or seq gap → same resync trigger
        else stale (finalUpdateId <= currentLast)
            Boot->>Boot: skippedStale++ → continue (idempotent)
        else applied
            Boot->>Store: applyDepthDiff(candidate)
            alt store returns gap (e.g. meta diverged)
                Store-->>Boot: 'gap' → same emitGap → resync
            else store returns applied
                Store-->>Boot: 'applied' → currentLast = candidate.finalUpdateId<br/>applied++ → onEvent(diff-applied)
            end
        end
    end
    Boot-->>Bus: return {status:'synced', lastUpdateId, applied, discardedStale, elapsedMs}
    Note over Boot,Store: unsynced after maxAttempts=3 returns<br/>{status:'unsynced', reason:'max-resync-attempts-exceeded'|'snapshot-fetch-failed'}<br/>caller runSymbolBook retries in 15s (src/app.ts:170-173)
```

**Invariants.** Buffered diffs are kept **until** the snapshot's `lastUpdateId` is known; stale diffs are discarded explicitly, not silently skipped. Max `3` snapshot attempts before `unsynced`; the outer `while(attempts<maxAttempts)` at `:102-221` preserves `pending.slice(gapIdx-1)` so a gap does not lose its triggering diff.

---

## 3. Depth apply — continuity cache → ZSET `MULTI/EXEC`, gap → resync (steady-state)

The fast path after bootstrap is synced: every new `DepthDiff` flows through the in-memory continuity cache and a single Redis transaction; continuity failures bubble to a full resync (Section 2) instead of being patched.

```mermaid
sequenceDiagram
    participant Diff as DepthDiff<br/>firstUpdateId U / finalUpdateId u / levels
    participant Bus as SymbolDiffBus<br/>src/app.ts:61-131
    participant Cache as #continuityCache<br/>Map<symbol,lastUpdateId><br/>src/book/redis-store.ts:42
    participant Redis as Redis 8.10<br/>book:{symbol}:{bids|asks}: ZSET<br/>book:{symbol}:qty: HASH<br/>book:{symbol}:meta: HASH
    participant Cont as assessContinuity<br/>src/book/continuity.ts:5-11
    participant Multi as MULTI/EXEC
    participant Runner as runSymbolBook<br/>src/app.ts:149-205

    Bus->>Diff: next(symbol) / iterable yield
    Diff->>Cache: applyDepthDiff(diff) entry
    Cache->>Cache: cached = #cachedLastUpdateId(symbol)
    alt cache miss
        Cache->>Redis: HGETALL book:{symbol}:meta
        Redis-->>Cache: {lastUpdateId: String} or {} → gap if absent
        Cache->>Cache: #rememberLastUpdateId(symbol, Number(stored))
    end
    Cache->>Cont: assessContinuity(current, diff)
    alt stale (finalUpdateId <= current)
        Cont-->>Runner: return 'stale' → counters.bookStale++<br/>diff discarded, no write
    else gap (firstUpdateId > current+1 || firstUpdateId <= current but finalUpdateId stale-range mismatch)
        Cont-->>Runner: return 'gap' → break steady-state loop<br/>counters.bookGaps++ ; bookResyncs++<br/>log "continuity gap; resynchronizing"
        Runner->>Runner: bootstrap() (Diagram 2) — full snapshot + buffered replay
        Note over Runner,Redis: Overflow case is same trigger:<br/>SymbolDiffBus queue.length>=cap → queue.length=0 + overflowResync.add(symbol)<br/>next loop consumeOverflowResync() breaks to bootstrap (src/app.ts:89-92,109-111,181)
    else applied (firstUpdateId <= current+1 && finalUpdateId >= current+1)
        Cache->>Multi: MULTI
        loop per DepthLevelChange in diff.levels
            alt newQty == 0
                Multi->>Redis: ZREM book:{symbol}:{bids|asks} price<br/>HDEL book:{symbol}:qty price
            else newQty > 0
                Multi->>Redis: ZADD book:{symbol}:{bids|asks} score=price member=String(price)<br/>HSET book:{symbol}:qty [price]=String(newQty)
            end
        end
        Multi->>Redis: HSET book:{symbol}:meta lastUpdateId=String(finalUpdateId)
        Multi->>Redis: EXEC
        Redis-->>Cache: ok
        Cache->>Cache: #rememberLastUpdateId(symbol, finalUpdateId)
        Cache-->>Runner: return 'applied' → counters.bookApplied++
    end
```

**Footnotes.** Cache hit skips `HGETALL` → measured **−76% / 0.014 ms** [2026-08-26-zset.md]. The comment at `src/book/redis-store.ts:87-89` flags the **single-writer-per-symbol** assumption; multi-instance deployments need per-symbol leader election (fencing token reuse, ADR-0004). Bus overflow (`10K` at `src/app.ts:68`) and continuity gap converge on the same resync path so the book is never partially patched.

---

## 4. Flush — `buffer.drainUpTo(1K)` → `sink.insertMany` unordered `w:1` → metrics / retry / close

Persistence is the only async writer. The staleness bound (200 ms) and in-flight cap (2) make throughput `2×1K per flush RTT`, not per interval (`src/store/flusher.ts:134-145`). Failures are seq-ordered retries, never silent drops.

```mermaid
sequenceDiagram
    participant Tick as NormalizedEvent<br/>trade|bbo|depth
    participant Buf as TickBuffer<br/>50K cap src/store/buffer.ts
    participant Pump as FlushService.pump()<br/>src/store/flusher.ts:134-145
    participant Sched as Scheduler<br/>setInterval 200ms src/store/flusher.ts:119-130
    participant Sink as mongoSink<br/>src/store/sink.ts:26-37<br/>insertMany unordered w:1
    participant Mongo as MongoDB 8.2<br/>quantflow.ticks<br/>time-series bucket 1K/125KB
    participant Metrics as FlushStats<br/>src/store/flusher.ts:45-60
    participant Retry as retries: RetryEntry[]<br/>seq-ordered src/store/flusher.ts:62-67

    Tick->>Buf: push(event)<br/>if depth && droppableCount==0 → refusedDepth++ (onRefuse)<br/>else shedOldestDroppable() → droppedOldestByKind<br/>else accepted++
    Sched->>Pump: every 200ms (maxLatencyMs)
    Pump->>Pump: while inflight < maxInFlight(2)
    Pump->>Retry: if retries.length>0 → launchRetry() first<br/>entry.attempts++ → launch(entry.docs, entry.seq, attempts)
    Pump->>Buf: else events = drainUpTo(maxDocs=1000)
    alt events.length==0
        Pump-->>Pump: break (nothing to launch)
    else events.length>0
        Buf-->>Pump: Tick[≤1000]
        Pump->>Pump: launch(docs=map(toDoc), seq=++seqCounter, attempts=1)<br/>inflight++ ; flushesStarted++ ; startedAt=now()
        Pump->>Sink: insertMany(TickDoc[], {ordered:false, w:1})
        Sink->>Mongo: insertMany on quantflow.ticks<br/>(timeField ts, metaField symbol)<br/>bucket closes at 1K measurements / 125KB
        alt ack
            Mongo-->>Sink: ok
            Sink-->>Pump: resolved → docsWritten+=docs.length<br/>flushesSucceeded++ ; recordLatency(now()-startedAt)
        else reject
            Mongo-->>Sink: error
            Sink-->>Pump: rejected → errors++ ; flushesFailed++<br/>lastErrorMessage = messageOf(err)<br/>insertRetry({seq,docs,attempts}) — seq-ordered splice
            Pump-->>Retry: batchesRetried++
        end
        Pump->>Pump: finally: inflight-- ; notifySettled()<br/>if !closing && !closed → pump() chain (keeps 2 in flight)
    end
    Sched->>Metrics: stats() on 30s timer (src/app.ts:299-305)<br/>p50/p99 over last 4096 latencies (nearest-rank)<br/>{flushesSucceeded/Started, docsWritten, bufferDepth, inFlight, refusedDepth, droppedOldest}
    Note over Pump,Retry: close() (src/store/flusher.ts:147-177)<br/>stop() → while inflight>0 || buffer.size()>0 || retries.length>0:<br/>if inflight==0:<br/>  retryable = retries.find(attempts<3) → relaunch (+=1)<br/>  else if buffer.size()>0 && retries empty → drainUpTo → launch<br/>await settleWaiters (notifies on each finally)<br/>for each remaining r: unwrittenDocs+=r.docs.length<br/>if unwritten>0 → throw "close(): N docs unwritten after 3 attempts"<br/>measured: 24,001 docs/s ×600s = 14.4M docs, 0 failed [2026-08-26-mongo-writer.md]

```

**Why 1K/125KB matters.** Mongo time-series bucket-close is `min(1K measurements, 125KB)` per `(symbol)` series — so `maxDocs=1000` aligns one `insertMany` batch to one bucket write. `FlushService` does **not** need to know bucket boundaries; the Mongo engine handles it, and `FlushService` just keeps `2×1K` in the air (`ordered:false, w:1` lets the engine interleave batches across symbols). `w:1` is the measured 24K/s point; `w:0` or `maxInFlight>2` are the documented levers toward 1M (HLD §6). Pool guard at `src/store/sink.ts:15-16`: `maxPoolSize=100, waitQueueTimeoutMS=10s` — with `maxInFlight=2` no single writer can exhaust the pool.

---

## See also

* [`01-hld.md`](./01-hld.md) — context, container, NFR table, capacity arithmetic, failure matrix, 1M sharding.
* [`02-lld.md`](./02-lld.md) — class sketches (`NormalizedEvent`, `BookStore`, `TickBuffer`/`FlushService`, `SymbolDiffBus`, `FanoutHub`, gateway), measured ceilings per seam.
* [`03-data-model.md`](./03-data-model.md) — key → query → collection map, bucket DDL, fencing/idempotency DDL.
* [`README.md`](./README.md) — file index.
* ADRs: [0001 fixtures local](../adr/0001-recorded-fixtures-stay-local.md) · [0002 book not engine](../adr/0002-v1-is-book-state-not-a-matching-engine.md) · [0003 derived-only](../adr/0003-derived-signal-boundary.md) · [0004 ledger serializability + fencing](../adr/0004-ledger-strict-serializability.md) · [0005 bounded contexts](../adr/0005-scenariox-bounded-context.md).
* Canonical terms & claim tags: [`CONTEXT.md`](../../CONTEXT.md) · [`CONTEXT-MAP.md`](../../CONTEXT-MAP.md).
* Evidence annex: [`docs/benchmarks/README.md`](../benchmarks/README.md) + `2026-08-26-*.md` results.

