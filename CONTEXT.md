# Context

QuantFlow.io is a real-time crypto market-data pipeline whose secondary output is career Evidence: every subsystem that ships must produce claims an interviewer can drill.

## Glossary

- **Normalized Tick**: a venue-agnostic market event (aggregate trade, best-quote change, or depth diff) produced by ingestion; the unit all throughput claims are measured in.
- **Symbol Universe**: the six liquid USDT pairs v1 subscribes to (BTC, ETH, SOL, XRP, DOGE, ADA).
- **Order Book**: maintained level-2 price-level state for one symbol — queried and persisted; it never matches or accepts orders in v1.
- **Snapshot Bootstrap**: rebuilding book state from a REST snapshot, then applying buffered diffs.
- **Update Continuity**: detection of gaps in venue diff sequences; a gap forces resynchronization rather than silent application.
- **Fan-out**: pushing ticks to concurrent dashboard subscribers over WebSocket.
- **Flush**: one batched write of buffered ticks into MongoDB time-series storage.
- **Backpressure**: bounded buffers with explicit limits so overload degrades predictably; silent drops do not exist.
- **Fixture**: recorded or synthesized input events used by tests. Venue-neutral inside the repo; raw venue captures stay local-only (ADR-0001).
- **Evidence Dossier**: resume bullets, interview talking points, and the engineering blog post built from verified claims.
- **Claim Tag**: `cited` / `measured` / `to-be-measured` — mandatory label on every Dossier claim.
- **Tranche 1 / Tranche 2**: evidence shipped before benchmarks exist vs evidence upgraded with measured numbers.

### Signals

- **Alpha Signal**: A derived, non-reconstructible score/label produced over a rolling window of order-book events.
  _Avoid_: alpha, prediction, alert (unqualified)
- **Signal Score**: The numeric score ∈ [0,1] inside an Alpha Signal.
  _Avoid_: score (unqualified)
- **Derived Signal**: An Alpha Signal that is lossy, throttled (≤1 per symbol per 1–5 s), windowed, and venue-entitled, safe to bill to a third party.
  _Avoid_: derived data
- **Sweep Risk**: A guardrail label indicating rapid depletion of top-of-book liquidity with spread widen and no refill.
  _Avoid_: sweep, depletion
- **Order Flow Imbalance (OFI)**: The net bid-minus-ask flow summed over a 100 ms–1 s bucket per Cont-Kukanov-Stoikov.
  _Avoid_: flow
- **Imbalance**: Top-of-book queue imbalance I=(Qb-Qa)/(Qb+Qa), the cheapest pressure gauge.
  _Avoid_: OBI, book imbalance

### Ledger

- **Ledger Account**: Per-tenant, per-asset balance row identified by (tenantId, accountId, asset) with version.
  _Avoid_: account (unqualified), wallet
- **Transaction**: Immutable ledger event (debit, credit, or paired transfer) carrying amount, prevVersion, idempotencyKey, and fencing token.
  _Avoid_: transfer (unqualified), entry
- **Idempotency Key**: Client-supplied UUID unique per tenant for 24 h that dedupes retried transfers.
  _Avoid_: request id
- **Fencing Token**: Monotonic value from INCR on ledger:fence:{tenant}:{account}:{asset} checked before materializing to fence stale lock holders.
  _Avoid_: token (unqualified)
- **Ledger Statement**: Paginated, tenant-isolated, ordered Transaction rows with balanceAfter, prevVersion chain, kafkaOffset and fencingToken; cursor is last version.
  _Avoid_: statement (unqualified), history
- **Audit Export**: Streaming CSV/JSONL file of the same rows for a from/to version window, tenant-isolated, reproducible.
  _Avoid_: export (unqualified), dump
- **Time-Travel Query**: Point-in-time balance at a version (deferred beyond statement).
  _Avoid_: time travel (unqualified)
