# v2 AI alpha alerts — where we win, where we cede, and what we bill

_2026-08-27 · Positioning addendum for the billed alert pipeline (F1/F2/F4). All alert claims are `measured` (hermetic, 272/272) or `cited`; no simulated alpha. Hardware, percentile definitions, and reproduce commands live in `docs/benchmarks/` and `src/ai/`._

The flagship sold raw market-data plumbing. v2 sells the _opinion_ on pressure — a throttled, lossy, venue-entitled Derived Signal — not the quotation. That distinction is the product and the legal position.

## 1. The signal is not the data

External tenants never receive `NormalizedEvent` or raw depth arrays (`cited: docs/adr/0003-derived-signal-boundary.md:1`). The internal pipeline holds a short-lived `NormalizedEvent` window in Redis (`ZSET` hot book), computes a `FeatureVector {I, ofi, dwp, sweepFlag}` in `<1 ms` (`measured: src/ai/features.ts:1`), gates on `|I|>0.3 || |OFI|>p75 || sweepFlag`, classifies via a dequeued `llama.cpp` Worker (`FakeModel` 5–15 ms in tests, Llama 3.2 1B Q4_K_M 60–80 ms E2E `cited: findings/02`), and fans out only:

```json
{
  "symbol": "BTCUSDT",
  "ts": "2026-08-27T14:03:00.120Z",
  "signal": "liquidity_sweep_risk",
  "score": 0.73,
  "confidence": 0.68,
  "horizon_s": 30,
  "window_s": 60,
  "features_version": "obi-v1"
}
```

Throttled `≤1/symbol/1–5s`, no price/qty/level, not a benchmark, venue-entitled. This is the ToS-safe test that keeps Coinbase §3 (bans even Derived Works externally) and OKX §9.4 (bans even public-endpoint training, applies equally to derived scores) from attaching (`cited: findings/01-alpha-market-tos.md:30`).

## 2. Where we win vs Polygon.io-class

Polygon/Massive (`$29` Starter delayed → `$199` Advanced real-time → `$2,499` Business `cited`), Twelve Enterprise `$1,099` `cited`, Finnhub `$49–$3,500` `cited` sell _breadth_ (SIP, 20+ years, corporate actions). Buildix `$79` Whale / Baselion `$299` enterprise `cited` prove $79–$300 willingness for pure OBI/VPIN _signals_.

We win on **per-symbol pressure insight** that is ToS-safe because it never exposes raw levels: Starter F1 queue imbalance `I=(Qb-Qa)/(Qb+Qa)` as a gauge, Professional F1+F2 OFI `Σ OFI_n` over 100 ms–1 s (`R²≈65%` contemporaneous, `β=c/D` `cited: CKS`) as execution-lean, Guardrail F4 sweep (top-book evaporation + spread widen + no-refill <500 ms) as protection — all `<1 ms` before the model, gated to 5–15% of windows, throttled per-tenant (`Map<tenantId:symbol>`).

## 3. Where we deliberately cede

- Full-market SIP/history/corporate actions — we are 6-symbol USDT spot (Binance+Kraken) only.
- Tick-store / raw depth replay — pipeline stays internal; we never fan out `NormalizedEvent`.
- Forward-alpha prediction — framed as execution lean (contemporaneous `R²≈65%`, forward precision 55–60% on 500 ms, measured live and persisted `feature vector + OFI bucket + next-500 ms ΔP` to Mongo for audit), not promised return.
- HFT colocation — we are laptop-class `250 ms P99` (`WS → Redis → 100 ms micro-batch → 60–80 ms 1-token classifier → fan-out`).

Ceding these is the honesty that makes the wins believable.

## 4. What we bill and what we don't

Raw tick/quote WS is **internal only**, no Stripe tier — protects legal position. **Starter $49–$99** delayed F1 only; **Pro $199–$299** real-time per-symbol alerts (F1+F2, 1/s, team seats); **Enterprise custom** high limits/SLA/venue-entitled (OKX excluded until written license). Anchor: personal floor `$49–$200`, business ceiling `$1,099–$2,499` — we sit as a _different product_ between them.

## 5. How to reproduce

`npm test` — `272/272` hermetic (`test/ai/features.test.ts`, `model.test.ts`, `pipeline.test.ts`); throttling per-tenant, tier feature checks `isFeatureAllowed`, zero silent drops (`ticks == emitted + gatedDropped + throttledDropped + queueShed`), second-tenant isolation; e2e `p99 ≤80 ms` inside 250 ms budget. No `llama.cpp` in CI — real `llama-server --n-gpu-layers 0 Q4_K_M max_tokens=1` is a documented swap behind the same `Model` interface.

## 6. What's next

No publishing without owner action. Next is measured `precision 55–60%` on live forward horizon, persisted for audit — the Dossier addendum (`.scratch/quantflow-v2-ai/dossier-addendum.md`) carries the hire-grade version of this positioning.
