# Context Map

## Contexts

- [Market Data & Ledger](./CONTEXT.md): real-time market-data pipeline, order-book maintenance, and per-tenant serializable ledger (normalized ticks, ZSET book, time-series flush, ledger accounts/transactions)
- [ScenarioX](./src/scenariox/CONTEXT.md): multi-asset portfolio risk simulation (portfolios, positions, VaR/Sharpe, shock scenarios, CRDT workspace, macro embeddings)

## Relationships

- **Market Data & Ledger → ScenarioX**: Market Data provides `Normalized Tick` and `Order Book` snapshots; Ledger provides `Ledger Account` balances; ScenarioX consumes both as inputs to `Position` PnL and `Scenario` shocks
- **ScenarioX → Billing**: ScenarioX emits `ScenarioResult` events; Billing consumes risk-tier entitlements to gate shock count per month
