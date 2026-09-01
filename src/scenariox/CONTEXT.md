# Context: ScenarioX

Multi-asset portfolio risk simulation where tenants collaboratively model shocks.

## Glossary

- **Portfolio**: Per-tenant collection of `Position`s owned by one tenant, identified by `(tenantId, portfolioId)`.
  _Avoid_: account, basket
- **Position**: Per-symbol holding `{symbol, quantity, avgEntryPrice, side}` inside a `Portfolio`, whose PnL derives from market ticks, distinct from `Ledger Account` cash.
  _Avoid_: holding (unqualified), ledger position
- **VaR (Value at Risk)**: Loss threshold at 95/99% confidence over 1d horizon, computed via historical shock replay.
  _Avoid_: risk (unqualified), var (unqualified)
- **Sharpe**: Annualized excess return / volatility, derived from same return series as VaR.
  _Avoid_: sharpe ratio (unqualified)
- **Scenario**: Named set of `Macro Shock`s `{id, name, shocks: MacroShock[]}` applied to a `Portfolio`.
  _Avoid_: shock (unqualified), simulation
- **Macro Shock**: Natural-language shock description (e.g., "Fed hikes 50bps") mapped via `Embedding` to nearest historical `Scenario`.
  _Avoid_: macro (unqualified)
- **Embedding**: Dense vector from `all-MiniLM-L6-v2` local model (80 MB) representing a `Macro Shock` for cosine lookup.
  _Avoid_: vector (unqualified)
- **CRDT Workspace**: Automerge-backed JSON document for multi-user `Portfolio` editing, merging concurrent Position edits without locks.
  _Avoid_: collaborative doc, Yjs doc
- **Worker Thread Risk Engine**: Own thin `worker_threads` wrapper for VaR/Sharpe; `piscina` is the documented fallback when pool lifecycle exceeds thin wrapper.
  _Avoid_: worker pool (unqualified)
