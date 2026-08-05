# Data sources & fixture policy

Policy summary ([ADR-0001](docs/adr/0001-recorded-fixtures-stay-local.md)): this repo carries **only venue-neutral normalized events or synthetic generators**. Raw exchange captures are recorded exclusively into the gitignored `.local-fixtures/` directory and are never committed. Rationale: reversing this after raw bytes enter git history is practically impossible; committing venue-neutral data instead means CI never depends on live sockets and any clone reproduces tests without legal exposure.

## Venues

| Venue          | Role                      | Notes                                                                                                                                                                                                                                                                                        |
| -------------- | ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Binance Spot   | Primary feed              | WebSocket via `data-stream.binance.vision`: `aggTrade`, `depth@100ms`, `bookTicker`. Bulk historical archives are openly published at `data.binance.vision`, but without a clean explicit redistribution license — treat archive-derived material as source input, not as committable bytes. |
| Kraken Spot v2 | Documented fallback       | WebSocket v2 `book` channel.                                                                                                                                                                                                                                                                 |
| Coinbase       | Redistribution prohibited | Market Data Terms of Use §3.2 prohibit redistributing the data or derived works. Never record, commit, or derive fixtures from it.                                                                                                                                                           |
| OKX            | Redistribution prohibited | API Agreement §9.4 extends the redistribution ban even to unauthenticated public-endpoint data. Same handling as Coinbase.                                                                                                                                                                   |

## Fixture rules

1. Committed test input is venue-neutral normalized events or synthetic generators only.
2. Raw venue captures stay local-only in `.local-fixtures/` (gitignored).
3. Any small fixture derived from Binance Vision archives must carry a provenance note: source URL, retrieval date, and the derivation applied.
4. Coinbase and OKX data is never committed, in any form.
