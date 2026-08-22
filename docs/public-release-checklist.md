# Public release checklist

Run before making this repository public. Every item must be checked off; an unchecked item means the release is not done.

## Before pushing

- [ ] Choose and add a `LICENSE` file (e.g., MIT or Apache-2.0) at the repo root.
- [ ] Sweep for personal/private data — working tree and history. Specifically confirm `.scratch/` (the private local issue tracker) is covered by `.gitignore`; it must never appear in the published repo or its history. Note: as of 2026-08-26 no root `.gitignore` exists yet — create one covering `.scratch/` and `.local-fixtures/`.
- [ ] Run a secrets scan over the full history (e.g., `gitleaks detect`) and confirm zero findings; verify no `.env` or credential file is tracked.

## Publish

- [ ] Create the GitHub repository and push:

  ```bash
  gh auth login
  gh repo create QuantFlow.io --public --source=. --push
  ```

- [ ] Confirm CI is green on the remote (applies once the push above has triggered the first run; re-check immediately before announcing).
- [ ] Set the repository description and topics, e.g. description: "Real-time crypto market-data pipeline: WS ingestion → Redis L2 order book → MongoDB time-series → WS fan-out"; topics such as `nodejs`, `typescript`, `websocket`, `redis`, `mongodb`, `market-data`, `orderbook`.
- [ ] Enable basic branch protection on the default branch: block force pushes and deletion, require passing status checks.
