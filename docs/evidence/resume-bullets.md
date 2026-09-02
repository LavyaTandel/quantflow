# Resume bullets — QuantFlow.io flagship (promoted from local dossier)

> Promoted sanitized snapshot of `.scratch/quantflow-flagship/dossier/resume-bullets.md:1` — private tracking table stripped, `[measured]` tags preserved. Full local history stays in `.scratch/` (gitignored per `.gitignore:7`).

## Section A — Flagship bullets

- Built a Binance Spot WebSocket ingestion cluster sustaining 5,999 normalised ticks/s through a 30-second burst of 29,988 msg/s with zero dropped events and 1.765 ms end-to-end p99 against a 50 ms budget [measured].
- Fanned real-time ticks out to 2,000 concurrent WebSocket subscribers at p99 3.000 ms, with a capability probe at 5,000 subscribers reaching p99 5.000 ms — zero connect failures, zero sequence gaps [measured].
- Sustained 24,001 docs/s into MongoDB time-series collections via ~1,000-document unordered batched inserts across 14.4M documents (600 s soak) with zero silent drops — flush p50 45 ms / p99 57 ms [measured].
- Root-caused a missed order-book throughput target to a mis-calibrated figure colliding with the host's loopback RTT floor, then renegotiated the performance-contract wording with data — new pipelined-ceiling evidence of 16,142 ops/s [measured].
- Validated the full pipeline live against production Binance streams: 19,314 frames in 60 seconds with zero parse errors, reconnects, or pipeline drops [measured].

## Section B — Project header (resume "Projects" entry)

**QuantFlow.io** — Real-time crypto market-data pipeline: exchange WebSocket ingestion feeding a Redis sorted-set L2 order book and MongoDB time-series storage, with every performance claim benchmarked by committed, reproducible scripts.
Stack: TypeScript, Node 26, Redis ZSETs, MongoDB time-series, Docker, ws.

## Section C — Compression guidance

<!-- Space-tight variant: if the resume allows only two bullets for this project, use the pair below and keep both [measured] tags.
- Built QuantFlow.io, a real-time market-data pipeline sustaining 5,999 ticks/s through a 30K msg/s burst and fanning out to 2,000 WebSocket subscribers at 3 ms p99 — zero drops end to end [measured].
- Persisted 24,001 docs/s to MongoDB time-series storage (600 s soak, zero silent drops) then validated the whole system live on Binance streams (19,314 frames/60 s, zero errors) [measured].
-->

See `interview-talking-points.md:1` for the 11 drill Q&As that make these bullets defensible.
