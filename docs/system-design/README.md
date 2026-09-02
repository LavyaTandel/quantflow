# System Design — QuantFlow.io

Single source for HLD + LLD. Every diagram is Mermaid (GitHub-native, no build) with an SVG fallback in `assets/` for PDF.

## Index

- `01-hld.md` — C4 L1/L2, ingest→book→store→fan-out, NFRs (5K ticks/s, 5K fan-out p99 5ms), capacity model + failure modes, scaling note to 1M, ADRs 0001–0005
- `02-lld.md` — Per-module LLD: ingest pipeline, book continuity/ZSET (2×RTT×6 chains → 1K ops/s bound), store buffer+flush (1K docs/125KB), fan-out WS `hello|trade|bbo|depth`
- `03-data-model.md` — Redis `book:{symbol}:{side}` ZSET, Mongo time-series bucket, idempotency/fencing (ledger preview)
- `04-sequence-diagrams.md` — 3–4 Mermaid sequences (live ingest, bootstrap U/u vs CRC32+internal seq, depth apply, flush)
- `assets/` — `demo-tape.gif` (30–45s screen capture from `docs/demo-script.md` Beat 4, `REPLAY_FILE` paced 600ms) + `architecture.svg` (exported Mermaid)

Evidence annex stays in `docs/benchmarks/` (locked suite + measured results); cross-link from `01-hld.md` Table 4.
