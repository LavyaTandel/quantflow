# Demo video script — QuantFlow.io v1

Recording script for a 3–5 minute walkthrough (planned runtime: about 3 min 50 s across seven beats). The owner records this; commands are paste-safe for PowerShell on Windows. Rule one of the shoot: never type on camera — paste the block, then narrate while the output lands.

## Prerequisites checklist (verify before recording)

- [ ] Git identity set (`git config user.name` and `git config user.email` both return values) so any on-screen history looks human
- [ ] `docker compose up -d` has been run at least once beforehand and both containers are up (`docker compose ps`)
- [ ] `npm ci` has completed cleanly
- [ ] A local replay fixture exists at `.local-fixtures/demo-replay.jsonl` (recorded per `DATA_SOURCES.md`; stays gitignored per ADR-0001)
- [ ] Two terminal windows open side by side: Terminal 1 reserved for `npm run dev`, Terminal 2 for everything else
- [ ] Screen recorder armed; notifications silenced; editor already open at README.md; browser closed or on a clean tab
- [ ] Dry run done once end to end

## Beats

### Beat 1 — Cold open over the status table (30 s)

No command. Scroll slowly through the **Status** section of `README.md`, pausing on the four-row results table.

Narration cue: "This repo is a real-time crypto market-data pipeline whose second output is evidence: every subsystem ships with numbers an interviewer can drill. Three rows passed benchmark targets; the fourth missed, was root-caused, and stayed published."

### Beat 2 — Architecture one-liner over CONTEXT.md (20 s)

Open `CONTEXT.md` in the editor and scroll the Glossary top to bottom at reading speed.

Narration cue: "The whole architecture is one line: ingest, book, store, fan-out. WebSocket ticks are normalised into venue-neutral events, maintain level-2 state in Redis, flush to MongoDB time-series, and fan out to dashboard subscribers."

### Beat 3 — Services up (15 s)

Paste in Terminal 2:

```powershell
docker compose up -d
npx tsx scripts/init-mongo.ts
```

Narration cue: "Redis and MongoDB run containerized; the init script idempotently creates the time-series collection."

### Beat 4 — Replay-mode runtime plus live viewer (60 s)

Paste in Terminal 1:

```powershell
$env:REPLAY_FILE = ".local-fixtures/demo-replay.jsonl"
npm run dev
```

Wait for the startup log lines (`stages=ingest,book,store,fanout`, replay mode notice), then open <http://localhost:8080> in the browser and let the viewer run: live tape scrolling, top-of-book updating across trades, quotes, and depth diffs. Near the end of the beat press Ctrl+C in Terminal 1 and hold on the shutdown summary line showing drop counters at zero.

Narration cue: "This is the production pipeline in demo mode: a recorded fixture replays through the same normaliser, order-book continuity logic, writer, and fan-out that a live socket feeds — paced about 600 milliseconds between events so you can watch it. The browser is just a WebSocket subscriber. On shutdown every drop counter reads zero — no silent drops is a tested invariant, not a slogan."

Stop the dev server fully before continuing (free port 8080).

### Beat 5 — Benchmark evidence flash (45 s)

Paste in Terminal 2 (or open the files in your editor if the VS Code `code` command is unavailable):

```powershell
code docs/benchmarks/2026-08-26-ingest.md docs/benchmarks/2026-08-26-fanout.md docs/benchmarks/2026-08-26-mongo-writer.md docs/benchmarks/2026-08-26-zset.md docs/benchmarks/2026-08-26-zset-recalibration.md
```

Flash each headline table quickly for the three PASS rows, then slow down deliberately on the ZSET MISS file and its recalibration follow-up.

Narration cue: "Targets were locked before results existed, and methodology is committed next to every number. Ingest, fan-out, and the Mongo writer hit their targets. The Redis row missed — and that miss stays published untouched, because it anchored the analysis: ordered application costs two sequential round trips per diff across six symbol chains. Follow-up legs measured the pipelined ceiling, and the contract wording went back for renegotiation with data."

### Beat 6 — Live smoke against Binance (40 s)

Paste in Terminal 2:

```powershell
$env:LIVE_SMOKE = "1"
npx tsx scripts/live-smoke.ts
```

The run lasts 60 seconds and prints counters every 5 seconds. Let three or four prints land, then Ctrl+C — the beat only needs the counters visible. Requires outbound access to `data-stream.binance.vision`.

Narration cue: "Hermetic-first doesn't mean fake-forever. Flag-gated, never in CI: sixty seconds against Binance's production stream. Frames climb, parse errors zero, unknown-frame drops zero, reconnects zero — the reconnect budget and watchdog behave outside fixtures."

### Beat 7 — Close: where the receipts live (20 s)

In the editor, show `docs/adr/` (0001 fixtures policy, 0002 scope), then the blog post at `docs/blog/`, then the test-count line in README.md.

Narration cue: "Every decision is an ADR, every claim traces to a committed benchmark record, and the fully hermetic test suite — count cited right here in the README — keeps it honest. Repo and write-up linked below."

## Failure recovery (if something errors on camera)

Keep rolling, pause narration, and use these moves — cuts hide the retry:

| Symptom                                    | Say                                                                             | Do                                                                      |
| ------------------------------------------ | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Any paste fails or errors                  | "Let me rerun that clean."                                                      | Rerun the same block after the cut                                      |
| Beat 3: Mongo/Redis connection refused     | "Services weren't warm — bringing them up now."                                 | `docker compose up -d`, wait 5 s, rerun `npx tsx scripts/init-mongo.ts` |
| Beat 4: port 8080 already in use           | "A stray instance held the port."                                               | Ctrl+C stray terminals, rerun the Beat 4 block                          |
| Beat 4: replay file missing                | "Replay fixtures stay local by policy — I'll show the live path instead."       | Run plain `npm run dev` (live ingestion); adjust the cue to "live feed" |
| Beat 6: network error / stream unreachable | "The smoke needs outbound access to Binance; the committed record covers this." | Skip to Beat 7; cite the published 19,314-frame smoke result            |
| Anything else unfixable in 30 s            | "I'll pick this up off-camera."                                                 | Jump to the next beat; fix between takes                                |
