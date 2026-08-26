// Row 3 — Fan-out orchestrator. Spawns the bench server and the decoupled load
// generator as separate OS processes (own event loops), runs a 2,000-subscriber
// leg and a best-effort 5,000-subscriber probe, and writes the results file.
//
//   npx tsx scripts/bench/fanout.ts
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import {
  PERCENTILE_DEFINITION,
  collectHardwareLine,
  fmtInt,
  fmtMs,
  hardwareTable,
  verdict,
  writeResultsFile,
} from './lib.js';

const PORT = Number(process.env.PORT ?? 8090);
const TICKS_PER_SEC = Number(process.env.TICKS_PER_SEC ?? 100);
const DURATION_MS = Number(process.env.DURATION_MS ?? 30_000);
const RAW_LEGS = (process.env.FANOUT_LEGS ?? '2000,5000')
  .split(',')
  .map((s) => Number(s.trim()))
  .filter((n) => Number.isFinite(n) && n > 0);
const LEGS = RAW_LEGS.length > 0 ? RAW_LEGS : [2000];
const BENCH_DIR = import.meta.dirname;
const TSX_CLI = join(BENCH_DIR, '..', '..', 'node_modules', 'tsx', 'dist', 'cli.mjs');

interface LatencyStats {
  samples: number;
  p50: number;
  p99: number;
  max: number;
}

interface LoadgenPayload {
  targetSubs: number;
  subsOpened: number;
  connectFailures: number;
  durationMs: number;
  warmupMs: number;
  monitoredConnections: number;
  messagesReceivedTotal: number;
  seqGapsOnMonitored: number;
  latency: LatencyStats;
  error?: string;
}

interface ServerPayload {
  subscribersAtEnd: number;
  ticksBroadcast: number;
  elapsedSecs: number;
  ticksPerSecAchieved: number;
  sendErrors: number;
  waveCompletionMs: LatencyStats;
}

function extractJson<T>(output: string, tag: string): T | undefined {
  for (const lineText of output.split(/\r?\n/)) {
    if (lineText.startsWith(tag)) {
      try {
        return JSON.parse(lineText.slice(tag.length).trim()) as T;
      } catch {
        return undefined;
      }
    }
  }
  return undefined;
}

async function waitForReady(
  getOutput: () => string,
  prefix: string,
  timeoutMs: number,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (getOutput().includes(prefix)) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return false;
}

function pipeStdout(child: ReturnType<typeof spawn>, sink: { text: string }): void {
  child.stdout?.on('data', (chunk: Buffer) => {
    sink.text += chunk.toString();
    process.stdout.write(chunk);
  });
  child.stderr?.on('data', (chunk: Buffer) => process.stderr.write(chunk));
}

interface LegOutcome {
  target: number;
  server?: ServerPayload;
  loadgen?: LoadgenPayload;
}

async function runLeg(target: number): Promise<LegOutcome> {
  console.log(`\n[fanout] === leg: ${fmtInt(target)} subscribers ===`);

  const serverSink = { text: '' };
  const server = spawn(process.execPath, [TSX_CLI, join(BENCH_DIR, 'fanout-server.ts')], {
    env: { ...process.env, PORT: String(PORT), TICKS_PER_SEC: String(TICKS_PER_SEC) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  pipeStdout(server, serverSink);

  const ready = await waitForReady(() => serverSink.text, `READY port=${PORT}`, 20_000);
  if (!ready) {
    server.kill();
    console.error('[fanout] server failed to become ready');
    return { target };
  }

  const loadgenSink = { text: '' };
  const loadgen = spawn(process.execPath, [TSX_CLI, join(BENCH_DIR, 'fanout-loadgen.ts')], {
    env: {
      ...process.env,
      PORT: String(PORT),
      TARGET_SUBS: String(target),
      DURATION_MS: String(DURATION_MS),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  pipeStdout(loadgen, loadgenSink);

  let safetyTimer: NodeJS.Timeout | undefined;
  await Promise.race([
    Promise.all([
      new Promise<void>((resolve) => loadgen.on('close', () => resolve())),
      new Promise<void>((resolve) => server.on('close', () => resolve())),
    ]),
    new Promise<void>((resolve) => {
      safetyTimer = setTimeout(resolve, DURATION_MS + 180_000);
    }),
  ]);
  safetyTimer?.unref();

  if (!loadgen.killed) loadgen.kill();
  if (!server.killed) server.kill();
  await new Promise((resolve) => setTimeout(resolve, 250)); // let stdout flush
  return {
    target,
    server: extractJson<ServerPayload>(serverSink.text, 'SERVER_RESULTS'),
    loadgen: extractJson<LoadgenPayload>(loadgenSink.text, 'LOADGEN_RESULTS'),
  };
}

function legSection(o: LegOutcome): string {
  if (!o.loadgen || !o.loadgen.latency || o.loadgen.latency.samples === 0) {
    return `### Leg ${fmtInt(o.target)} subscribers — **FAILED**\n\nThe load generator produced no usable latency samples for this leg.`;
  }
  const lg = o.loadgen;
  const sv = o.server;
  const legPass =
    lg.seqGapsOnMonitored === 0 && lg.subsOpened >= o.target * 0.98 && lg.latency.p99 <= 15;
  return `### Leg ${fmtInt(o.target)} subscribers — ${verdict(legPass)}

| Metric | Value |
| --- | --- |
| Connections opened | ${fmtInt(lg.subsOpened)} / ${fmtInt(lg.targetSubs)} targeted (${lg.connectFailures} connect failures) |
| Broadcast rate | ${
    sv
      ? `${(sv.ticksPerSecAchieved / 1000).toFixed(1)}K ticks/s x ${fmtInt(sv.subscribersAtEnd)} subscriber sockets`
      : 'server payload missing'
  } |
| Messages received (all conns) | ${fmtInt(lg.messagesReceivedTotal)} |
| Seq gaps on monitored conns | ${lg.seqGapsOnMonitored} |
| e2e p50 / p99 / max | ${fmtMs(lg.latency.p50)} / ${fmtMs(lg.latency.p99)} / ${fmtMs(lg.latency.max)} |
| Latency samples | ${fmtInt(lg.latency.samples)} (monitored subset of ${fmtInt(lg.monitoredConnections)} connections) |
| Server wave-completion p50/p99 | ${
    sv
      ? `${fmtMs(sv.waveCompletionMs.p50)} / ${fmtMs(sv.waveCompletionMs.p99)} over ${fmtInt(sv.waveCompletionMs.samples)} waves`
      : 'n/a'
  } |
| Server send errors | ${sv ? sv.sendErrors : 'n/a'} |`;
}

async function main(): Promise<void> {
  console.log('[fanout] collecting hardware line...');
  const hardware = await collectHardwareLine();
  const outcomes: LegOutcome[] = [];
  for (const target of LEGS) {
    outcomes.push(await runLeg(target));
  }

  const requiredTarget: number = LEGS[0] ?? 2000;
  const requiredLeg = outcomes.find((o) => o.target === requiredTarget);
  const lg = requiredLeg?.loadgen;
  const p99Ok = Boolean(lg?.latency && lg.latency.samples > 0 && lg.latency.p99 <= 15);
  const subsOk = Boolean(lg && lg.subsOpened >= requiredTarget * 0.98);
  const gapsOk = Boolean(lg && lg.seqGapsOnMonitored === 0);
  const passed = p99Ok && subsOk && gapsOk;

  const probeTarget = Math.max(...LEGS);
  const probe = outcomes.find((o) => o.target === probeTarget);

  const markdown = `# Benchmark — Row 3: WebSocket fan-out

Date: ${new Date().toISOString().slice(0, 10)} · Script: \`scripts/bench/fanout.ts\` (+ \`fanout-server.ts\`, \`fanout-loadgen.ts\`) · Verdict: **${verdict(passed)}**

## Headline

${fmtInt(requiredTarget)} concurrent WebSocket subscribers at ${TICKS_PER_SEC} ticks/s broadcast: **e2e p99 ${
    lg?.latency ? fmtMs(lg.latency.p99) : 'NOT MEASURED'
  }** vs ≤15 ms target. Highest probed scale: ${fmtInt(probeTarget)} subscribers (${
    probe?.loadgen?.latency
      ? `p99 ${fmtMs(probe.loadgen.latency.p99)}`
      : 'probe failed — see anomalies'
  }).

## Hardware (recorded at run time)

${hardwareTable(hardware)}

## Methodology

- **Decoupled load generation**: the load generator is a separate OS process with its own event loop (\`fanout-loadgen.ts\`), spawned by the orchestrator alongside the server process (\`fanout-server.ts\`); no shared event loop.
- The server feeds synthetic ticks through \`EventPipeline\` and broadcasts compact JSON frames (~60-byte JSON objects carrying seq + send timestamp) to every open subscriber socket at ${TICKS_PER_SEC} ticks/s.
- **Measurement**: per-message client timestamp echo — the server stamps send time via \`Date.now()\`; each client computes e2e = receive-time \`Date.now()\` − send time. Same host clock (sub-ms precision on Windows); this measures server-send → client-receive end-to-end. No TCP ACK is involved despite the "send-to-client-ack" shorthand in the row definition.
- Latency samples come from a monitored subset of 64 connections; every connection counts deliveries for throughput. First WARMUP_MS=1000 of each window is excluded. Sequence numbers on monitored connections detect dropped frames.
- The server separately records broadcast-wave completion (time until every subscriber's \`send\` callback has fired) as server-side context — that is kernel-buffer flush, not application-level acknowledgement.
- The ${fmtInt(probeTarget)}-subscriber leg is a **probe**: capability documentation only, no dossier claim attached.
- ${PERCENTILE_DEFINITION}
- Redis/MongoDB are not involved in this row.

## Results

${outcomes.map(legSection).join('\n\n')}

## Config constants

PORT=${PORT}, TICKS_PER_SEC=${TICKS_PER_SEC}, DURATION_MS=${DURATION_MS}, legs=${LEGS.join(',')}, MONITORED=64, CONNECT_BATCH=100 per 20ms ramp, REPORT_SAMPLES=20000.

## Anomalies

${outcomes
  .map((o) =>
    o.loadgen?.latency
      ? `Leg ${fmtInt(o.target)}: connectFailures=${o.loadgen.connectFailures}, seqGaps=${o.loadgen.seqGapsOnMonitored}${o.server ? `, sendErrors=${o.server.sendErrors}` : ', no server payload'}`
      : `Leg ${fmtInt(o.target)}: no loadgen results.`,
  )
  .join('\n\n')}

## Verdict criteria (required ${fmtInt(requiredTarget)}-subscriber leg)

- ≥98% of targeted connections opened: ${verdict(subsOk)}
- Zero sequence gaps on monitored connections: ${verdict(gapsOk)}
- e2e p99 ≤15 ms: ${verdict(p99Ok)}

**Overall: ${verdict(passed)}**

Reproduce: no Docker needed for this row · \`npx tsx scripts/bench/fanout.ts\`
`;
  const path = await writeResultsFile('fanout', markdown);
  console.log(`[fanout] results written to ${path}`);
  console.log(`[fanout] VERDICT: ${verdict(passed)}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
