// Fan-out load generator (Row 3). A fully decoupled OS process with its own
// event loop: opens TARGET_SUBS concurrent WebSocket connections, then measures
// end-to-end delivery (server send timestamp → client receive timestamp, same
// host clock) on a monitored subset of connections. Results go back to the
// server over the control channel and to stdout as one JSON line.
//
//   npx tsx scripts/bench/fanout-loadgen.ts
//   (env: PORT, TARGET_SUBS, DURATION_MS, MONITORED, REPORT_SAMPLES)
import { WebSocket } from 'ws';
import { LatencyRecorder, sleep } from './lib.js';

const PORT = Number(process.env.PORT ?? 8090);
const TARGET_SUBS = Number(process.env.TARGET_SUBS ?? 2000);
const DURATION_MS = Number(process.env.DURATION_MS ?? 30_000);
const MONITORED = Number(process.env.MONITORED ?? 64);
const CONNECT_BATCH = Number(process.env.CONNECT_BATCH ?? 100);
const CONNECT_BATCH_DELAY_MS = Number(process.env.CONNECT_BATCH_DELAY_MS ?? 20);
const REPORT_SAMPLES = Number(process.env.REPORT_SAMPLES ?? 20_000);
const WARMUP_MS = Number(process.env.WARMUP_MS ?? 1000);

interface TickMsg {
  seq: number;
  st: number;
}

function line(payload: unknown): void {
  console.log(`LOADGEN_RESULTS ${JSON.stringify(payload)}`);
}

function connectOnce(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`ws://127.0.0.1:${PORT}`);
    socket.on('open', () => resolve(socket));
    socket.on('error', reject);
    setTimeout(() => reject(new Error('connect timeout')), 15_000);
  });
}

async function openSubscribers(): Promise<{ sockets: WebSocket[]; failures: number }> {
  const sockets: WebSocket[] = [];
  let failures = 0;
  for (let opened = 0; opened < TARGET_SUBS; opened += CONNECT_BATCH) {
    const batch = Math.min(CONNECT_BATCH, TARGET_SUBS - opened);
    const results = await Promise.allSettled(Array.from({ length: batch }, () => connectOnce()));
    for (const result of results) {
      if (result.status === 'fulfilled') sockets.push(result.value);
      else failures += 1;
    }
    await sleep(CONNECT_BATCH_DELAY_MS);
  }
  return { sockets, failures };
}

async function main(): Promise<void> {
  console.error(`[loadgen] opening ${TARGET_SUBS} connections...`);
  const { sockets, failures } = await openSubscribers();
  if (sockets.length === 0) {
    line({ error: 'no subscriber connections could be opened', target: TARGET_SUBS });
    process.exit(1);
  }

  // Monitored subset: the first MONITORED sockets record latency + contiguity.
  const monitoredSet = new Set(sockets.slice(0, Math.min(MONITORED, sockets.length)));
  const recorder = new LatencyRecorder();
  let receivedAny = 0;
  let seqGaps = 0;
  let measureFrom = Number.POSITIVE_INFINITY;
  const nextSeq = new Map<WebSocket, number>();

  for (const socket of monitoredSet) {
    socket.on('message', (data) => {
      try {
        const msg = JSON.parse(String(data)) as TickMsg;
        receivedAny += 1;
        if (Date.now() < measureFrom) return; // warmup guard
        const expected = nextSeq.get(socket);
        if (expected !== undefined && msg.seq !== expected) seqGaps += 1;
        nextSeq.set(socket, msg.seq + 1);
        recorder.record(Date.now() - msg.st);
      } catch {
        /* ignore */
      }
    });
  }
  // Non-monitored sockets still count deliveries so throughput is honest.
  for (const socket of sockets) {
    if (monitoredSet.has(socket)) continue;
    socket.on('message', () => {
      receivedAny += 1;
    });
  }

  const control = await connectOnce();
  control.send(JSON.stringify({ role: 'loadgen' }));

  const begunAt: number = await new Promise((resolve) => {
    control.on('message', (data) => {
      try {
        const parsed = JSON.parse(String(data)) as { type?: string; at?: number };
        if (parsed.type === 'begun') resolve(parsed.at ?? Date.now());
      } catch {
        /* ignore */
      }
    });
    control.send(JSON.stringify({ type: 'begin', durationMs: DURATION_MS }));
    setTimeout(() => resolve(Date.now()), 5000);
  });

  console.error('[loadgen] measurement window running...');
  measureFrom = begunAt + WARMUP_MS;
  while (Date.now() < begunAt + DURATION_MS) await sleep(50);

  const summary = recorder.summarize();
  const allSamples = recorder.exportSamples();
  const stride = Math.max(1, Math.ceil(allSamples.length / REPORT_SAMPLES));
  const subsample = allSamples.filter((_, i) => i % stride === 0);

  const payload = {
    targetSubs: TARGET_SUBS,
    subsOpened: sockets.length,
    connectFailures: failures,
    durationMs: DURATION_MS,
    warmupMs: WARMUP_MS,
    monitoredConnections: monitoredSet.size,
    messagesReceivedTotal: receivedAny,
    seqGapsOnMonitored: seqGaps,
    latency: {
      samples: summary.samples,
      p50: summary.p50,
      p99: summary.p99,
      max: summary.max,
    },
    subsample,
  };

  control.send(JSON.stringify({ type: 'client-results', payload }));
  line(payload);
  control.close();
  for (const socket of sockets) socket.close();
  process.exit(0);
}

main().catch((err: unknown) => {
  console.error(err);
  line({ error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
