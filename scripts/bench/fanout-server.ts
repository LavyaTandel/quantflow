// Fan-out bench server (Row 3). Started by scripts/bench/fanout.ts as a
// separate OS process. Broadcasts compact JSON ticks through EventPipeline to
// every connected subscriber WebSocket. The load generator connects one extra
// control socket (role: 'loadgen') that triggers the measurement window and
// receives nothing but control frames.
//
//   npx tsx scripts/bench/fanout-server.ts   (env: PORT, TICKS_PER_SEC)
import { WebSocketServer, WebSocket } from 'ws';
import { EventPipeline } from '../../src/ingest/pipeline.js';
import { PacedEmitter, yieldToLoop } from './lib.js';

const PORT = Number(process.env.PORT ?? 8090);
const TICKS_PER_SEC = Number(process.env.TICKS_PER_SEC ?? 100);

interface TickMsg {
  seq: number;
  st: number;
  symbol: string;
  price: number;
}

function line(tag: string, payload: unknown): void {
  console.log(`${tag} ${JSON.stringify(payload)}`);
}

const wss = new WebSocketServer({ host: '127.0.0.1', port: PORT });
const subscribers = new Set<WebSocket>();
let controlSocket: WebSocket | undefined;

wss.on('listening', () => {
  console.log(`READY port=${PORT}`);
});

wss.on('connection', (socket) => {
  let isControl = false;
  socket.on('message', (data) => {
    try {
      const parsed = JSON.parse(String(data)) as {
        role?: string;
        type?: string;
        durationMs?: number;
      };
      if (parsed.role === 'loadgen') {
        isControl = true;
        controlSocket = socket;
        subscribers.delete(socket);
        return;
      }
      if (isControl && parsed.type === 'begin') {
        const durationMs = parsed.durationMs ?? 30_000;
        if (controlSocket?.readyState === WebSocket.OPEN) {
          controlSocket.send(JSON.stringify({ type: 'begun', at: Date.now() }));
        }
        void runBroadcast(durationMs);
      }
    } catch {
      // ignore malformed control frames
    }
  });
  socket.on('close', () => {
    if (isControl && controlSocket === socket) controlSocket = undefined;
    else subscribers.delete(socket);
  });
  socket.on('error', () => {
    /* counted via close */
  });
  if (!isControl) subscribers.add(socket);
});

async function runBroadcast(durationMs: number): Promise<void> {
  const pipeline = new EventPipeline({ capacity: 10_000 });
  const symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
  const startedAt = performance.now();
  const deadline = startedAt + durationMs;
  let seq = 0;

  // Per-wave accounting: overlapping waves must not pollute each other's
  // completion-time samples.
  let waveSeq = 0;
  const pendingByWave = new Map<number, number>();
  const waveStarts = new Map<number, number>();
  const waveSamples: number[] = [];

  const sendToAll = (msg: string): number => {
    const wave = waveSeq++;
    waveStarts.set(wave, performance.now());
    pendingByWave.set(wave, subscribers.size);
    let queued = 0;
    for (const socket of subscribers) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      queued += 1;
      socket.send(msg, () => {
        const remaining = (pendingByWave.get(wave) ?? 0) - 1;
        if (remaining <= 0 && waveStarts.has(wave)) {
          waveSamples.push(performance.now() - (waveStarts.get(wave) ?? 0));
          pendingByWave.delete(wave);
          waveStarts.delete(wave);
        } else {
          pendingByWave.set(wave, remaining);
        }
      });
    }
    return queued;
  };

  const emitter = new PacedEmitter(
    TICKS_PER_SEC,
    () => {
      pipeline.push({
        kind: 'trade',
        symbol: symbols[seq % symbols.length] as string,
        price: 64000 + (seq % 100),
        qty: 0.01,
        ts: Date.now(),
        tradeId: `b-${seq}`,
      });
      seq += 1;
    },
    5,
  );

  let drainedToSubscribers = 0;
  let sendErrors = 0;
  emitter.start();

  let draining = true;
  const drainer = (async () => {
    while (draining || pipeline.counters().currentDepth > 0) {
      let taken = 0;
      for (;;) {
        const event = pipeline.take();
        if (!event) break;
        if (event.kind !== 'trade') continue;
        const msg: TickMsg = {
          seq: drainedToSubscribers,
          st: Date.now(),
          symbol: event.symbol,
          price: event.price,
        };
        const sent = sendToAll(JSON.stringify(msg));
        if (sent < subscribers.size) sendErrors += subscribers.size - sent;
        drainedToSubscribers += 1;
        taken += 1;
        if (taken >= 32) break;
      }
      await yieldToLoop();
    }
  })();

  const waitStop = async (): Promise<void> => {
    while (performance.now() < deadline) await yieldToLoop();
  };
  await waitStop();
  emitter.stop();
  draining = false;
  pipeline.close();
  await drainer;

  const elapsedSecs = (performance.now() - startedAt) / 1000;
  const sorted = [...waveSamples].sort((a, b) => a - b);
  const pct = (p: number): number => {
    if (sorted.length === 0) return 0;
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
    return sorted[idx] as number;
  };
  const payload = {
    subscribersAtEnd: subscribers.size,
    ticksBroadcast: drainedToSubscribers,
    elapsedSecs,
    ticksPerSecAchieved: seq / elapsedSecs,
    sendErrors,
    waveCompletionMs: {
      samples: sorted.length,
      p50: pct(50),
      p99: pct(99),
      max: sorted.at(-1) ?? 0,
    },
  };
  if (controlSocket?.readyState === WebSocket.OPEN) {
    controlSocket.send(JSON.stringify({ type: 'server-results', payload }));
  }
  line('SERVER_RESULTS', payload);
  wss.close();
  process.exit(0);
}
