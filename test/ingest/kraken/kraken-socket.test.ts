import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type WebSocket from 'ws';
import {
  KRAKEN_BUDGET_WINDOW_MS,
  KRAKEN_LIVENESS_TIMEOUT_MS,
  KRAKEN_MAX_CONNECTS_PER_WINDOW,
  KRAKEN_PING_INTERVAL_MS,
  KRAKEN_WS_URL,
  KrakenMarketSocket,
  type KrakenSocketConfig,
} from '../../../src/ingest/kraken/kraken-socket.js';

const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 3;

class FakeWebSocket extends EventEmitter {
  readyState: number = CONNECTING;
  readonly url: string;
  readonly sent: string[] = [];
  readonly closes: Array<{ code: number; reason: string }> = [];
  terminated = false;

  constructor(url: string) {
    super();
    this.url = url;
  }

  override emit(event: string | symbol, ...args: unknown[]): boolean {
    if (event === 'open') this.readyState = OPEN;
    return super.emit(event, ...args);
  }

  send(data: string, cb?: (err?: Error) => void): void {
    if (this.readyState !== OPEN) {
      const err = new Error('not open');
      if (cb) cb(err);
      else throw err;
      return;
    }
    this.sent.push(data);
    if (cb) cb();
  }

  close(code = 1005, reason = ''): void {
    if (this.readyState === CLOSED) return;
    this.readyState = CLOSED;
    this.closes.push({ code, reason });
    queueMicrotask(() => this.emit('close', code, Buffer.from(reason)));
  }

  terminate(): void {
    if (this.readyState === CLOSED) return;
    this.terminated = true;
    this.readyState = CLOSED;
    queueMicrotask(() => this.emit('close', 1006, Buffer.from('terminated')));
  }

  pong(): void {}
}

interface Harness {
  socket: KrakenMarketSocket;
  sockets: FakeWebSocket[];
}

function makeHarness(config: Partial<KrakenSocketConfig> = {}): Harness {
  const sockets: FakeWebSocket[] = [];
  const socket = new KrakenMarketSocket({
    symbols: ['BTC/USD', 'ETH/USD'],
    channels: ['book', 'trade'],
    depth: 25,
    backoff: {
      baseDelayMs: 100,
      maxDelayMs: 200,
      rng: () => 0,
      maxAttemptsPerWindow: KRAKEN_MAX_CONNECTS_PER_WINDOW,
      windowMs: KRAKEN_BUDGET_WINDOW_MS,
    },
    ...config,
    wsFactory: (url) => {
      const ws = new FakeWebSocket(url);
      sockets.push(ws);
      return ws as unknown as WebSocket;
    },
  });
  return { socket, sockets };
}

describe('KrakenMarketSocket posture constants (Kraken docs, Aug 2026)', () => {
  it('caps connects at ~150 per rolling 10 minutes and pings well under the 60 s idle close', () => {
    expect(KRAKEN_WS_URL).toBe('wss://ws.kraken.com/v2');
    expect(KRAKEN_MAX_CONNECTS_PER_WINDOW).toBe(150);
    expect(KRAKEN_BUDGET_WINDOW_MS).toBe(600_000);
    expect(KRAKEN_PING_INTERVAL_MS).toBeLessThanOrEqual(50_000);
    expect(KRAKEN_LIVENESS_TIMEOUT_MS).toBeGreaterThan(KRAKEN_PING_INTERVAL_MS);
  });
});

describe('KrakenMarketSocket lifecycle (injected fake WebSocket)', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('dials the v2 endpoint and flushes channel subscribes through the limiter after open', async () => {
    const { socket, sockets } = makeHarness();
    socket.on('error', () => {});
    socket.start();

    expect(sockets[0]!.url).toBe('wss://ws.kraken.com/v2');
    expect(sockets[0]!.sent).toHaveLength(0);
    sockets[0]!.emit('open');

    await vi.advanceTimersByTimeAsync(249);
    expect(sockets[0]!.sent).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(1); // t=250ms: first limiter interval
    expect(sockets[0]!.sent).toEqual([
      JSON.stringify({
        method: 'subscribe',
        params: { channel: 'book', symbol: ['BTC/USD', 'ETH/USD'], depth: 25 },
      }),
      JSON.stringify({
        method: 'subscribe',
        params: { channel: 'trade', symbol: ['BTC/USD', 'ETH/USD'] },
      }),
    ]);

    await vi.advanceTimersByTimeAsync(2_000);
    expect(sockets[0]!.sent).toHaveLength(2); // queue drained, no repeats
    socket.stop();
  });

  it('sends application-level pings on the configured cadence and tracks pongs', async () => {
    const { socket, sockets } = makeHarness({ pingIntervalMs: 50_000 });
    socket.on('error', () => {});
    socket.start();
    sockets[0]!.emit('open');

    const pingMessages = () => sockets[0]!.sent.filter((message) => message.includes('"ping"'));

    await vi.advanceTimersByTimeAsync(49_999);
    expect(pingMessages()).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(1);
    expect(JSON.parse(sockets[0]!.sent.at(-1)!)).toEqual({ method: 'ping' });
    expect(socket.getStats().pingsSent).toBe(1);

    sockets[0]!.emit('message', Buffer.from(JSON.stringify({ method: 'pong', req_id: 1 })));
    expect(socket.getStats().lastPongAtMs).not.toBeNull();

    await vi.advanceTimersByTimeAsync(50_000);
    expect(socket.getStats().pingsSent).toBe(2);
    socket.stop();
  });

  it('terminates silent connections but keeps pong-fed connections alive', async () => {
    const silent = makeHarness({ livenessCheckIntervalMs: 15_000 });
    silent.socket.on('error', () => {});
    silent.socket.start();
    silent.sockets[0]!.emit('open');

    // watchdog checks at t=15k..105k see <=105 s silence; the t=120k check trips
    await vi.advanceTimersByTimeAsync(125_000);
    expect(silent.sockets[0]!.terminated).toBe(true);
    silent.socket.stop();
    await vi.advanceTimersByTimeAsync(1);

    const alive = makeHarness({ livenessCheckIntervalMs: 15_000 });
    alive.socket.on('error', () => {});
    alive.socket.start();
    alive.sockets[0]!.emit('open');

    for (let tick = 0; tick < 9; tick += 1) {
      await vi.advanceTimersByTimeAsync(15_000);
      alive.sockets[0]?.emit('message', Buffer.from(JSON.stringify({ method: 'pong' })));
    }
    expect(alive.sockets[0]!.terminated).toBe(false);
    alive.socket.stop();
  });

  it('schedules reconnects through the shared scheduler and budgets them like Cloudflare', async () => {
    const delays: number[] = [];
    const { socket, sockets } = makeHarness();
    socket.on('error', () => {});
    socket.on('reconnectScheduled', ({ delayMs }) => delays.push(delayMs));
    socket.start();

    for (let cycle = 0; cycle < 155; cycle += 1) {
      sockets.at(-1)!.emit('open');
      sockets.at(-1)!.close(1000, 'cycle');
      await Promise.resolve(); // flush close microtask so handleClose registers the delay first
      await vi.advanceTimersByTimeAsync(Math.max(delays.at(-1) ?? 0, 1));
    }

    const preBudget = delays.slice(0, KRAKEN_MAX_CONNECTS_PER_WINDOW);
    expect(Math.max(...preBudget)).toBe(0);
    expect(delays[KRAKEN_MAX_CONNECTS_PER_WINDOW]!).toBeGreaterThanOrEqual(3_500);
    expect(delays.at(-1)).toBeGreaterThanOrEqual(3_500);
    expect(socket.getStats().connects).toBe(156);
    socket.stop();
  });

  it('does not reschedule after an intentional stop', async () => {
    const { socket, sockets } = makeHarness();
    const reconnections = vi.fn();
    let closedIntentional: boolean | undefined;
    socket.on('error', () => {});
    socket.on('reconnectScheduled', reconnections);
    socket.on('closed', ({ intentional }) => {
      closedIntentional = intentional;
    });

    socket.start();
    sockets[0]!.emit('open');
    socket.stop();
    await vi.advanceTimersByTimeAsync(120_000);

    expect(closedIntentional).toBe(true);
    expect(reconnections).not.toHaveBeenCalled();
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.closes).toEqual([{ code: 1000, reason: 'client-stop' }]);
  });

  it('emits raw envelopes and frameError for unparseable payloads', async () => {
    const { socket, sockets } = makeHarness();
    const frames: unknown[] = [];
    const frameErrors: string[] = [];
    socket.on('error', () => {});
    socket.on('frame', (frame) => frames.push(frame));
    socket.on('frameError', (raw) => frameErrors.push(raw));
    socket.start();
    sockets[0]!.emit('open');

    sockets[0]!.emit('message', Buffer.from('{"channel":"book","type":"update"}'));
    sockets[0]!.emit('message', Buffer.from('{broken json'));

    expect(frames).toEqual([{ channel: 'book', type: 'update' }]);
    expect(frameErrors).toHaveLength(1);
    expect(socket.getStats().framesReceived).toBe(2);
    expect(socket.getStats().parseErrors).toBe(1);
    socket.stop();
  });
});
