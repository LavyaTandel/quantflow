import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type WebSocket from 'ws';
import { BinanceMarketSocket, type BinanceSocketConfig } from '../../src/ingest/binance-socket.js';

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
  socket: BinanceMarketSocket;
  sockets: FakeWebSocket[];
}

function makeHarness(config: Partial<BinanceSocketConfig> = {}): Harness {
  const sockets: FakeWebSocket[] = [];
  const socket = new BinanceMarketSocket({
    symbols: ['BTCUSDT'],
    streams: ['aggTrade'],
    backoff: {
      baseDelayMs: 100,
      maxDelayMs: 200,
      rng: () => 0,
      maxAttemptsPerWindow: 1_000_000,
      windowMs: 3_600_000,
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

describe('BinanceMarketSocket lifecycle (injected fake WebSocket)', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('dials the combined-stream URL once per start and reaches ready on open', async () => {
    const { socket, sockets } = makeHarness({ baseUrl: 'wss://example.test' });
    const opened = vi.fn();
    socket.on('open', opened);
    socket.on('error', () => {});

    socket.start();
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.url).toBe('wss://example.test/stream?streams=btcusdt@aggTrade');
    expect(opened).not.toHaveBeenCalled();
    expect(socket.isConnected()).toBe(false);

    sockets[0]!.emit('open');
    expect(opened).toHaveBeenCalledTimes(1);
    expect(socket.isConnected()).toBe(true);
    const stats = socket.getStats();
    expect(stats.connects).toBe(1);
    expect(stats.reconnects).toBe(0);
    expect(stats.openedAtMs).not.toBeNull();

    socket.stop();
  });

  it('recycles proactively after recycleAfterMs and reconnects with reason "recycle"', async () => {
    const { socket, sockets } = makeHarness({ recycleAfterMs: 5_000 });
    const recycled = vi.fn();
    const reconnections: Array<{ attempt: number; delayMs: number; reason: string }> = [];
    socket.on('error', () => {});
    socket.on('recycled', recycled);
    socket.on('reconnectScheduled', (info) => reconnections.push({ ...info }));

    socket.start();
    sockets[0]!.emit('open');
    await vi.advanceTimersByTimeAsync(4_999);
    expect(recycled).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);

    expect(recycled).toHaveBeenCalledTimes(1);
    expect(sockets[0]!.closes).toEqual([{ code: 1000, reason: 'proactive-recycle' }]);
    await vi.advanceTimersByTimeAsync(200);

    expect(reconnections).toHaveLength(1);
    expect(reconnections[0]).toMatchObject({ attempt: 1, reason: 'recycle' });
    expect(socket.getStats().reconnects).toBe(1);
    expect(sockets).toHaveLength(2);
    sockets[1]!.emit('open');
    expect(socket.isConnected()).toBe(true);

    socket.stop();
  });

  it('terminates a silent connection when the pong-timeout watchdog fires', async () => {
    const { socket, sockets } = makeHarness({
      livenessTimeoutMs: 5_000,
      livenessCheckIntervalMs: 1_000,
      recycleAfterMs: 3_600_000,
    });
    socket.on('error', () => {});
    socket.start();
    sockets[0]!.emit('open');

    await vi.advanceTimersByTimeAsync(6_000);
    expect(sockets[0]!.terminated).toBe(true);
    await vi.advanceTimersByTimeAsync(200);
    expect(sockets).toHaveLength(2);
    sockets[1]!.emit('open');
    socket.stop();
  });

  it('keeps the connection alive while pongs arrive and times out only after silence', async () => {
    const { socket, sockets } = makeHarness({
      livenessTimeoutMs: 5_000,
      livenessCheckIntervalMs: 1_000,
      recycleAfterMs: 3_600_000,
    });
    socket.on('error', () => {});
    socket.start();
    sockets[0]!.emit('open');

    await vi.advanceTimersByTimeAsync(4_000);
    sockets[0]!.emit('pong'); // lastPongAtMs ~= 4000
    await vi.advanceTimersByTimeAsync(5_000); // ticks through t=9000, worst diff = 5000
    expect(sockets[0]!.terminated).toBe(false);
    expect(sockets).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(1_500); // tick at t=10000 sees diff 6000
    expect(sockets[0]!.terminated).toBe(true);
    await vi.advanceTimersByTimeAsync(200);
    expect(sockets).toHaveLength(2);
    sockets[1]!.emit('open');
    socket.stop();
  });

  it('drains subscribe bursts through the control-rate limiter at one message per interval', async () => {
    const { socket, sockets } = makeHarness();
    socket.on('error', () => {});
    socket.start();
    sockets[0]!.emit('open');

    socket.subscribe(['btcusdt@a']);
    socket.subscribe(['btcusdt@b']);
    socket.subscribe(['btcusdt@c']);
    await vi.advanceTimersByTimeAsync(100);
    expect(sockets[0]!.sent).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(150); // t=250
    await vi.advanceTimersByTimeAsync(250); // t=500
    await vi.advanceTimersByTimeAsync(250); // t=750

    expect(sockets[0]!.sent).toEqual([
      JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@a'], id: 1 }),
      JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@b'], id: 2 }),
      JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@c'], id: 3 }),
    ]);

    // Queue drained: no further control frames are sent.
    await vi.advanceTimersByTimeAsync(2_000);
    expect(sockets[0]!.sent).toHaveLength(3);
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
    await vi.advanceTimersByTimeAsync(10_000);

    expect(closedIntentional).toBe(true);
    expect(reconnections).not.toHaveBeenCalled();
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.closes).toEqual([{ code: 1000, reason: 'client-stop' }]);
  });
});
