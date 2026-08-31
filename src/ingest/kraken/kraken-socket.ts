// Kraken Spot WebSocket v2 market-data socket.
// Connection posture per Kraken docs (Aug 2026): application-level ping at least
// every 60 s (~1 min idle close) and Cloudflare ~150 connect attempts per rolling
// 10 min per IP before a 10-min ban -> ConnectBudget(150, 10 min), app-ping <=50 s.
import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import {
  BackoffScheduler,
  ControlRateLimiter,
  type BackoffSchedulerOptions,
  type CloseInfo,
  type ConnectionInfo,
  type ControlMessage,
  type SocketStats,
} from '../binance-socket.js';
import { KRAKEN_SYMBOLS } from './normalize.js';

export const KRAKEN_WS_URL = 'wss://ws.kraken.com/v2';
export const KRAKEN_MAX_CONNECTS_PER_WINDOW = 150;
export const KRAKEN_BUDGET_WINDOW_MS = 10 * 60 * 1000;
export const KRAKEN_PING_INTERVAL_MS = 50_000;
export const KRAKEN_LIVENESS_TIMEOUT_MS = 110_000;

const CONTROL_MIN_INTERVAL_MS = 250;
const CONTROL_MAX_PER_INTERVAL = 5;
const DEFAULT_RECYCLE_AFTER_MS = 23 * 60 * 60 * 1000;
const DEFAULT_LIVENESS_CHECK_MS = 15_000;

export type KrakenChannel = 'book' | 'trade' | 'ticker';
export type KrakenDepth = 10 | 25 | 100 | 500 | 1000;

export interface KrakenSocketConfig {
  url?: string;
  symbols?: readonly string[];
  channels?: readonly KrakenChannel[];
  depth?: KrakenDepth;
  recycleAfterMs?: number;
  livenessTimeoutMs?: number;
  livenessCheckIntervalMs?: number;
  pingIntervalMs?: number;
  backoff?: BackoffSchedulerOptions;
  wsFactory?: (url: string) => WebSocket;
}

export interface KrakenSocketStats extends SocketStats {
  pingsSent: number;
}

export interface KrakenSocketEvents {
  open: [];
  frame: [unknown];
  frameError: [string];
  closed: [CloseInfo];
  reconnectScheduled: [ConnectionInfo];
  recycled: [];
  error: [Error];
  pong: [];
}

type EventListener<K extends keyof KrakenSocketEvents> = (...args: KrakenSocketEvents[K]) => void;

interface SubscribePlan {
  channel: KrakenChannel;
  depth?: KrakenDepth;
}

function buildSubscribeMessage(
  plan: SubscribePlan,
  symbols: readonly string[],
): Record<string, unknown> {
  return {
    method: 'subscribe',
    params:
      plan.depth === undefined
        ? { channel: plan.channel, symbol: [...symbols] }
        : { channel: plan.channel, symbol: [...symbols], depth: plan.depth },
  };
}

export class KrakenMarketSocket extends EventEmitter {
  private readonly config: Required<
    Pick<
      KrakenSocketConfig,
      | 'url'
      | 'symbols'
      | 'recycleAfterMs'
      | 'livenessTimeoutMs'
      | 'livenessCheckIntervalMs'
      | 'pingIntervalMs'
    >
  >;
  private readonly plans: readonly SubscribePlan[];
  private readonly scheduler: BackoffScheduler;
  private readonly limiter: ControlRateLimiter;
  private readonly wsFactory: (url: string) => WebSocket;

  private ws: WebSocket | null = null;
  private running = false;
  private recycling = false;
  private connectTimer: NodeJS.Timeout | null = null;
  private recycleTimer: NodeJS.Timeout | null = null;
  private livenessTimer: NodeJS.Timeout | null = null;
  private pingTimer: NodeJS.Timeout | null = null;
  private limiterTimer: NodeJS.Timeout | null = null;
  private lastLimiterSendMs = 0;

  private stats: KrakenSocketStats = {
    connects: 0,
    reconnects: 0,
    framesReceived: 0,
    parseErrors: 0,
    openedAtMs: null,
    lastPongAtMs: null,
    pingsSent: 0,
  };

  constructor(config: KrakenSocketConfig = {}) {
    super();
    this.config = {
      url: config.url ?? KRAKEN_WS_URL,
      symbols: config.symbols ?? KRAKEN_SYMBOLS,
      recycleAfterMs: config.recycleAfterMs ?? DEFAULT_RECYCLE_AFTER_MS,
      livenessTimeoutMs: config.livenessTimeoutMs ?? KRAKEN_LIVENESS_TIMEOUT_MS,
      livenessCheckIntervalMs: config.livenessCheckIntervalMs ?? DEFAULT_LIVENESS_CHECK_MS,
      pingIntervalMs: Math.min(config.pingIntervalMs ?? KRAKEN_PING_INTERVAL_MS, 60_000),
    };
    const requestedChannels = config.channels ?? (['book', 'trade'] as const);
    this.plans = requestedChannels.map((channel) =>
      channel === 'book' ? { channel, depth: config.depth ?? 100 } : { channel },
    );
    this.scheduler = new BackoffScheduler({
      maxAttemptsPerWindow: KRAKEN_MAX_CONNECTS_PER_WINDOW,
      windowMs: KRAKEN_BUDGET_WINDOW_MS,
      ...config.backoff,
    });
    this.limiter = new ControlRateLimiter(CONTROL_MIN_INTERVAL_MS, CONTROL_MAX_PER_INTERVAL);
    this.wsFactory = config.wsFactory ?? ((url) => new WebSocket(url));
  }

  override on<K extends keyof KrakenSocketEvents>(event: K, listener: EventListener<K>): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  override once<K extends keyof KrakenSocketEvents>(event: K, listener: EventListener<K>): this {
    return super.once(event, listener as (...args: unknown[]) => void);
  }

  override off<K extends keyof KrakenSocketEvents>(event: K, listener: EventListener<K>): this {
    return super.off(event, listener as (...args: unknown[]) => void);
  }

  override emit<K extends keyof KrakenSocketEvents>(
    event: K,
    ...args: KrakenSocketEvents[K]
  ): boolean {
    return super.emit(event, ...args);
  }

  getStats(): KrakenSocketStats {
    return { ...this.stats };
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.dial('initial');
  }

  stop(): void {
    this.running = false;
    this.clearTimers();
    if (this.ws) {
      this.ws.close(1000, 'client-stop');
      this.ws = null;
    }
  }

  subscribe(channels: readonly KrakenChannel[]): void {
    for (const channel of channels) {
      this.enqueueControl({ method: 'SUBSCRIBE', params: [channel] });
    }
    this.ensureLimiterTimer();
  }

  unsubscribe(channels: readonly KrakenChannel[]): void {
    for (const channel of channels) {
      this.enqueueControl({ method: 'UNSUBSCRIBE', params: [channel] });
    }
    this.ensureLimiterTimer();
  }

  private enqueueControl(message: Omit<ControlMessage, 'id'>): void {
    this.limiter.enqueue(message);
  }

  private ensureLimiterTimer(): void {
    if (this.limiterTimer || !this.running) return;
    this.limiterTimer = setInterval(() => this.flushControl(), CONTROL_MIN_INTERVAL_MS);
    this.limiterTimer.unref?.();
  }

  private flushControl(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const now = Date.now();
    for (const control of this.limiter.takeDue(now, this.lastLimiterSendMs)) {
      const plan: SubscribePlan = {
        channel: control.params[0] as KrakenChannel,
        depth: this.plans.find((p) => p.channel === control.params[0])?.depth,
      };
      const wire =
        control.method === 'SUBSCRIBE'
          ? buildSubscribeMessage(plan, this.config.symbols)
          : {
              method: 'unsubscribe',
              params: { channel: plan.channel, symbol: [...this.config.symbols] },
            };
      this.ws.send(JSON.stringify(wire));
      this.lastLimiterSendMs = now;
    }
    if (this.limiter.pending === 0 && this.limiterTimer) {
      clearInterval(this.limiterTimer);
      this.limiterTimer = null;
    }
  }

  private dial(reason: ConnectionInfo['reason']): void {
    if (!this.running) return;
    const ws = this.wsFactory(this.config.url);
    this.ws = ws;
    if (reason !== 'initial') this.stats.reconnects += 1;
    this.stats.connects += 1;

    ws.on('open', () => this.handleOpen());
    ws.on('message', (data) => this.handleMessage(data));
    ws.on('ping', () => {
      ws.pong();
    });
    ws.on('error', (err: Error) => {
      this.emit('error', err);
    });
    ws.on('close', (code: number, reasonBuffer: Buffer) => {
      this.handleClose(code, reasonBuffer.toString('utf8'));
    });
  }

  private handleOpen(): void {
    this.scheduler.reset();
    this.stats.openedAtMs = Date.now();
    this.recycling = false;
    this.emit('open');
    for (const plan of this.plans) {
      this.enqueueControl({ method: 'SUBSCRIBE', params: [plan.channel] });
    }
    this.ensureLimiterTimer();
    this.scheduleRecycle();
    this.startLivenessWatch();
    this.startPingLoop();
  }

  private startPingLoop(): void {
    this.pingTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      this.ws.send(JSON.stringify({ method: 'ping' }));
      this.stats.pingsSent += 1;
    }, this.config.pingIntervalMs);
    this.pingTimer.unref?.();
  }

  private scheduleRecycle(): void {
    this.recycleTimer = setTimeout(() => {
      this.emit('recycled');
      this.recycling = true;
      this.ws?.close(1000, 'proactive-recycle');
    }, this.config.recycleAfterMs);
    this.recycleTimer.unref?.();
  }

  private startLivenessWatch(): void {
    this.livenessTimer = setInterval(() => {
      const anchor = this.stats.lastPongAtMs ?? this.stats.openedAtMs;
      if (anchor === null) return;
      if (Date.now() - anchor > this.config.livenessTimeoutMs) {
        this.ws?.terminate();
      }
    }, this.config.livenessCheckIntervalMs);
    this.livenessTimer.unref?.();
  }

  private handleMessage(data: WebSocket.RawData): void {
    this.stats.framesReceived += 1;
    let parsed: unknown;
    try {
      parsed = JSON.parse(data.toString('utf8'));
    } catch {
      this.stats.parseErrors += 1;
      this.emit('frameError', data.toString('utf8').slice(0, 256));
      return;
    }
    if (isRecord(parsed) && parsed.method === 'pong') {
      this.stats.lastPongAtMs = Date.now();
      this.emit('pong');
    }
    this.emit('frame', parsed);
  }

  private handleClose(code: number, reason: string): void {
    this.clearConnectionTimers();
    this.ws = null;
    const intentional = !this.running || this.recycling;
    this.emit('closed', { code, reason, intentional });
    if (!this.running) return;
    const cause: ConnectionInfo['reason'] = this.recycling ? 'recycle' : 'error';
    const delayMs = this.scheduler.nextDelayMs(Date.now());
    this.emit('reconnectScheduled', {
      attempt: this.scheduler.attemptCount,
      delayMs,
      reason: cause,
    });
    this.connectTimer = setTimeout(() => this.dial(cause), delayMs);
    this.connectTimer.unref?.();
  }

  private clearConnectionTimers(): void {
    if (this.recycleTimer) clearTimeout(this.recycleTimer);
    if (this.livenessTimer) clearInterval(this.livenessTimer);
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.limiterTimer) clearInterval(this.limiterTimer);
    this.recycleTimer = null;
    this.livenessTimer = null;
    this.pingTimer = null;
    this.limiterTimer = null;
  }

  private clearTimers(): void {
    this.clearConnectionTimers();
    if (this.connectTimer) clearTimeout(this.connectTimer);
    this.connectTimer = null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
