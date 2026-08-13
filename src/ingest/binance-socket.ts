import { EventEmitter } from 'node:events';
import WebSocket from 'ws';

export interface ConnectBudgetOptions {
  maxAttemptsPerWindow?: number;
  windowMs?: number;
}

export class ConnectBudget {
  private tokens: number;
  private lastRefillMs: number;

  readonly capacity: number;
  private readonly ratePerMs: number;

  constructor(opts: ConnectBudgetOptions = {}) {
    this.capacity = opts.maxAttemptsPerWindow ?? 300;
    const windowMs = opts.windowMs ?? 300_000;
    this.ratePerMs = this.capacity / windowMs;
    this.tokens = this.capacity;
    this.lastRefillMs = 0;
  }

  advance(nowMs: number): void {
    if (nowMs <= this.lastRefillMs) return;
    const elapsed = nowMs - this.lastRefillMs;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.ratePerMs);
    this.lastRefillMs = nowMs;
  }

  acquireWaitMs(nowMs: number): number {
    this.advance(nowMs);
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return 0;
    }
    const deficit = 1 - this.tokens;
    this.tokens = 0;
    return Math.ceil(deficit / this.ratePerMs);
  }
}

export interface BackoffSchedulerOptions extends ConnectBudgetOptions {
  baseDelayMs?: number;
  maxDelayMs?: number;
  rng?: () => number;
}

export class BackoffScheduler {
  private attempts = 0;
  private readonly budget: ConnectBudget;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly rng: () => number;

  constructor(opts: BackoffSchedulerOptions = {}) {
    this.baseDelayMs = Math.max(1, opts.baseDelayMs ?? 500);
    this.maxDelayMs = Math.max(this.baseDelayMs, opts.maxDelayMs ?? 30_000);
    this.rng = opts.rng ?? Math.random;
    this.budget = new ConnectBudget(opts);
  }

  get attemptCount(): number {
    return this.attempts;
  }

  nextDelayMs(nowMs: number = Date.now()): number {
    const exponent = Math.min(this.attempts, 62);
    const uncapped = this.baseDelayMs * 2 ** exponent;
    const ceiling = Math.min(this.maxDelayMs, uncapped);
    this.attempts += 1;
    const jitter = this.rng() * ceiling;
    const budgetWaitMs = this.budget.acquireWaitMs(nowMs);
    return Math.ceil(jitter + budgetWaitMs);
  }

  reset(): void {
    this.attempts = 0;
  }
}

export type ControlMessage = { method: 'SUBSCRIBE' | 'UNSUBSCRIBE'; params: string[]; id: number };

export class ControlRateLimiter {
  private queue = new Map<string, ControlMessage>();
  private nextId = 1;

  constructor(
    private readonly minIntervalMs = 250,
    private readonly maxPerInterval = 1,
  ) {}

  enqueue(message: Omit<ControlMessage, 'id'>): ControlMessage | undefined {
    const key = `${message.method}:${[...message.params].sort().join(',')}`;
    if (this.queue.has(key)) return undefined;
    const full: ControlMessage = { ...message, id: this.nextId++ };
    this.queue.set(key, full);
    return full;
  }

  takeDue(nowMs: number, lastSentMs: number): ControlMessage[] {
    if (nowMs - lastSentMs < this.minIntervalMs) return [];
    const out: ControlMessage[] = [];
    for (let i = 0; i < this.maxPerInterval; i++) {
      const next = this.queue.entries().next();
      if (next.done) break;
      this.queue.delete(next.value[0]);
      out.push(next.value[1]);
    }
    return out;
  }

  get pending(): number {
    return this.queue.size;
  }
}

export interface BinanceSocketConfig {
  baseUrl?: string;
  symbols?: readonly string[];
  streams?: readonly string[];
  recycleAfterMs?: number;
  livenessTimeoutMs?: number;
  livenessCheckIntervalMs?: number;
  backoff?: BackoffSchedulerOptions;
  wsFactory?: (url: string) => WebSocket;
}

export interface ConnectionInfo {
  attempt: number;
  delayMs: number;
  reason: 'initial' | 'error' | 'recycle';
}

export interface CloseInfo {
  code: number;
  reason: string;
  intentional: boolean;
}

export interface SocketStats {
  connects: number;
  reconnects: number;
  framesReceived: number;
  parseErrors: number;
  openedAtMs: number | null;
  lastPongAtMs: number | null;
}

export interface SocketEvents {
  open: [];
  frame: [unknown];
  frameError: [string];
  closed: [CloseInfo];
  reconnectScheduled: [ConnectionInfo];
  recycled: [];
  error: [Error];
}

type EventListener<K extends keyof SocketEvents> = (...args: SocketEvents[K]) => void;

export const DEFAULT_SYMBOLS: readonly string[] = [
  'BTCUSDT',
  'ETHUSDT',
  'SOLUSDT',
  'XRPUSDT',
  'DOGEUSDT',
  'ADAUSDT',
];

export const DEFAULT_STREAMS: readonly string[] = ['aggTrade', 'bookTicker', 'depth@100ms'];

const DEFAULT_BASE_URL = 'wss://data-stream.binance.vision';
const DEFAULT_RECYCLE_AFTER_MS = 23 * 60 * 60 * 1000;
const DEFAULT_LIVENESS_TIMEOUT_MS = 90_000;
const DEFAULT_LIVENESS_CHECK_MS = 15_000;
const CONTROL_FLUSH_INTERVAL_MS = 250;

export class BinanceMarketSocket extends EventEmitter {
  private readonly config: Required<
    Pick<
      BinanceSocketConfig,
      | 'baseUrl'
      | 'symbols'
      | 'streams'
      | 'recycleAfterMs'
      | 'livenessTimeoutMs'
      | 'livenessCheckIntervalMs'
    >
  >;
  private readonly scheduler: BackoffScheduler;
  private readonly limiter: ControlRateLimiter;
  private readonly wsFactory: (url: string) => WebSocket;

  private ws: WebSocket | null = null;
  private running = false;
  private recycling = false;
  private connectTimer: NodeJS.Timeout | null = null;
  private recycleTimer: NodeJS.Timeout | null = null;
  private livenessTimer: NodeJS.Timeout | null = null;
  private lastLimiterSendMs = 0;
  private limiterTimer: NodeJS.Timeout | null = null;

  private stats: SocketStats = {
    connects: 0,
    reconnects: 0,
    framesReceived: 0,
    parseErrors: 0,
    openedAtMs: null,
    lastPongAtMs: null,
  };

  constructor(config: BinanceSocketConfig = {}) {
    super();
    this.config = {
      baseUrl: config.baseUrl ?? DEFAULT_BASE_URL,
      symbols: config.symbols ?? DEFAULT_SYMBOLS,
      streams: config.streams ?? DEFAULT_STREAMS,
      recycleAfterMs: config.recycleAfterMs ?? DEFAULT_RECYCLE_AFTER_MS,
      livenessTimeoutMs: config.livenessTimeoutMs ?? DEFAULT_LIVENESS_TIMEOUT_MS,
      livenessCheckIntervalMs: config.livenessCheckIntervalMs ?? DEFAULT_LIVENESS_CHECK_MS,
    };
    this.scheduler = new BackoffScheduler(config.backoff);
    this.limiter = new ControlRateLimiter();
    this.wsFactory = config.wsFactory ?? ((url) => new WebSocket(url));
  }

  override on<K extends keyof SocketEvents>(event: K, listener: EventListener<K>): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  override once<K extends keyof SocketEvents>(event: K, listener: EventListener<K>): this {
    return super.once(event, listener as (...args: unknown[]) => void);
  }

  override off<K extends keyof SocketEvents>(event: K, listener: EventListener<K>): this {
    return super.off(event, listener as (...args: unknown[]) => void);
  }

  override emit<K extends keyof SocketEvents>(event: K, ...args: SocketEvents[K]): boolean {
    return super.emit(event, ...args);
  }

  getStats(): SocketStats {
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

  subscribe(streams: readonly string[]): void {
    this.enqueueControl({ method: 'SUBSCRIBE', params: [...streams] });
    this.ensureLimiterTimer();
  }

  unsubscribe(streams: readonly string[]): void {
    this.enqueueControl({ method: 'UNSUBSCRIBE', params: [...streams] });
    this.ensureLimiterTimer();
  }

  private enqueueControl(message: Omit<ControlMessage, 'id'>): void {
    this.limiter.enqueue(message);
  }

  private ensureLimiterTimer(): void {
    if (this.limiterTimer || !this.running) return;
    this.limiterTimer = setInterval(() => this.flushControl(), CONTROL_FLUSH_INTERVAL_MS);
    this.limiterTimer.unref?.();
  }

  private flushControl(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const now = Date.now();
    for (const message of this.limiter.takeDue(now, this.lastLimiterSendMs)) {
      this.ws.send(JSON.stringify(message));
      this.lastLimiterSendMs = now;
    }
    if (this.limiter.pending === 0 && this.limiterTimer) {
      clearInterval(this.limiterTimer);
      this.limiterTimer = null;
    }
  }

  private streamPath(): string {
    const names = this.config.symbols.flatMap((symbol) =>
      this.config.streams.map((stream) => `${symbol.toLowerCase()}@${stream}`),
    );
    return `/stream?streams=${names.join('/')}`;
  }

  private dial(reason: ConnectionInfo['reason']): void {
    if (!this.running) return;
    const url = `${this.config.baseUrl}${this.streamPath()}`;
    const ws = this.wsFactory(url);
    this.ws = ws;
    if (reason !== 'initial') this.stats.reconnects += 1;
    this.stats.connects += 1;

    ws.on('open', () => this.handleOpen());
    ws.on('message', (data) => this.handleMessage(data));
    ws.on('ping', () => {
      ws.pong();
    });
    ws.on('pong', () => {
      this.stats.lastPongAtMs = Date.now();
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
    this.scheduleRecycle();
    this.startLivenessWatch();
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
    if (this.limiterTimer) clearInterval(this.limiterTimer);
    this.recycleTimer = null;
    this.livenessTimer = null;
    this.limiterTimer = null;
  }

  private clearTimers(): void {
    this.clearConnectionTimers();
    if (this.connectTimer) clearTimeout(this.connectTimer);
    this.connectTimer = null;
  }
}
