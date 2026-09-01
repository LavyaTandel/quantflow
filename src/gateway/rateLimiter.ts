import { createClient } from 'redis';
import { getTier } from '../billing/tiers.js';

export interface RateLimiterResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;
}

export interface RateLimiter {
  check(tenantId: string, tier: string): Promise<RateLimiterResult>;
}

const WINDOW_MS = 60_000;

function tierLimit(tier: string): number {
  const def = getTier(tier);
  if (def) return def.rateLimits.api;
  return 60;
}

function windowStartFor(now: number): number {
  return Math.floor(now / WINDOW_MS) * WINDOW_MS;
}

export interface RedisRateLimiterClient {
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number): Promise<number>;
  get?(key: string): Promise<string | null>;
  connect?(): Promise<void>;
  isOpen?: boolean;
}

class InMemoryRateLimiterRedis implements RedisRateLimiterClient {
  private readonly counts = new Map<string, number>();
  private readonly exps = new Map<string, number>();

  async incr(key: string): Promise<number> {
    const now = Date.now();
    const exp = this.exps.get(key);
    if (exp !== undefined && exp <= now) {
      this.counts.delete(key);
      this.exps.delete(key);
    }
    const cur = this.counts.get(key) ?? 0;
    const next = cur + 1;
    this.counts.set(key, next);
    return next;
  }

  async expire(key: string, seconds: number): Promise<number> {
    this.exps.set(key, Date.now() + seconds * 1000);
    return 1;
  }

  async get(key: string): Promise<string | null> {
    const exp = this.exps.get(key);
    if (exp !== undefined && exp <= Date.now()) {
      this.counts.delete(key);
      this.exps.delete(key);
      return null;
    }
    const v = this.counts.get(key);
    return v !== undefined ? String(v) : null;
  }

  flush(): void {
    this.counts.clear();
    this.exps.clear();
  }
}

export { InMemoryRateLimiterRedis as FakeRedisRateLimiterClient };

export class MemoryRateLimiter implements RateLimiter {
  readonly #store = new Map<string, { count: number; windowStart: number }>();

  async check(tenantId: string, tier: string): Promise<RateLimiterResult> {
    const limit = tierLimit(tier);
    const now = Date.now();
    const ws = windowStartFor(now);
    const resetAt = ws + WINDOW_MS;

    if (!Number.isFinite(limit)) {
      return { allowed: true, remaining: Infinity, resetAt };
    }

    const key = tenantId;
    const entry = this.#store.get(key);

    if (!entry || entry.windowStart !== ws) {
      this.#store.set(key, { count: 1, windowStart: ws });
      return { allowed: true, remaining: limit - 1, resetAt };
    }

    if (entry.count < limit) {
      entry.count += 1;
      return { allowed: true, remaining: limit - entry.count, resetAt };
    }

    return { allowed: false, remaining: 0, resetAt };
  }
}

/**
 * Redis-backed limiter: `ratelimit:{tenantId}:{windowStart}` via INCR + EXPIRE 60
 * windowStart = floor(now/60s)*60s, resetAt = windowStart+60000, remaining = max(0, limit-count)
 * Enterprise (Infinity) always allowed. Every rejection is counted (no silent drops).
 */
export class RedisRateLimiter implements RateLimiter {
  private readonly client: RedisRateLimiterClient;
  private _connected = false;
  private _connectPromise: Promise<void> | null = null;
  private _useFallback = false;
  private readonly fallback = new MemoryRateLimiter();

  constructor(client?: RedisRateLimiterClient) {
    if (client) {
      this.client = client;
      const maybeReal = client as unknown as { connect?: () => Promise<void>; isOpen?: boolean };
      if (typeof maybeReal.connect === 'function' && maybeReal.isOpen === false) {
        this._connected = false;
      } else {
        this._connected = true;
      }
    } else {
      // Hermetic default: in-memory fake; real via createWithRealRedis() which uses createClient from redis
      this.client = new InMemoryRateLimiterRedis();
      this._connected = true;
    }
  }

  static createWithRealRedis(
    url = process.env['REDIS_URL'] ?? 'redis://localhost:6379',
  ): RedisRateLimiter {
    const c = createClient({ url }) as unknown as RedisRateLimiterClient;
    return new RedisRateLimiter(c);
  }

  private async ensureConnected(): Promise<void> {
    if (this._connected || this._useFallback) return;
    const c = this.client as unknown as { connect?: () => Promise<void>; isOpen?: boolean };
    if (typeof c.connect === 'function' && c.isOpen === false) {
      if (!this._connectPromise) {
        this._connectPromise = c
          .connect()
          .then(() => {
            this._connected = true;
          })
          .catch(() => {
            this._useFallback = true;
          });
      }
      await this._connectPromise;
    } else {
      this._connected = true;
    }
  }

  async check(tenantId: string, tier: string): Promise<RateLimiterResult> {
    const limit = tierLimit(tier);
    const now = Date.now();
    const ws = windowStartFor(now);
    const resetAt = ws + WINDOW_MS;
    if (!Number.isFinite(limit)) {
      return { allowed: true, remaining: Infinity, resetAt };
    }
    if (this._useFallback) return this.fallback.check(tenantId, tier);
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.check(tenantId, tier);
    }
    if (this._useFallback) return this.fallback.check(tenantId, tier);
    const key = `ratelimit:${tenantId}:${ws}`;
    const count = await this.client.incr(key);
    if (count === 1) await this.client.expire(key, 60);
    const allowed = count <= limit;
    const remaining = Math.max(0, limit - count);
    return { allowed, remaining, resetAt };
  }

  async flushForTests(): Promise<void> {
    const anyClient = this.client as unknown as InMemoryRateLimiterRedis & {
      sendCommand?: (args: string[]) => Promise<unknown>;
    };
    if (typeof anyClient.flush === 'function') {
      anyClient.flush();
      return;
    }
    try {
      if (anyClient.sendCommand) await anyClient.sendCommand(['FLUSHDB']);
    } catch {
      // ignore
    }
  }
}

export function createMemoryRateLimiter(): RateLimiter {
  return new MemoryRateLimiter();
}
