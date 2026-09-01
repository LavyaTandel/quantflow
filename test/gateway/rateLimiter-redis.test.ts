import { describe, it, expect, beforeEach, vi } from 'vitest';
import { RedisRateLimiter, FakeRedisRateLimiterClient } from '../../src/gateway/rateLimiter.js';
import { createClient } from 'redis';

function createFake(): FakeRedisRateLimiterClient {
  return new FakeRedisRateLimiterClient();
}

async function tryRealClient(): Promise<FakeRedisRateLimiterClient | null> {
  try {
    const url = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
    const real = createClient({ url });
    await real.connect();
    // Do not flushDb globally — use unique keys per test for isolation
    const adapter = {
      incr: (k: string) => (real as unknown as { incr: (k: string) => Promise<number> }).incr(k),
      expire: (k: string, s: number) =>
        (real as unknown as { expire: (k: string, s: number) => Promise<number> }).expire(k, s),
      get: (k: string) =>
        (real as unknown as { get: (k: string) => Promise<string | null> }).get(k),
      connect: async () => {
        return;
      },
      isOpen: true,
      _real: real,
      flush: async () => {
        return;
      },
    } as unknown as FakeRedisRateLimiterClient;
    (globalThis as unknown as { __realRateLimiterRedis?: typeof real }).__realRateLimiterRedis =
      real;
    return adapter;
  } catch {
    return null;
  }
}

describe('RedisRateLimiter hermetic with FakeRedis', () => {
  let client: FakeRedisRateLimiterClient;
  let rl: RedisRateLimiter;

  beforeEach(() => {
    client = createFake();
    rl = new RedisRateLimiter(client);
  });

  it('enforces starter 60/min with INCR+EXPIRE', async () => {
    const id = 't-starter-redis';
    for (let i = 0; i < 60; i++) {
      const r = await rl.check(id, 'starter');
      expect(r.allowed).toBe(true);
      if (i === 59) expect(r.remaining).toBe(0);
    }
    const blocked = await rl.check(id, 'starter');
    expect(blocked.allowed).toBe(false);
    expect(blocked.remaining).toBe(0);
  });

  it('second tenant rate-limit isolation', async () => {
    for (let i = 0; i < 60; i++) {
      expect((await rl.check('tenant-a', 'starter')).allowed).toBe(true);
    }
    expect((await rl.check('tenant-a', 'starter')).allowed).toBe(false);
    expect((await rl.check('tenant-b', 'starter')).allowed).toBe(true);
    expect((await rl.check('tenant-b', 'starter')).remaining).toBe(58);
  });

  it('rate-limit state survives simulated restart (new limiter with same Redis)', async () => {
    const id = 'restart-tenant';
    for (let i = 0; i < 30; i++) {
      expect((await rl.check(id, 'starter')).allowed).toBe(true);
    }
    // Simulate restart: new limiter instance with same underlying redis client
    const rl2 = new RedisRateLimiter(client);
    for (let i = 0; i < 30; i++) {
      const r = await rl2.check(id, 'starter');
      expect(r.allowed).toBe(true);
    }
    const blocked = await rl2.check(id, 'starter');
    expect(blocked.allowed).toBe(false);
  });

  it('window reset allows again after 60s', async () => {
    vi.useFakeTimers();
    try {
      const base = 1_700_000_000_000;
      vi.setSystemTime(base);
      const c = createFake();
      const limiter = new RedisRateLimiter(c);
      for (let i = 0; i < 60; i++) {
        expect((await limiter.check('t-reset', 'starter')).allowed).toBe(true);
      }
      expect((await limiter.check('t-reset', 'starter')).allowed).toBe(false);
      vi.setSystemTime(base + 60_001);
      const after = await limiter.check('t-reset', 'starter');
      expect(after.allowed).toBe(true);
      expect(after.remaining).toBe(59);
    } finally {
      vi.useRealTimers();
    }
  });

  it('enterprise is unlimited', async () => {
    for (let i = 0; i < 500; i++) {
      const r = await rl.check('ent', 'enterprise');
      expect(r.allowed).toBe(true);
      expect(r.remaining).toBe(Infinity);
    }
  });

  it('remaining and resetAt shape', async () => {
    const r = await rl.check('t-shape', 'starter');
    expect(r.allowed).toBe(true);
    expect(r.remaining).toBe(59);
    expect(r.resetAt).toBeGreaterThan(Date.now());
  });
});

describe('RedisRateLimiter with real redis if available', () => {
  it('survives restart on real redis, else skips gracefully', async () => {
    const realClient = await tryRealClient();
    if (!realClient) {
      const fake = createFake();
      const r = new RedisRateLimiter(fake);
      expect((await r.check('fallback', 'starter')).allowed).toBe(true);
      return;
    }
    const rl1 = new RedisRateLimiter(realClient);
    const id = `real-restart-tenant-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    for (let i = 0; i < 10; i++) await rl1.check(id, 'starter');
    const rl2 = new RedisRateLimiter(realClient);
    const r = await rl2.check(id, 'starter');
    expect(r.allowed).toBe(true);
    // Survives restart: second limiter continues same window, remaining should be 49 (10+1)
    expect(r.remaining).toBe(49);
    const maybeReal = (
      globalThis as unknown as { __realRateLimiterRedis?: { quit: () => Promise<void> } }
    ).__realRateLimiterRedis;
    if (maybeReal) {
      // cleanup only this key, not whole DB
      try {
        const windowStart = Math.floor(Date.now() / 60000) * 60000;
        await (maybeReal as unknown as { del: (k: string) => Promise<number> }).del(
          `ratelimit:${id}:${windowStart}`,
        );
      } catch (_e) {
        void _e;
      }
      await maybeReal.quit().catch(() => {});
    }
  });
});
