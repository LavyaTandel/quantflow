import { describe, expect, it, vi } from 'vitest';
import { createMemoryRateLimiter, MemoryRateLimiter } from '../../src/gateway/rateLimiter.js';

describe('MemoryRateLimiter', () => {
  it('enforces starter 60/min', async () => {
    const rl = createMemoryRateLimiter();
    const id = 't-starter';
    for (let i = 0; i < 60; i++) {
      const r = await rl.check(id, 'starter');
      expect(r.allowed).toBe(true);
      if (i === 59) expect(r.remaining).toBe(0);
    }
    const blocked = await rl.check(id, 'starter');
    expect(blocked.allowed).toBe(false);
    expect(blocked.remaining).toBe(0);
  });

  it('enforces pro 300/min', async () => {
    const rl = new MemoryRateLimiter();
    const id = 't-pro';
    for (let i = 0; i < 300; i++) {
      const r = await rl.check(id, 'pro');
      expect(r.allowed).toBe(true);
    }
    const blocked = await rl.check(id, 'pro');
    expect(blocked.allowed).toBe(false);
  });

  it('resets after window', async () => {
    const rl = new MemoryRateLimiter();
    const id = 't-reset';
    vi.useFakeTimers();
    try {
      const base = 1_700_000_000_000;
      vi.setSystemTime(base);
      for (let i = 0; i < 60; i++) {
        const r = await rl.check(id, 'starter');
        expect(r.allowed).toBe(true);
      }
      const blocked = await rl.check(id, 'starter');
      expect(blocked.allowed).toBe(false);
      vi.setSystemTime(base + 60_000 + 1);
      const after = await rl.check(id, 'starter');
      expect(after.allowed).toBe(true);
      expect(after.remaining).toBe(59);
    } finally {
      vi.useRealTimers();
    }
  });

  it('enterprise is unlimited', async () => {
    const rl = createMemoryRateLimiter();
    const id = 't-ent';
    for (let i = 0; i < 500; i++) {
      const r = await rl.check(id, 'enterprise');
      expect(r.allowed).toBe(true);
      expect(r.remaining).toBe(Infinity);
    }
  });

  it('isolates second tenant', async () => {
    const rl = createMemoryRateLimiter();
    for (let i = 0; i < 60; i++) {
      expect((await rl.check('a', 'starter')).allowed).toBe(true);
    }
    expect((await rl.check('a', 'starter')).allowed).toBe(false);
    expect((await rl.check('b', 'starter')).allowed).toBe(true);
    expect((await rl.check('b', 'starter')).remaining).toBe(58);
  });
});
