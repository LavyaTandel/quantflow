import { describe, it, expect } from 'vitest';
import { Redlock } from '../../src/ledger/redlock.js';
import { FakeRedis } from '../../src/ledger/service.js';

describe('Redlock single-instance SET NX PX + fencing', () => {
  it('acquire success', async () => {
    const redis = new FakeRedis() as unknown as import('../../src/ledger/redlock.js').RedisLike;
    const rl = new Redlock(redis);
    const h = await rl.acquire('ledger:lock:t1:acc1:USDT', 9000);
    expect(h.token).toMatch(/:/);
    expect(h.fencingToken).toBe(1);
    await h.release();
  });

  it('second concurrent acquire fails', async () => {
    const redis = new FakeRedis() as unknown as import('../../src/ledger/redlock.js').RedisLike;
    const rl = new Redlock(redis);
    const h1 = await rl.acquire('ledger:lock:t1:acc1:USDT', 9000);
    await expect(rl.acquire('ledger:lock:t1:acc1:USDT', 9000)).rejects.toThrow(/lock not acquired/);
    await h1.release();
  });

  it('release ownership check — only holder can release', async () => {
    const redis = new FakeRedis() as unknown as import('../../src/ledger/redlock.js').RedisLike;
    const rl = new Redlock(redis);
    const h1 = await rl.acquire('ledger:lock:t1:acc1:USDT', 9000);
    // steal attempt: try to release with wrong token directly via redis eval
    const fakeToken = 'steal:999';
    const wrong = await redis.eval(
      'if redis.call("get",KEYS[1])==ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end',
      { keys: ['ledger:lock:t1:acc1:USDT'], arguments: [fakeToken] },
    );
    expect(wrong).toBe(0);
    // original still holds
    const cur = await redis.get('ledger:lock:t1:acc1:USDT');
    expect(cur).toBe(h1.token);
    // proper release deletes
    await h1.release();
    const after = await redis.get('ledger:lock:t1:acc1:USDT');
    expect(after).toBeNull();
    // second release is idempotent
    await h1.release();
  });

  it('TTL expiry allows re-acquire', async () => {
    const redis = new FakeRedis() as unknown as import('../../src/ledger/redlock.js').RedisLike;
    const rl = new Redlock(redis);
    const h1 = await rl.acquire('ledger:lock:t1:acc1:USDT', 50);
    expect(h1.fencingToken).toBe(1);
    // don't release, wait for TTL
    await new Promise((r) => setTimeout(r, 70));
    const h2 = await rl.acquire('ledger:lock:t1:acc1:USDT', 9000);
    expect(h2.fencingToken).toBe(2);
    await h2.release();
  });

  it('fencing token monotonic', async () => {
    const redis = new FakeRedis() as unknown as import('../../src/ledger/redlock.js').RedisLike;
    const rl = new Redlock(redis);
    const tokens: number[] = [];
    for (let i = 0; i < 5; i++) {
      const h = await rl.acquire(`ledger:lock:t1:acc${i}:USDT`, 9000);
      tokens.push(h.fencingToken);
      await h.release();
    }
    // Each key has its own fence counter; same key should monotonic
    const redis2 = new FakeRedis() as unknown as import('../../src/ledger/redlock.js').RedisLike;
    const rl2 = new Redlock(redis2);
    const seq: number[] = [];
    for (let i = 0; i < 3; i++) {
      const h = await rl2.acquire('ledger:lock:t1:same:USDT', 9000);
      seq.push(h.fencingToken);
      await h.release();
    }
    expect(seq).toEqual([1, 2, 3]);
    seq.forEach((v, idx) => {
      if (idx > 0) expect(v).toBeGreaterThan(seq[idx - 1]!);
    });
  });
});
