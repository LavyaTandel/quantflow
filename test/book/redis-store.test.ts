import { describe, expect, it } from 'vitest';
import type { BookRedisClient, BookRedisMulti } from '../../src/book/redis-store.js';
import { RedisBookStore } from '../../src/book/redis-store.js';
import { MemoryBookStore } from '../../src/book/memory-store.js';
import type { Level } from '../../src/book/store.js';
import type { DepthDiff, DepthLevelChange } from '../../src/ingest/types.js';
import type { RedisClientType } from 'redis';

function diff(first: number, final: number, levels: DepthLevelChange[]): DepthDiff {
  return {
    kind: 'depth',
    symbol: 'BTCUSDT',
    ts: 0,
    firstUpdateId: first,
    finalUpdateId: final,
    levels,
  };
}

function levels(entries: Array<[number, number]>): Level[] {
  return entries.map(([price, qty]) => ({ price, qty }));
}

type Thunk = () => unknown;

class FakeRedis implements BookRedisClient {
  readonly zsets = new Map<string, Map<string, number>>();
  readonly hashes = new Map<string, Map<string, string>>();
  readonly hGetAllCounts = new Map<string, number>();
  multiCalls = 0;
  execCalls = 0;
  lastMultiOps: readonly string[] = [];

  hGetAllTotal(): number {
    let total = 0;
    for (const count of this.hGetAllCounts.values()) total += count;
    return total;
  }

  #hash(key: string): Map<string, string> {
    let hash = this.hashes.get(key);
    if (!hash) {
      hash = new Map();
      this.hashes.set(key, hash);
    }
    return hash;
  }

  multi(): BookRedisMulti {
    this.multiCalls += 1;
    const labels: string[] = [];
    const thunks: Thunk[] = [];
    const api: BookRedisMulti = {
      zAdd: (key, members) => {
        labels.push(`ZADD ${key}`);
        thunks.push(() => {
          let zset = this.zsets.get(key);
          if (!zset) {
            zset = new Map();
            this.zsets.set(key, zset);
          }
          for (const member of members) zset.set(member.value, member.score);
          return members.length;
        });
        return api;
      },
      zRem: (key, members) => {
        labels.push(`ZREM ${key}`);
        thunks.push(() => {
          const zset = this.zsets.get(key);
          let removed = 0;
          for (const member of members) if (zset?.delete(member)) removed += 1;
          return removed;
        });
        return api;
      },
      hSet: (key, fieldValue) => {
        labels.push(`HSET ${key}`);
        thunks.push(() => {
          const hash = this.#hash(key);
          for (const [field, value] of Object.entries(fieldValue)) hash.set(field, value);
          return Object.keys(fieldValue).length;
        });
        return api;
      },
      hDel: (key, fields) => {
        labels.push(`HDEL ${key}`);
        thunks.push(() => {
          const hash = this.hashes.get(key);
          let removed = 0;
          for (const field of fields) if (hash?.delete(field)) removed += 1;
          return removed;
        });
        return api;
      },
      del: (keys) => {
        labels.push('DEL');
        thunks.push(() => {
          let removed = 0;
          for (const key of keys) {
            if (this.zsets.delete(key)) removed += 1;
            if (this.hashes.delete(key)) removed += 1;
          }
          return removed;
        });
        return api;
      },
      exec: async () => {
        this.execCalls += 1;
        this.lastMultiOps = [...labels];
        const results: unknown[] = [];
        for (const thunk of thunks) results.push(thunk());
        return results;
      },
    };
    return api;
  }

  async del(keys: string[]): Promise<number> {
    let removed = 0;
    for (const key of keys) {
      if (this.zsets.delete(key)) removed += 1;
      if (this.hashes.delete(key)) removed += 1;
    }
    return removed;
  }

  async hGetAll(key: string): Promise<Record<string, string>> {
    this.hGetAllCounts.set(key, (this.hGetAllCounts.get(key) ?? 0) + 1);
    const hash = this.hashes.get(key);
    return hash ? Object.fromEntries(hash) : {};
  }

  async zRangeWithScores(
    key: string,
    min: number,
    max: number,
    options?: { REV?: boolean },
  ): Promise<Array<{ value: string; score: number }>> {
    const entries = [...(this.zsets.get(key)?.entries() ?? [])].map(([value, score]) => ({
      value,
      score,
    }));
    entries.sort((a, b) => a.score - b.score || (a.value < b.value ? -1 : 1));
    const ordered = options?.REV ? entries.reverse() : entries;
    return ordered.slice(min, max - min + 1);
  }
}

async function seededFake(): Promise<FakeRedis> {
  const fake = new FakeRedis();
  await new RedisBookStore(fake).applySnapshot(
    'BTCUSDT',
    levels([
      [100.5, 2],
      [100.25, 1],
    ]),
    levels([[101, 1]]),
    10,
  );
  return fake;
}

describe('RedisBookStore', () => {
  it('keeps the narrow client interface structurally satisfied by node-redis RedisClientType', () => {
    type IsCompatible = RedisClientType extends BookRedisClient ? true : false;
    const compatible: IsCompatible = true;
    expect(compatible).toBe(true);
  });

  it('populates a snapshot in a single pipeline and replaces prior state', async () => {
    const fake = await seededFake();
    expect(fake.multiCalls).toBe(1);
    expect(fake.hGetAllTotal()).toBe(0);
    expect(fake.lastMultiOps[0]).toBe('DEL');
    expect(await new RedisBookStore(fake).getMeta('BTCUSDT')).toEqual({ lastUpdateId: 10 });
    const side = await new RedisBookStore(fake).topN('BTCUSDT', 5);
    expect(side.bids).toEqual(
      levels([
        [100.5, 2],
        [100.25, 1],
      ]),
    );
    expect(side.asks).toEqual(levels([[101, 1]]));

    await new RedisBookStore(fake).applySnapshot(
      'BTCUSDT',
      levels([[99, 4]]),
      levels([[102, 2]]),
      20,
    );
    expect(fake.multiCalls).toBe(2);
    const replaced = await new RedisBookStore(fake).topN('BTCUSDT', 10);
    expect(replaced.bids).toEqual(levels([[99, 4]]));
    expect(replaced.asks).toEqual(levels([[102, 2]]));
    expect(await new RedisBookStore(fake).getMeta('BTCUSDT')).toEqual({ lastUpdateId: 20 });
  });

  it('batches every level change of an applied diff into exactly one multi call', async () => {
    const fake = await seededFake();
    const store = new RedisBookStore(fake);
    const before = fake.multiCalls;
    const status = await store.applyDepthDiff(
      diff(11, 12, [
        { side: 'bid', price: 100.25, newQty: 1.5 },
        { side: 'bid', price: 100.5, newQty: 0 },
        { side: 'ask', price: 101.25, newQty: 0.5 },
      ]),
    );
    expect(status).toBe('applied');
    expect(fake.multiCalls - before).toBe(1);
    expect(fake.execCalls).toBe(2);
    expect(fake.hGetAllCounts.get('book:BTCUSDT:meta')).toBe(1);
    expect(fake.hGetAllTotal()).toBe(1);
    expect(fake.lastMultiOps).toEqual([
      'ZADD book:BTCUSDT:bids',
      'HSET book:BTCUSDT:qty',
      'ZREM book:BTCUSDT:bids',
      'HDEL book:BTCUSDT:qty',
      'ZADD book:BTCUSDT:asks',
      'HSET book:BTCUSDT:qty',
      'HSET book:BTCUSDT:meta',
    ]);
    const side = await store.topN('BTCUSDT', 10);
    expect(side.bids).toEqual(levels([[100.25, 1.5]]));
    expect(side.asks).toEqual(
      levels([
        [101, 1],
        [101.25, 0.5],
      ]),
    );
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 12 });
    expect(fake.hGetAllCounts.get('book:BTCUSDT:meta')).toBe(1);
  });

  it('issues no client writes at all for stale or gapped diffs', async () => {
    const fake = await seededFake();
    const store = new RedisBookStore(fake);
    const before = fake.multiCalls;
    expect(await store.applyDepthDiff(diff(9, 10, [{ side: 'bid', price: 50, newQty: 1 }]))).toBe(
      'stale',
    );
    expect(await store.applyDepthDiff(diff(20, 30, [{ side: 'bid', price: 51, newQty: 1 }]))).toBe(
      'gap',
    );
    expect(fake.multiCalls).toBe(before);
    expect(fake.hGetAllCounts.get('book:BTCUSDT:meta')).toBe(1);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 10 });
    const side = await store.topN('BTCUSDT', 10);
    expect(side.bids).toHaveLength(2);
  });

  it('reports gaps for symbols without snapshot state and writes nothing', async () => {
    const fake = new FakeRedis();
    const store = new RedisBookStore(fake);
    expect(await store.applyDepthDiff(diff(1, 2, [{ side: 'ask', price: 5, newQty: 1 }]))).toBe(
      'gap',
    );
    expect(fake.multiCalls).toBe(0);
    expect(fake.hGetAllTotal()).toBe(1);
    expect(fake.zsets.size).toBe(0);
    expect(fake.hashes.size).toBe(0);
    expect(await store.topN('ETHUSDT', 3)).toEqual({ bids: [], asks: [] });
  });

  it('decides continuity with zero client reads once the symbol cache is warm', async () => {
    const fake = await seededFake();
    const store = new RedisBookStore(fake);
    expect(
      await store.applyDepthDiff(diff(11, 12, [{ side: 'bid', price: 100.25, newQty: 1.5 }])),
    ).toBe('applied');
    expect(fake.hGetAllTotal()).toBe(1);
    expect(await store.applyDepthDiff(diff(13, 14, [{ side: 'ask', price: 101, newQty: 2 }]))).toBe(
      'applied',
    );
    expect(await store.applyDepthDiff(diff(9, 9, [{ side: 'bid', price: 50, newQty: 1 }]))).toBe(
      'stale',
    );
    expect(await store.applyDepthDiff(diff(30, 40, [{ side: 'bid', price: 51, newQty: 1 }]))).toBe(
      'gap',
    );
    expect(fake.hGetAllTotal()).toBe(1);
    expect(fake.multiCalls).toBe(3);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 14 });
    expect(fake.hGetAllTotal()).toBe(1);
  });

  it('hydrates a cold symbol exactly once no matter how many diffs follow', async () => {
    const fake = await seededFake();
    const store = new RedisBookStore(fake);
    const scripted = [
      diff(11, 12, [{ side: 'bid', price: 100.5, newQty: 3 }]),
      diff(13, 15, [{ side: 'ask', price: 102, newQty: 1 }]),
      diff(9, 10, [{ side: 'bid', price: 50, newQty: 1 }]),
      diff(20, 30, [{ side: 'ask', price: 105, newQty: 1 }]),
      diff(16, 16, [{ side: 'bid', price: 90, newQty: 1 }]),
    ];
    const observed = [];
    for (const single of scripted) observed.push(await store.applyDepthDiff(single));
    expect(observed).toEqual(['applied', 'applied', 'stale', 'gap', 'applied']);
    expect(fake.hGetAllCounts.get('book:BTCUSDT:meta')).toBe(1);
    expect(fake.execCalls).toBe(4);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 16 });
  });

  it('installs the continuity cache from applySnapshot so later ops need no reads', async () => {
    const fake = new FakeRedis();
    const store = new RedisBookStore(fake);
    await store.applySnapshot('BTCUSDT', levels([[100, 1]]), levels([[101, 1]]), 7);
    expect(fake.hGetAllTotal()).toBe(0);
    expect(await store.applyDepthDiff(diff(8, 9, [{ side: 'bid', price: 100.5, newQty: 2 }]))).toBe(
      'applied',
    );
    expect(fake.hGetAllTotal()).toBe(0);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 9 });
    expect(fake.hGetAllTotal()).toBe(0);
    expect(await store.topN('BTCUSDT', 5)).toEqual({
      bids: levels([
        [100.5, 2],
        [100, 1],
      ]),
      asks: levels([[101, 1]]),
    });
  });

  it('honors topN limits and returns empty sides without touching the client for degenerate n', async () => {
    const fake = await seededFake();
    const store = new RedisBookStore(fake);
    expect(await store.topN('BTCUSDT', 1)).toEqual({
      bids: levels([[100.5, 2]]),
      asks: levels([[101, 1]]),
    });
    expect(await store.topN('BTCUSDT', 0)).toEqual({ bids: [], asks: [] });
    expect(fake.execCalls).toBe(1);
  });

  it('stays in lockstep with MemoryBookStore across a scripted mixed sequence', async () => {
    const scripted: DepthDiff[] = [
      diff(11, 11, [
        { side: 'bid', price: 100.5, newQty: 0 },
        { side: 'ask', price: 101.5, newQty: 2 },
      ]),
      diff(12, 14, [{ side: 'ask', price: 101, newQty: 3 }]),
      diff(16, 16, [{ side: 'bid', price: 90, newQty: 1 }]),
      diff(5, 6, [{ side: 'bid', price: 95, newQty: 1 }]),
      diff(15, 18, [
        { side: 'bid', price: 100.25, newQty: 0.25 },
        { side: 'ask', price: 103, newQty: 1 },
      ]),
    ];
    const memory = new MemoryBookStore();
    const redis = new RedisBookStore(new FakeRedis());
    for (const store of [memory, redis]) {
      await store.applySnapshot(
        'BTCUSDT',
        levels([
          [100.5, 2],
          [100.25, 1],
        ]),
        levels([[101, 1]]),
        10,
      );
      const observed = [];
      for (const single of scripted) observed.push(await store.applyDepthDiff(single));
      expect(observed).toEqual(['applied', 'applied', 'gap', 'stale', 'applied']);
      await store.applySnapshot(
        'BTCUSDT',
        levels([
          [100.75, 5],
          [99, 1],
        ]),
        levels([[104, 6]]),
        20,
      );
      expect(
        await store.applyDepthDiff(diff(21, 21, [{ side: 'bid', price: 100.75, newQty: 5.5 }])),
      ).toBe('applied');
    }
    expect(await memory.topN('BTCUSDT', 10)).toEqual(await redis.topN('BTCUSDT', 10));
    expect(await memory.getMeta('BTCUSDT')).toEqual(await redis.getMeta('BTCUSDT'));
  });
});
