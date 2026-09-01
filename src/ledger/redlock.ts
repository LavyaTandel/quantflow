import { randomUUID } from 'node:crypto';

/**
 * Single-instance Redlock with fencing token.
 *
 * Correctness notes (per redis.io + findings/01-redlock-log.md):
 * - TTL 8–10 s (default 9000 ms) — covers p99 Redis <5 ms + Mongo <20 ms + Kafka <100 ms with 60–80× margin.
 * - No auto-extension for the critical debit path; bounded section must finish within TTL.
 * - Retry on lock miss: 200 ms + jitter ±100 ms × 3 (caller-driven).
 * - Release via Lua DELEX `if redis.call("get",KEYS[1])==ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`
 *   ownership-checked so a successor's lock is not deleted.
 * - Token = randomUUID() + ":" + fencingToken where fencingToken = INCR ledger:fence:{tenant}:{account}:{asset}
 * - Redpanda topic ledger-{tenantId} keyed by accountId:asset (murmur2) — not used here.
 */

export const DEFAULT_TTL_MS = 9000;
export const DELEX_LUA =
  'if redis.call("get",KEYS[1])==ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end';

export interface RedisLike {
  set(key: string, value: string, opts: { NX: true; PX: number }): Promise<string | null>;
  get(key: string): Promise<string | null>;
  del(key: string): Promise<number>;
  incr(key: string): Promise<number>;
  eval(script: string, opts: { keys: string[]; arguments: string[] }): Promise<unknown>;
}

export interface LockHandle {
  token: string;
  fencingToken: number;
  release(): Promise<void>;
}

function fencingKeyFor(lockKey: string): string {
  // ledger:lock:{tenant}:{account}:{asset} -> ledger:fence:{tenant}:{account}:{asset}
  if (lockKey.startsWith('ledger:lock:')) {
    return lockKey.replace('ledger:lock:', 'ledger:fence:');
  }
  return `fence:${lockKey}`;
}

function parseFencingToken(token: string): number {
  const idx = token.lastIndexOf(':');
  if (idx === -1) return 0;
  const n = Number(token.slice(idx + 1));
  return Number.isFinite(n) ? n : 0;
}

export class Redlock {
  readonly #redis: RedisLike;

  constructor(redis: RedisLike) {
    this.#redis = redis;
  }

  /**
   * Acquire distributed lock.
   * @param key - e.g. `ledger:lock:${tenantId}:${accountId}:${asset}`
   * @param ttlMs - 8000–10000 ms recommended
   * @returns handle with ownership-checked release
   * @throws if lock not acquired
   */
  async acquire(key: string, ttlMs = DEFAULT_TTL_MS): Promise<LockHandle> {
    const fKey = fencingKeyFor(key);
    const randomPart = randomUUID();

    // Try to acquire lock with randomPart; avoids wasting fencing tokens on contention.
    const setResult = await this.#redis.set(key, randomPart, { NX: true, PX: ttlMs });
    if (setResult !== 'OK' && setResult !== null) {
      // node-redis returns 'OK' on success, null on NX failure
      // Some fakes return 'OK'/'null' — treat anything != OK as failure
      if (setResult === null || setResult === undefined) {
        throw new Error(`lock not acquired: ${key}`);
      }
    }
    if (setResult === null) {
      throw new Error(`lock not acquired: ${key}`);
    }

    // Only holder increments fencing token
    const fencingToken = await this.#redis.incr(fKey);
    const token = `${randomPart}:${fencingToken}`;

    // CAS update lock value to full token (ownership check)
    // If lock expired or was overwritten, this will fail and we clean up fencing? Keep token anyway but release will no-op.
    const updated = await this.#redis.eval(
      'if redis.call("get",KEYS[1])==ARGV[1] then return redis.call("set",KEYS[1],ARGV[2],"PX",ARGV[3]) else return nil end',
      { keys: [key], arguments: [randomPart, token, String(ttlMs)] },
    );
    // If updated is null/nil, lock was lost before fencing update — treat as failure
    if (updated === null || updated === undefined) {
      // Best effort: try to not leave stale randomPart lock if we lost it (already lost)
      throw new Error(`lock not acquired (fencing race): ${key}`);
    }

    let released = false;
    const release = async (): Promise<void> => {
      if (released) return;
      released = true;
      await this.#redis.eval(DELEX_LUA, { keys: [key], arguments: [token] });
    };

    return { token, fencingToken, release };
  }

  /** Acquire with retry: 200 ms + jitter ×3 — caller-driven per ADR */
  async acquireWithRetry(
    key: string,
    ttlMs = DEFAULT_TTL_MS,
    retries = 3,
    baseDelayMs = 200,
  ): Promise<LockHandle> {
    let lastErr: unknown;
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        return await this.acquire(key, ttlMs);
      } catch (e) {
        lastErr = e;
        if (attempt === retries) break;
        const jitter = (Math.random() * 200 - 100) | 0; // ±100 ms
        const delay = baseDelayMs + jitter;
        await new Promise((r) => setTimeout(r, Math.max(0, delay)));
      }
    }
    throw lastErr;
  }
}

export function createRedlock(redisClient: RedisLike): Redlock {
  return new Redlock(redisClient);
}

// Re-export helper for service to extract fencing token
export { parseFencingToken, fencingKeyFor };
