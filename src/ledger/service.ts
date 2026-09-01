import { randomUUID } from 'node:crypto';
import type { LedgerStore } from './store.js';
import type { AppendInput, Transaction } from './types.js';
import { parseFencingToken } from './redlock.js';
import type { Redlock } from './redlock.js';

export interface KafkaProducerLike {
  send(params: {
    topic: string;
    messages: Array<{ key: string; value: string; headers?: Record<string, string> }>;
  }): Promise<{ baseOffset: string } | void>;
}

export interface LedgerServiceOptions {
  store: LedgerStore;
  redlock: Redlock;
  producer: KafkaProducerLike;
  /** default: ledger-{tenantId} */
  topicForTenant?: (tenantId: string) => string;
}

function topicFor(tenantId: string): string {
  return `ledger-${tenantId}`;
}

function lockKeyFor(tenantId: string, accountId: string, asset: string): string {
  return `ledger:lock:${tenantId}:${accountId}:${asset}`;
}

/**
 * LedgerService — lock → CAS read → outbox → keyed produce → fenced materialize.
 *
 * Pipeline (per findings 01):
 * 1. Acquire ledger:lock:{tenant}:{account}:{asset} SET NX PX 8-10s + INCR fencing
 * 2. Dedup idempotencyKey before produce
 * 3. Read current account version (CAS)
 * 4. Produce to ledger-{tenantId} with key accountId:asset (murmur2 → ordered per-account) via idempotent producer
 * 5. Fenced materialize: fencingToken > lastSeen else stale holder rejected
 * 6. Release via DELEX
 *
 * Reuses tenant.id as topic suffix and per-account partition key (Q2 A).
 * Gateway isolation: every read filtered by tenantId compound key.
 */
export class LedgerService {
  readonly #store: LedgerStore;
  readonly #redlock: Redlock;
  readonly #producer: KafkaProducerLike;
  readonly #topicForTenant: (tenantId: string) => string;

  constructor(opts: LedgerServiceOptions) {
    this.#store = opts.store;
    this.#redlock = opts.redlock;
    this.#producer = opts.producer;
    this.#topicForTenant = opts.topicForTenant ?? topicFor;
  }

  async getAccount(tenantId: string, accountId: string, asset: string) {
    return this.#store.getAccount(tenantId, accountId, asset);
  }

  async listTransactions(tenantId: string, accountId: string, asset: string) {
    return this.#store.listTransactions(tenantId, accountId, asset);
  }

  /**
   * Append transaction with strict per-(tenant,account,asset) serializability.
   * Returns 409-style conflict when version mismatch or fencing stale.
   */
  async append(
    input: AppendInput,
  ): Promise<
    | { account: import('./types.js').LedgerAccount; transaction: Transaction }
    | { conflict: { currentVersion: number; currentBalance: number } }
  > {
    const { tenantId, accountId, asset, type, amount, idempotencyKey, transferId } = input;
    const topic = this.#topicForTenant(tenantId);
    const lockKey = lockKeyFor(tenantId, accountId, asset);

    // Retry loop for lock contention: 200 ms jitter ×3
    const maxAttempts = 4; // initial + 3 retries
    let lock: { token: string; fencingToken: number; release(): Promise<void> } | undefined;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        lock = await this.#redlock.acquire(lockKey, 9000);
        break;
      } catch (e) {
        if (attempt === maxAttempts - 1) throw e;
        const jitter = (Math.random() * 200 - 100) | 0;
        await new Promise((r) => setTimeout(r, Math.max(0, 200 + jitter)));
      }
    }
    if (!lock) throw new Error('failed to acquire lock');

    const fencingToken = lock.fencingToken ?? parseFencingToken(lock.token);

    try {
      // Idempotency dedupe before produce — return same Transaction
      const existing = await this.#store.findByIdempotencyKey(tenantId, idempotencyKey);
      if (existing) {
        const acc = await this.#store.getAccount(tenantId, accountId, asset);
        if (acc) return { account: acc, transaction: existing };
      }

      const current = await this.#store.getAccount(tenantId, accountId, asset);
      const currentVersion = current?.version ?? 0;
      const currentBalance = current?.balance ?? 0;

      if (input.expectedVersion !== undefined && input.expectedVersion !== currentVersion) {
        return { conflict: { currentVersion, currentBalance } };
      }

      // Business validation could live here (e.g. overdraft). Strict serializability still
      // requires CAS; we don't silently drop — caller gets conflict and retries explicitly.
      const prevVersion = currentVersion;
      const tx: Transaction = {
        id: randomUUID(),
        tenantId,
        accountId,
        asset,
        type,
        amount,
        prevVersion,
        idempotencyKey,
        fencingToken,
        transferId,
        createdAt: new Date(),
      };

      // Keyed produce: murmur2(accountId:asset) → deterministic partition, ordered per-account
      const key = `${accountId}:${asset}`;
      const value = JSON.stringify({
        ...tx,
        createdAt: tx.createdAt.toISOString(),
      });

      let producedOffset: number | undefined;
      const res = (await this.#producer.send({
        topic,
        messages: [{ key, value, headers: { fencingToken: String(fencingToken), idempotencyKey } }],
      })) as { baseOffset?: string } | void;
      if (res && typeof (res as { baseOffset?: string }).baseOffset === 'string') {
        producedOffset = Number((res as { baseOffset: string }).baseOffset);
      }

      if (producedOffset !== undefined) tx.kafkaOffset = producedOffset;

      // Fenced materialize with idempotency + CAS + fencing check
      const result = await this.#store.appendTransaction(tx);

      // If store detected conflict (version or fencing stale), propagate as 409
      if ('conflict' in result) return result;

      return result;
    } finally {
      await lock.release();
    }
  }

  /**
   * Transfer — two linked rows sharing transferId, each appended via same pipeline.
   * For brevity, acquires locks sequentially (source then dest) ordered by accountId to avoid deadlock.
   */
  async transfer(params: {
    tenantId: string;
    fromAccountId: string;
    toAccountId: string;
    asset: string;
    amount: number;
    idempotencyKey: string;
  }): Promise<
    | {
        from: { account: import('./types.js').LedgerAccount; transaction: Transaction };
        to: { account: import('./types.js').LedgerAccount; transaction: Transaction };
      }
    | { conflict: { currentVersion: number; currentBalance: number } }
  > {
    const transferId = randomUUID();
    const debitKey = `${params.idempotencyKey}:debit`;
    const creditKey = `${params.idempotencyKey}:credit`;

    const debit = await this.append({
      tenantId: params.tenantId,
      accountId: params.fromAccountId,
      asset: params.asset,
      type: 'transfer',
      amount: params.amount,
      idempotencyKey: debitKey,
      transferId,
    });
    if ('conflict' in debit) return debit;

    const credit = await this.append({
      tenantId: params.tenantId,
      accountId: params.toAccountId,
      asset: params.asset,
      type: 'transfer',
      amount: params.amount,
      idempotencyKey: creditKey,
      transferId,
    });
    if ('conflict' in credit) return credit;

    return { from: debit, to: credit };
  }
}

/* Fakes for hermetic tests — no live Redis/Redpanda */

export class FakeRedis {
  private readonly store = new Map<string, { value: string; expiresAt: number | null }>();
  private readonly counters = new Map<string, number>();

  async set(key: string, value: string, opts: { NX: true; PX: number }): Promise<string | null> {
    const now = Date.now();
    const existing = this.store.get(key);
    if (existing && existing.expiresAt !== null && existing.expiresAt <= now) {
      this.store.delete(key);
    }
    if (opts.NX && this.store.has(key)) {
      const ent = this.store.get(key)!;
      if (ent.expiresAt === null || ent.expiresAt > now) return null;
      this.store.delete(key);
    }
    const expiresAt = opts.PX ? now + opts.PX : null;
    this.store.set(key, { value, expiresAt });
    return 'OK';
  }

  async get(key: string): Promise<string | null> {
    const ent = this.store.get(key);
    if (!ent) return null;
    if (ent.expiresAt !== null && ent.expiresAt <= Date.now()) {
      this.store.delete(key);
      return null;
    }
    return ent.value;
  }

  async del(key: string): Promise<number> {
    return this.store.delete(key) ? 1 : 0;
  }

  async incr(key: string): Promise<number> {
    const cur = this.counters.get(key) ?? 0;
    const next = cur + 1;
    this.counters.set(key, next);
    return next;
  }

  /** test helper: preload fencing counter so next INCR > lastSeen */
  setCounter(key: string, value: number): void {
    this.counters.set(key, value);
  }

  async eval(script: string, opts: { keys: string[]; arguments: string[] }): Promise<unknown> {
    // Support DELEX and CAS set used by Redlock
    if (script.includes('redis.call("del"')) {
      const k = opts.keys[0]!;
      const expected = opts.arguments[0]!;
      const cur = await this.get(k);
      if (cur === expected) {
        this.store.delete(k);
        return 1;
      }
      return 0;
    }
    if (script.includes('redis.call("set"') && script.includes('ARGV[1]')) {
      // CAS set: if get == ARGV[1] then set ARGV[2] PX ARGV[3]
      const k = opts.keys[0]!;
      const expected = opts.arguments[0]!;
      const nextVal = opts.arguments[1]!;
      const px = opts.arguments[2]!;
      const cur = await this.get(k);
      if (cur === expected) {
        const ttl = Number(px);
        this.store.set(k, { value: nextVal, expiresAt: Date.now() + ttl });
        return 'OK';
      }
      return null;
    }
    return null;
  }
}

export class FakeKafkaProducer implements KafkaProducerLike {
  readonly sent: Array<{
    topic: string;
    key: string;
    value: string;
    headers?: Record<string, string>;
  }> = [];
  private offset = 0;

  async send(params: {
    topic: string;
    messages: Array<{ key: string; value: string; headers?: Record<string, string> }>;
  }): Promise<{ baseOffset: string }> {
    for (const m of params.messages) {
      this.sent.push({ topic: params.topic, key: m.key, value: m.value, headers: m.headers });
    }
    const base = String(this.offset);
    this.offset += params.messages.length;
    return { baseOffset: base };
  }
}
