import { randomUUID } from 'node:crypto';
import type { LedgerAccount, Transaction } from './types.js';

/**
 * LedgerStore — per-(tenantId,accountId,asset) linearizable append.
 *
 * Invariants:
 * - compound key (tenantId, accountId, asset) + monotonic version
 * - CAS: updateOne({..., version: expectedVersion}, {$set:{balance, version: expectedVersion+1}})
 * - idempotencyKey unique per tenant (24h) — duplicate returns same Transaction, no balance change
 * - fencingToken > lastFencingToken checked before materialize
 */

export type AppendResult =
  | { account: LedgerAccount; transaction: Transaction }
  | { conflict: { currentVersion: number; currentBalance: number } };

export interface LedgerStore {
  getAccount(
    tenantId: string,
    accountId: string,
    asset: string,
  ): Promise<LedgerAccount | undefined>;
  appendTransaction(tx: Transaction): Promise<AppendResult>;
  listTransactions(tenantId: string, accountId: string, asset: string): Promise<Transaction[]>;
  listTransactionsFiltered(
    tenantId: string,
    accountId: string,
    asset: string,
    opts?: { cursor?: number; limit?: number; fromVersion?: number; toVersion?: number },
  ): Promise<{ transactions: Transaction[]; nextCursor?: number }>;
  /** for idempotency pre-check */
  findByIdempotencyKey(tenantId: string, idempotencyKey: string): Promise<Transaction | undefined>;
  /** helper for tests: seed account */
  seedAccount?(account: LedgerAccount): Promise<void>;
}

function txVersion(tx: Transaction): number {
  const v = (tx as unknown as { version?: number }).version;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return tx.prevVersion + 1;
}

function accountKey(tenantId: string, accountId: string, asset: string): string {
  return `${tenantId}:${accountId}:${asset}`;
}
function dedupKey(tenantId: string, idempotencyKey: string): string {
  return `${tenantId}:${idempotencyKey}`;
}

export class MemoryLedgerStore implements LedgerStore {
  private readonly accounts = new Map<string, LedgerAccount>();
  private readonly transactions: Transaction[] = [];
  private readonly byIdempotency = new Map<string, Transaction>();
  private readonly byAccount = new Map<string, Transaction[]>();

  async getAccount(
    tenantId: string,
    accountId: string,
    asset: string,
  ): Promise<LedgerAccount | undefined> {
    const acc = this.accounts.get(accountKey(tenantId, accountId, asset));
    if (!acc) return undefined;
    return { ...acc };
  }

  async findByIdempotencyKey(
    tenantId: string,
    idempotencyKey: string,
  ): Promise<Transaction | undefined> {
    const tx = this.byIdempotency.get(dedupKey(tenantId, idempotencyKey));
    return tx ? { ...tx, createdAt: new Date(tx.createdAt) } : undefined;
  }

  async appendTransaction(tx: Transaction): Promise<AppendResult> {
    const aKey = accountKey(tx.tenantId, tx.accountId, tx.asset);
    const dKey = dedupKey(tx.tenantId, tx.idempotencyKey);

    // 1. idempotency dedupe — return existing, no balance change
    const existing = this.byIdempotency.get(dKey);
    if (existing) {
      const acc = this.accounts.get(aKey);
      // Return existing transaction with current account snapshot
      if (acc) {
        return { account: { ...acc }, transaction: { ...existing } };
      }
      // If account missing but transaction exists (should not happen), return transaction
      return { account: { ...acc! }, transaction: { ...existing } } as AppendResult;
    }

    const current = this.accounts.get(aKey);
    const currentVersion = current?.version ?? 0;
    const currentBalance = current?.balance ?? 0;
    const lastFence = current?.lastFencingToken ?? 0;

    // 2. fencing check — stale holder fenced
    if (tx.fencingToken <= lastFence && current !== undefined) {
      return { conflict: { currentVersion, currentBalance } };
    }

    // 3. version CAS guard
    if (tx.prevVersion !== currentVersion) {
      return { conflict: { currentVersion, currentBalance } };
    }

    // 4. apply balance delta
    let newBalance: number;
    if (tx.type === 'credit') newBalance = currentBalance + tx.amount;
    else if (tx.type === 'debit') newBalance = currentBalance - tx.amount;
    else {
      // transfer: treat as debit for single row; two-row transfer caller creates paired credit row with same transferId
      // If this is the paired side, type still 'transfer' — we need to know direction.
      // Convention: amount positive; for transfer we apply as debit on source, credit on dest.
      // Since store is per-row, we default to debit; service should create second row as credit.
      // For single-row transfer tests, treat as debit.
      newBalance = currentBalance - tx.amount;
    }

    // Optional: overdraft guard — still allow negative? Q1 says strict serializable should prevent overdraft via 409 not via store reject.
    // Store does not enforce balance >=0; business validation is caller-side. We just CAS.

    const now = new Date();
    const updated: LedgerAccount = {
      tenantId: tx.tenantId,
      accountId: tx.accountId,
      asset: tx.asset,
      balance: newBalance,
      version: currentVersion + 1,
      createdAt: current?.createdAt ?? now,
      updatedAt: now,
      lastFencingToken: tx.fencingToken,
    };
    this.accounts.set(aKey, updated);

    const stored: Transaction = {
      ...tx,
      id: tx.id ?? randomUUID(),
      createdAt: tx.createdAt ?? now,
    };
    this.transactions.push(stored);
    this.byIdempotency.set(dKey, stored);
    const list = this.byAccount.get(aKey) ?? [];
    list.push(stored);
    this.byAccount.set(aKey, list);

    return { account: { ...updated }, transaction: { ...stored } };
  }

  async listTransactions(
    tenantId: string,
    accountId: string,
    asset: string,
  ): Promise<Transaction[]> {
    const { transactions } = await this.listTransactionsFiltered(tenantId, accountId, asset);
    return transactions;
  }

  async listTransactionsFiltered(
    tenantId: string,
    accountId: string,
    asset: string,
    opts: { cursor?: number; limit?: number; fromVersion?: number; toVersion?: number } = {},
  ): Promise<{ transactions: Transaction[]; nextCursor?: number }> {
    const aKey = accountKey(tenantId, accountId, asset);
    const list = this.byAccount.get(aKey) ?? [];
    let filtered = [...list].sort((a, b) => txVersion(a) - txVersion(b));
    if (opts.cursor !== undefined) {
      filtered = filtered.filter((t) => txVersion(t) > opts.cursor!);
    }
    if (opts.fromVersion !== undefined) {
      filtered = filtered.filter((t) => txVersion(t) >= opts.fromVersion!);
    }
    if (opts.toVersion !== undefined) {
      filtered = filtered.filter((t) => txVersion(t) <= opts.toVersion!);
    }
    const limit = opts.limit ?? 100;
    const sliced = filtered.slice(0, limit);
    const nextCursor =
      sliced.length === 0
        ? undefined
        : sliced.length === limit
          ? txVersion(sliced[sliced.length - 1]!)
          : undefined;
    return {
      transactions: sliced.map((t) => ({ ...t, createdAt: new Date(t.createdAt) })),
      nextCursor,
    };
  }

  async seedAccount(account: LedgerAccount): Promise<void> {
    this.accounts.set(accountKey(account.tenantId, account.accountId, account.asset), {
      ...account,
    });
  }
}

/**
 * MongoLedgerStore — production implementation sketch.
 * Uses collections `ledger_accounts` + `ledger_transactions` with compound unique indexes:
 * - ledger_accounts: unique {tenantId, accountId, asset}
 * - ledger_transactions: unique {tenantId, idempotencyKey} and index {tenantId, accountId, asset}
 *
 * CAS: updateOne({tenantId, accountId, asset, version: expectedVersion, lastFencingToken: {$lt: fencingToken}},
 *        {$set:{balance:newBalance, version:expectedVersion+1, lastFencingToken, updatedAt}})
 *
 * For hermetic tests without live Mongo, this class delegates to an in-memory map
 * when no `db` is provided (same semantics as MemoryLedgerStore).
 */
export interface MongoDbLike {
  collection(name: string): {
    findOne(filter: Record<string, unknown>): Promise<unknown>;
    updateOne(
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
    ): Promise<{ matchedCount: number }>;
    insertOne(doc: Record<string, unknown>): Promise<unknown>;
    find(filter: Record<string, unknown>): {
      sort?(spec: Record<string, unknown>): {
        limit(n: number): { toArray(): Promise<unknown[]> };
        toArray(): Promise<unknown[]>;
      };
      limit?(n: number): {
        toArray(): Promise<unknown[]>;
        sort?(spec: Record<string, unknown>): unknown;
      };
      toArray(): Promise<unknown[]>;
    };
    createIndex(keys: Record<string, unknown>, opts?: Record<string, unknown>): Promise<void>;
  };
}

export class MongoLedgerStore implements LedgerStore {
  private readonly fallback: MemoryLedgerStore;
  private readonly db?: MongoDbLike;

  constructor(db?: MongoDbLike) {
    this.db = db;
    this.fallback = new MemoryLedgerStore();
  }

  async getAccount(
    tenantId: string,
    accountId: string,
    asset: string,
  ): Promise<LedgerAccount | undefined> {
    if (!this.db) return this.fallback.getAccount(tenantId, accountId, asset);
    const doc = (await this.db
      .collection('ledger_accounts')
      .findOne({ tenantId, accountId, asset })) as LedgerAccount | null;
    return doc
      ? { ...doc, createdAt: new Date(doc.createdAt), updatedAt: new Date(doc.updatedAt) }
      : undefined;
  }

  async findByIdempotencyKey(
    tenantId: string,
    idempotencyKey: string,
  ): Promise<Transaction | undefined> {
    if (!this.db) return this.fallback.findByIdempotencyKey(tenantId, idempotencyKey);
    const doc = (await this.db
      .collection('ledger_transactions')
      .findOne({ tenantId, idempotencyKey })) as Transaction | null;
    return doc ? { ...doc, createdAt: new Date(doc.createdAt) } : undefined;
  }

  async appendTransaction(tx: Transaction): Promise<AppendResult> {
    if (!this.db) return this.fallback.appendTransaction(tx);

    // 1. idempotency check
    const existing = await this.findByIdempotencyKey(tx.tenantId, tx.idempotencyKey);
    if (existing) {
      const acc = await this.getAccount(tx.tenantId, tx.accountId, tx.asset);
      if (acc) return { account: acc, transaction: existing };
    }

    const acc = await this.getAccount(tx.tenantId, tx.accountId, tx.asset);
    const currentVersion = acc?.version ?? 0;
    const currentBalance = acc?.balance ?? 0;

    if (tx.prevVersion !== currentVersion) {
      return { conflict: { currentVersion, currentBalance } };
    }

    const lastFence = acc?.lastFencingToken ?? 0;
    if (tx.fencingToken <= lastFence && acc !== undefined) {
      return { conflict: { currentVersion, currentBalance } };
    }

    let newBalance: number;
    if (tx.type === 'credit') newBalance = currentBalance + tx.amount;
    else newBalance = currentBalance - tx.amount;

    // CAS update
    const filter: Record<string, unknown> = acc
      ? { tenantId: tx.tenantId, accountId: tx.accountId, asset: tx.asset, version: currentVersion }
      : { tenantId: tx.tenantId, accountId: tx.accountId, asset: tx.asset };

    // For new account, insert; for existing, CAS
    if (!acc) {
      try {
        await this.db.collection('ledger_accounts').insertOne({
          tenantId: tx.tenantId,
          accountId: tx.accountId,
          asset: tx.asset,
          balance: newBalance,
          version: 1,
          lastFencingToken: tx.fencingToken,
          createdAt: new Date(),
          updatedAt: new Date(),
        } as unknown as Record<string, unknown>);
      } catch (e: unknown) {
        // Duplicate -> version conflict
        if ((e as { code?: number })?.code === 11000) {
          const cur = await this.getAccount(tx.tenantId, tx.accountId, tx.asset);
          return {
            conflict: { currentVersion: cur?.version ?? 0, currentBalance: cur?.balance ?? 0 },
          };
        }
        throw e;
      }
    } else {
      const res = await this.db
        .collection('ledger_accounts')
        .updateOne(
          { ...filter, lastFencingToken: { $lt: tx.fencingToken } } as Record<string, unknown>,
          {
            $set: {
              balance: newBalance,
              version: currentVersion + 1,
              lastFencingToken: tx.fencingToken,
              updatedAt: new Date(),
            },
          },
        );
      if (res.matchedCount === 0) {
        const cur = await this.getAccount(tx.tenantId, tx.accountId, tx.asset);
        return {
          conflict: { currentVersion: cur?.version ?? 0, currentBalance: cur?.balance ?? 0 },
        };
      }
    }

    // Insert transaction (idempotency unique index guards duplicate)
    try {
      await this.db.collection('ledger_transactions').insertOne({
        ...tx,
        _id: tx.id,
        version: txVersion(tx),
      } as unknown as Record<string, unknown>);
    } catch (e: unknown) {
      if ((e as { code?: number })?.code === 11000) {
        const dup = await this.findByIdempotencyKey(tx.tenantId, tx.idempotencyKey);
        const curAcc = await this.getAccount(tx.tenantId, tx.accountId, tx.asset);
        if (dup && curAcc) return { account: curAcc, transaction: dup };
      }
      throw e;
    }

    const updatedAcc = await this.getAccount(tx.tenantId, tx.accountId, tx.asset);
    return { account: updatedAcc!, transaction: tx };
  }

  async listTransactions(
    tenantId: string,
    accountId: string,
    asset: string,
  ): Promise<Transaction[]> {
    if (!this.db) return this.fallback.listTransactions(tenantId, accountId, asset);
    const docs = (await this.db
      .collection('ledger_transactions')
      .find({ tenantId, accountId, asset })
      .toArray()) as Transaction[];
    return docs.map((d) => ({ ...d, createdAt: new Date((d as Transaction).createdAt) }));
  }

  async listTransactionsFiltered(
    tenantId: string,
    accountId: string,
    asset: string,
    opts: { cursor?: number; limit?: number; fromVersion?: number; toVersion?: number } = {},
  ): Promise<{ transactions: Transaction[]; nextCursor?: number }> {
    if (!this.db) return this.fallback.listTransactionsFiltered(tenantId, accountId, asset, opts);
    const limit = opts.limit ?? 100;
    const versionFilter: Record<string, unknown> = {};
    if (opts.cursor !== undefined) versionFilter['$gt'] = opts.cursor;
    if (opts.fromVersion !== undefined) versionFilter['$gte'] = opts.fromVersion;
    if (opts.toVersion !== undefined) versionFilter['$lte'] = opts.toVersion;
    const filter: Record<string, unknown> = { tenantId, accountId, asset };
    if (Object.keys(versionFilter).length > 0) filter['version'] = versionFilter;
    const findRes = this.db.collection('ledger_transactions').find(filter) as unknown as {
      sort(spec: Record<string, unknown>): { limit(n: number): { toArray(): Promise<unknown[]> } };
      toArray(): Promise<unknown[]>;
      limit(n: number): { toArray(): Promise<unknown[]> };
    };
    let docs: Transaction[];
    // Prefer sort+limit chain when available; fallback to manual when not
    if (typeof findRes.sort === 'function') {
      const sorted = findRes.sort({ version: 1 }) as unknown as {
        limit(n: number): { toArray(): Promise<unknown[]> };
        toArray(): Promise<unknown[]>;
      };
      const limited = typeof sorted.limit === 'function' ? sorted.limit(limit + 1) : sorted;
      docs = (await limited.toArray()) as Transaction[];
    } else if (typeof findRes.limit === 'function') {
      docs = (await findRes.limit(limit + 1).toArray()) as Transaction[];
      docs = docs.sort((a, b) => txVersion(a) - txVersion(b));
    } else {
      docs = (await findRes.toArray()) as Transaction[];
      docs = docs
        .filter((d) => {
          const v = txVersion(d);
          if (opts.cursor !== undefined && v <= opts.cursor) return false;
          if (opts.fromVersion !== undefined && v < opts.fromVersion) return false;
          if (opts.toVersion !== undefined && v > opts.toVersion) return false;
          return true;
        })
        .sort((a, b) => txVersion(a) - txVersion(b))
        .slice(0, limit + 1);
    }
    const hasMore = docs.length > limit;
    const sliced = hasMore ? docs.slice(0, limit) : docs;
    const nextCursor =
      sliced.length === 0
        ? undefined
        : sliced.length === limit
          ? txVersion(sliced[sliced.length - 1]!)
          : undefined;
    const finalDocs = sliced.map((d) => ({
      ...d,
      createdAt: new Date((d as Transaction).createdAt),
    }));
    return { transactions: finalDocs, nextCursor };
  }

  async seedAccount(account: LedgerAccount): Promise<void> {
    if (!this.db) return this.fallback.seedAccount(account);
    await this.db
      .collection('ledger_accounts')
      .insertOne(account as unknown as Record<string, unknown>);
  }
}
