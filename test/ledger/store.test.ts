import { describe, it, expect } from 'vitest';
import { MemoryLedgerStore } from '../../src/ledger/store.js';
import type { Transaction } from '../../src/ledger/types.js';
import { randomUUID } from 'node:crypto';

function makeTx(over: Partial<Transaction> = {}): Transaction {
  return {
    id: randomUUID(),
    tenantId: 't1',
    accountId: 'acc1',
    asset: 'USDT',
    type: 'credit',
    amount: 100,
    prevVersion: 0,
    idempotencyKey: randomUUID(),
    fencingToken: 1,
    createdAt: new Date(),
    ...over,
  };
}

describe('MemoryLedgerStore CAS + idempotency + tenant isolation', () => {
  it('version CAS conflict — double debit 60 against 100 version 5 yields 409', async () => {
    const store = new MemoryLedgerStore();
    // seed account balance 100 version 5
    await store.seedAccount!({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      balance: 100,
      version: 5,
      createdAt: new Date(),
      updatedAt: new Date(),
      lastFencingToken: 5,
    });

    const tx1 = makeTx({
      type: 'debit',
      amount: 60,
      prevVersion: 5,
      fencingToken: 6,
      idempotencyKey: 'idem-1',
    });
    const tx2 = makeTx({
      type: 'debit',
      amount: 60,
      prevVersion: 5,
      fencingToken: 7,
      idempotencyKey: 'idem-2',
    });

    const r1 = await store.appendTransaction(tx1);
    expect('account' in r1).toBe(true);
    if ('account' in r1) {
      expect(r1.account.balance).toBe(40);
      expect(r1.account.version).toBe(6);
    }

    const r2 = await store.appendTransaction(tx2);
    expect('conflict' in r2).toBe(true);
    if ('conflict' in r2) {
      expect(r2.conflict.currentVersion).toBe(6);
      expect(r2.conflict.currentBalance).toBe(40);
    }

    // Ensure only one transaction persisted
    const list = await store.listTransactions('t1', 'acc1', 'USDT');
    expect(list.length).toBe(1);
  });

  it('idempotencyKey dedupe returns same Transaction, no second balance change', async () => {
    const store = new MemoryLedgerStore();
    const idem = 'idem-same';
    const tx1 = makeTx({
      type: 'credit',
      amount: 50,
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: idem,
    });
    const r1 = await store.appendTransaction(tx1);
    expect('account' in r1).toBe(true);
    const bal1 = (r1 as { account: { balance: number } }).account.balance;

    // second append with same key but different amount — should dedupe and ignore second delta
    const tx2 = makeTx({
      type: 'credit',
      amount: 999,
      prevVersion: 999, // would be conflict if not deduped
      fencingToken: 2,
      idempotencyKey: idem,
    });
    const r2 = await store.appendTransaction(tx2);
    expect('account' in r2).toBe(true);
    if ('account' in r2 && 'transaction' in r2) {
      expect(r2.transaction.id).toBe((r1 as { transaction: Transaction }).transaction.id);
      expect(r2.account.balance).toBe(bal1);
    }
    const list = await store.listTransactions('t1', 'acc1', 'USDT');
    expect(list.length).toBe(1);
    expect(list[0]!.amount).toBe(50);
  });

  it('tenant isolation — same accountId/asset in different tenants isolated', async () => {
    const store = new MemoryLedgerStore();
    const txA = makeTx({
      tenantId: 't-a',
      accountId: 'shared',
      asset: 'USDT',
      amount: 100,
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: 'idem-a',
    });
    const txB = makeTx({
      tenantId: 't-b',
      accountId: 'shared',
      asset: 'USDT',
      amount: 200,
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: 'idem-b',
    });
    await store.appendTransaction(txA);
    await store.appendTransaction(txB);

    const accA = await store.getAccount('t-a', 'shared', 'USDT');
    const accB = await store.getAccount('t-b', 'shared', 'USDT');
    expect(accA?.balance).toBe(100);
    expect(accB?.balance).toBe(200);

    const listA = await store.listTransactions('t-a', 'shared', 'USDT');
    const listB = await store.listTransactions('t-b', 'shared', 'USDT');
    expect(listA.length).toBe(1);
    expect(listB.length).toBe(1);
    // dedupe key per tenant: same idempotencyKey reused across tenants should not collide
    const txA2 = makeTx({
      tenantId: 't-a',
      accountId: 'shared',
      asset: 'USDT',
      amount: 10,
      prevVersion: 1,
      fencingToken: 2,
      idempotencyKey: 'same-key',
    });
    const txB2 = makeTx({
      tenantId: 't-b',
      accountId: 'shared',
      asset: 'USDT',
      amount: 10,
      prevVersion: 1,
      fencingToken: 2,
      idempotencyKey: 'same-key',
    });
    const rA2 = await store.appendTransaction(txA2);
    const rB2 = await store.appendTransaction(txB2);
    expect('account' in rA2).toBe(true);
    expect('account' in rB2).toBe(true);
  });

  it('fencing token blocks stale holder', async () => {
    const store = new MemoryLedgerStore();
    await store.seedAccount!({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      balance: 100,
      version: 1,
      createdAt: new Date(),
      updatedAt: new Date(),
      lastFencingToken: 2,
    });
    // stale token 1 <= last 2 -> conflict
    const stale = makeTx({
      type: 'debit',
      amount: 10,
      prevVersion: 1,
      fencingToken: 1,
      idempotencyKey: 'stale-1',
    });
    const rStale = await store.appendTransaction(stale);
    expect('conflict' in rStale).toBe(true);

    const fresh = makeTx({
      type: 'debit',
      amount: 10,
      prevVersion: 1,
      fencingToken: 3,
      idempotencyKey: 'fresh-1',
    });
    const rFresh = await store.appendTransaction(fresh);
    expect('account' in rFresh).toBe(true);
  });
});
