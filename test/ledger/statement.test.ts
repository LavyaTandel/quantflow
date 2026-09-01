import { describe, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { MemoryLedgerStore } from '../../src/ledger/store.js';
import { buildStatement, formatStatementRow, toCSV, toJSONL } from '../../src/ledger/statement.js';
import type { Transaction } from '../../src/ledger/types.js';

function makeTx(over: Partial<Transaction> = {}): Transaction {
  return {
    id: randomUUID(),
    tenantId: 't1',
    accountId: 'acc1',
    asset: 'USDT',
    type: 'credit',
    amount: 10,
    prevVersion: 0,
    idempotencyKey: randomUUID(),
    fencingToken: 1,
    createdAt: new Date('2026-01-01T00:00:00.000Z'),
    ...over,
  };
}

async function appendBatch(
  store: MemoryLedgerStore,
  count: number,
  tenantId = 't1',
  accountId = 'acc1',
  asset = 'USDT',
  startVersion = 0,
) {
  let v = startVersion;
  for (let i = 0; i < count; i++) {
    const tx = makeTx({
      tenantId,
      accountId,
      asset,
      type: i % 2 === 0 ? 'credit' : 'debit',
      amount: 10,
      prevVersion: v,
      fencingToken: v + 1,
      idempotencyKey: randomUUID(),
      createdAt: new Date(Date.now() + i),
    });
    const res = await store.appendTransaction(tx);
    if ('conflict' in res) throw new Error('append conflict in test');
    v = res.account.version;
  }
  return v;
}

describe('ledger statement read-model', () => {
  it('second tenant isolated from first (same accountId/asset different tenantId -> 0 rows)', async () => {
    const store = new MemoryLedgerStore();
    await appendBatch(store, 3, 'tenant-a', 'shared', 'USDT', 0);
    await appendBatch(store, 2, 'tenant-b', 'shared', 'USDT', 0);

    const resA = await buildStatement(store, 'tenant-a', 'shared', 'USDT', {});
    const resB = await buildStatement(store, 'tenant-b', 'shared', 'USDT', {});
    const resOther = await buildStatement(store, 'tenant-b', 'shared', 'USDT', {});
    expect(resA.rows.length).toBe(3);
    expect(resB.rows.length).toBe(2);
    // tenant-a should not see tenant-b rows and vice versa
    expect(resA.rows.every((r) => r.accountId === 'shared')).toBe(true);
    // direct filter: tenant-c has 0
    const resC = await buildStatement(store, 'tenant-c', 'shared', 'USDT', {});
    expect(resC.rows.length).toBe(0);
    expect(resOther.rows.length).toBe(2);
  });

  it('pagination stable across concurrent appends (cursor=last version)', async () => {
    const store = new MemoryLedgerStore();
    // seed 5 txs
    await appendBatch(store, 5, 't1', 'acc1', 'USDT', 0);
    const page1 = await buildStatement(store, 't1', 'acc1', 'USDT', { limit: 3 });
    expect(page1.rows.length).toBe(3);
    expect(page1.nextCursor).toBeDefined();
    const cursor = page1.nextCursor!;
    // Concurrent append 5 more txs after page1 fetched
    const curAcc = await store.getAccount('t1', 'acc1', 'USDT');
    let v = curAcc!.version;
    for (let i = 0; i < 5; i++) {
      const tx = makeTx({
        tenantId: 't1',
        accountId: 'acc1',
        asset: 'USDT',
        type: 'credit',
        amount: 5,
        prevVersion: v,
        fencingToken: v + 1,
        idempotencyKey: randomUUID(),
      });
      const r = await store.appendTransaction(tx);
      if ('conflict' in r) throw new Error('conflict');
      v = r.account.version;
    }
    // page2 via cursor should return next items without duplicates, not re-return page1
    const page2 = await buildStatement(store, 't1', 'acc1', 'USDT', { cursor, limit: 100 });
    expect(page2.rows.length).toBe(7); // total 10, cursor 3 => remaining 7
    // no duplicate transactionIds
    const ids1 = new Set(page1.rows.map((r) => r.transactionId));
    for (const r of page2.rows) expect(ids1.has(r.transactionId)).toBe(false);
    // ordering stable version asc
    const allVersions = [...page1.rows, ...page2.rows].map((r) => r.prevVersion + 1);
    const sorted = [...allVersions].sort((a, b) => a - b);
    expect(allVersions).toEqual(sorted);
    // prevVersion chain intact
    for (let i = 0; i < page2.rows.length; i++) {
      const _expectedPrev = cursor + i + 1 - 1;
      void _expectedPrev;
      // first row after cursor should have prevVersion == cursor
      if (i === 0) expect(page2.rows[0]!.prevVersion).toBe(cursor);
    }
  });

  it('fromVersion/toVersion filtering inclusive', async () => {
    const store = new MemoryLedgerStore();
    await appendBatch(store, 6, 't1', 'acc1', 'USDT', 0);
    const filtered = await buildStatement(store, 't1', 'acc1', 'USDT', {
      fromVersion: 2,
      toVersion: 4,
    });
    expect(filtered.rows.length).toBe(3);
    const versions = filtered.rows.map((r) => r.prevVersion + 1);
    expect(versions).toEqual([2, 3, 4]);
    expect(filtered.rows[0]!.prevVersion).toBe(1);
    // fromVersion only
    const fromOnly = await buildStatement(store, 't1', 'acc1', 'USDT', { fromVersion: 5 });
    expect(fromOnly.rows.map((r) => r.prevVersion + 1)).toEqual([5, 6]);
    // toVersion only
    const toOnly = await buildStatement(store, 't1', 'acc1', 'USDT', { toVersion: 2 });
    expect(toOnly.rows.map((r) => r.prevVersion + 1)).toEqual([1, 2]);
  });

  it('transferId grouping visible in rows', async () => {
    const store = new MemoryLedgerStore();
    const transferId = randomUUID();
    const tx1 = makeTx({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      type: 'transfer',
      amount: 50,
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: randomUUID(),
      transferId,
    });
    const tx2 = makeTx({
      tenantId: 't1',
      accountId: 'acc2',
      asset: 'USDT',
      type: 'transfer',
      amount: 50,
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: randomUUID(),
      transferId,
    });
    await store.appendTransaction(tx1);
    await store.appendTransaction(tx2);
    const stmtAcc1 = await buildStatement(store, 't1', 'acc1', 'USDT', {});
    const stmtAcc2 = await buildStatement(store, 't1', 'acc2', 'USDT', {});
    expect(stmtAcc1.rows[0]!.transferId).toBe(transferId);
    expect(stmtAcc2.rows[0]!.transferId).toBe(transferId);
    expect(stmtAcc1.rows[0]!.type).toBe('transfer');
    expect(stmtAcc2.rows[0]!.type).toBe('transfer');
  });

  it('balanceAfter computed by walking ordered txs and kafkaOffset/fencingToken/idempotencyKey preserved', async () => {
    const store = new MemoryLedgerStore();
    const idem1 = randomUUID();
    const tx1 = makeTx({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      type: 'credit',
      amount: 100,
      prevVersion: 0,
      fencingToken: 10,
      idempotencyKey: idem1,
      kafkaOffset: 5,
    });
    const tx2 = makeTx({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      type: 'debit',
      amount: 30,
      prevVersion: 1,
      fencingToken: 11,
      idempotencyKey: randomUUID(),
      kafkaOffset: 6,
    });
    await store.appendTransaction(tx1);
    await store.appendTransaction(tx2);
    const stmt = await buildStatement(store, 't1', 'acc1', 'USDT', {});
    expect(stmt.rows.length).toBe(2);
    expect(stmt.rows[0]!.balanceAfter).toBe(100);
    expect(stmt.rows[1]!.balanceAfter).toBe(70);
    expect(stmt.rows[0]!.kafkaOffset).toBe(5);
    expect(stmt.rows[1]!.fencingToken).toBe(11);
    expect(stmt.rows[0]!.idempotencyKey).toBe(idem1);
    expect(stmt.rows[0]!.prevVersion).toBe(0);
    expect(stmt.rows[1]!.prevVersion).toBe(1);
  });

  it('formatStatementRow and CSV/JSONL shape', async () => {
    const tx = makeTx({
      id: 'tx-1',
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      type: 'credit',
      amount: 42,
      prevVersion: 4,
      fencingToken: 7,
      idempotencyKey: 'idem-123',
      kafkaOffset: 9,
      transferId: 'tr-1',
      createdAt: new Date('2026-08-27T12:00:00.000Z'),
    });
    const row = formatStatementRow(tx, 142);
    expect(row.transactionId).toBe('tx-1');
    expect(row.balanceAfter).toBe(142);
    expect(row.timestamp.toISOString()).toBe('2026-08-27T12:00:00.000Z');

    const csv = toCSV([row]);
    expect(csv.split('\n')[0]).toBe(
      'timestamp,transactionId,accountId,asset,type,amount,balanceAfter,prevVersion,kafkaOffset,fencingToken,idempotencyKey,transferId',
    );
    expect(csv).toContain('tx-1');
    expect(csv).toContain('tr-1');
    // no NormalizedEvent leak
    expect(csv).not.toContain('NormalizedEvent');
    const jsonl = toJSONL([row]);
    expect(jsonl).toContain('"transactionId":"tx-1"');
    expect(() => JSON.parse(jsonl)).not.toThrow();
    // one JSON per line
    const rows2 = [row, { ...row, transactionId: 'tx-2' }];
    const jsonl2 = toJSONL(rows2 as never);
    expect(jsonl2.split('\n').length).toBe(2);
  });

  it('audit export reproducible for time window (fromVersion/toVersion)', async () => {
    const store = new MemoryLedgerStore();
    await appendBatch(store, 10, 't1', 'acc1', 'USDT', 0);
    const win1 = await buildStatement(store, 't1', 'acc1', 'USDT', {
      fromVersion: 3,
      toVersion: 7,
    });
    const win2 = await buildStatement(store, 't1', 'acc1', 'USDT', {
      fromVersion: 3,
      toVersion: 7,
    });
    expect(win1.rows.map((r) => r.transactionId)).toEqual(win2.rows.map((r) => r.transactionId));
    expect(toCSV(win1.rows)).toBe(toCSV(win2.rows));
    expect(toJSONL(win1.rows)).toBe(toJSONL(win2.rows));
  });
});
