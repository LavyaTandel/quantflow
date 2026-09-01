import { describe, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { MemoryLedgerStore } from '../../src/ledger/store.js';
import { createLedgerGateway } from '../../src/ledger/gateway.js';
import { createMemoryRateLimiter } from '../../src/gateway/rateLimiter.js';
import type { Transaction } from '../../src/ledger/types.js';

class MemoryTenantStore {
  readonly #byKey = new Map<string, { id: string; tier: string; apiKey: string }>();
  constructor(tenants: Array<{ id: string; tier: string; apiKey: string }>) {
    for (const t of tenants) this.#byKey.set(t.apiKey, t);
  }
  async findByApiKey(apiKey: string) {
    return this.#byKey.get(apiKey);
  }
}

type MockRes = {
  statusCode: number | undefined;
  headers: Record<string, string>;
  body: string;
  writeHead(status: number, headers: Record<string, string>): void;
  end(data: string): void;
};

function mockRes(): MockRes {
  const r: MockRes = {
    statusCode: undefined,
    headers: {},
    body: '',
    writeHead(status, headers) {
      r.statusCode = status;
      for (const [k, v] of Object.entries(headers)) r.headers[k.toLowerCase()] = String(v);
    },
    end(data) {
      r.body = data ?? '';
    },
  };
  return r;
}

function mockReq(opts: {
  method?: string;
  url?: string;
  headers?: Record<string, string>;
}): Record<string, unknown> {
  return {
    method: opts.method ?? 'GET',
    url: opts.url ?? '/',
    headers: Object.fromEntries(
      Object.entries(opts.headers ?? {}).map(([k, v]) => [k.toLowerCase(), v]),
    ),
  };
}

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

describe('createLedgerGateway handleStatementRequest', () => {
  it('401 missing/invalid key', async () => {
    const ledgerStore = new MemoryLedgerStore();
    const tenantStore = new MemoryTenantStore([
      { id: 't1', tier: 'starter', apiKey: 'k1' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });

    const resMissing = mockRes();
    await gw.handleStatementRequest(
      mockReq({ url: '/api/ledger/statement?accountId=acc1&asset=USDT' }) as never,
      resMissing as never,
    );
    expect(resMissing.statusCode).toBe(401);

    const resBad = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT',
        headers: { 'x-api-key': 'bad' },
      }) as never,
      resBad as never,
    );
    expect(resBad.statusCode).toBe(401);

    const resBearerBad = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT',
        headers: { authorization: 'Bearer bad' },
      }) as never,
      resBearerBad as never,
    );
    expect(resBearerBad.statusCode).toBe(401);
  });

  it('403 tenant mismatch when tenantId param mismatches authenticated tenant', async () => {
    const ledgerStore = new MemoryLedgerStore();
    const tenantStore = new MemoryTenantStore([
      { id: 'tenant-a', tier: 'starter', apiKey: 'key-a' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });
    const res = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT&tenantId=tenant-b',
        headers: { 'x-api-key': 'key-a' },
      }) as never,
      res as never,
    );
    expect(res.statusCode).toBe(403);
  });

  it('200 with correct rows + X-Next-Cursor and per-tenant isolation', async () => {
    const ledgerStore = new MemoryLedgerStore();
    // seed tenant-a and tenant-b same accountId/asset
    for (let i = 0; i < 3; i++) {
      const tx = makeTx({
        tenantId: 'tenant-a',
        accountId: 'shared',
        asset: 'USDT',
        type: 'credit',
        amount: 10,
        prevVersion: i,
        fencingToken: i + 1,
        idempotencyKey: randomUUID(),
      });
      await ledgerStore.appendTransaction(tx);
    }
    for (let i = 0; i < 2; i++) {
      const tx = makeTx({
        tenantId: 'tenant-b',
        accountId: 'shared',
        asset: 'USDT',
        type: 'credit',
        amount: 99,
        prevVersion: i,
        fencingToken: i + 1,
        idempotencyKey: randomUUID(),
      });
      await ledgerStore.appendTransaction(tx);
    }
    const tenantStore = new MemoryTenantStore([
      { id: 'tenant-a', tier: 'starter', apiKey: 'key-a' },
      { id: 'tenant-b', tier: 'pro', apiKey: 'key-b' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });

    const resA = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=shared&asset=USDT&limit=2',
        headers: { 'x-api-key': 'key-a' },
      }) as never,
      resA as never,
    );
    expect(resA.statusCode).toBe(200);
    expect(resA.headers['content-type']).toContain('application/json');
    expect(resA.headers['x-next-cursor']).toBeDefined();
    const bodyA = JSON.parse(resA.body);
    expect(bodyA.rows.length).toBe(2);
    expect(bodyA.nextCursor).toBeDefined();
    // rows belong to tenant-a only
    expect(bodyA.rows.every((r: { accountId: string }) => r.accountId === 'shared')).toBe(true);
    // amount 10 for tenant-a, not 99
    expect(bodyA.rows[0].amount).toBe(10);

    // per-tenant isolation: tenant-b sees only its 2 rows
    const resB = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=shared&asset=USDT',
        headers: { 'x-api-key': 'key-b' },
      }) as never,
      resB as never,
    );
    expect(resB.statusCode).toBe(200);
    const bodyB = JSON.parse(resB.body);
    expect(bodyB.rows.length).toBe(2);
    expect(bodyB.rows[0].amount).toBe(99);

    // tenant-a next page via cursor
    const cursor = bodyA.nextCursor;
    const resA2 = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: `/api/ledger/statement?accountId=shared&asset=USDT&cursor=${cursor}`,
        headers: { 'x-api-key': 'key-a' },
      }) as never,
      resA2 as never,
    );
    const bodyA2 = JSON.parse(resA2.body);
    expect(bodyA2.rows.length).toBe(1); // remaining 1 of 3
    expect(bodyA2.rows[0].prevVersion).toBe(2);
    // no duplicates
    const ids = new Set(bodyA.rows.map((r: { transactionId: string }) => r.transactionId));
    expect(ids.has(bodyA2.rows[0].transactionId)).toBe(false);
  });

  it('csv/jsonl content-type and header correctness', async () => {
    const ledgerStore = new MemoryLedgerStore();
    const tx = makeTx({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      type: 'credit',
      amount: 50,
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: 'idem-csv',
    });
    await ledgerStore.appendTransaction(tx);
    const tenantStore = new MemoryTenantStore([
      { id: 't1', tier: 'starter', apiKey: 'k1' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });

    const resCsv = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT&format=csv',
        headers: { 'x-api-key': 'k1' },
      }) as never,
      resCsv as never,
    );
    expect(resCsv.statusCode).toBe(200);
    expect(resCsv.headers['content-type']).toBe('text/csv');
    expect(resCsv.body.split('\n')[0]).toBe(
      'timestamp,transactionId,accountId,asset,type,amount,balanceAfter,prevVersion,kafkaOffset,fencingToken,idempotencyKey,transferId',
    );
    expect(resCsv.body).toContain('idem-csv');

    const resJsonl = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT&format=jsonl',
        headers: { 'x-api-key': 'k1' },
      }) as never,
      resJsonl as never,
    );
    expect(resJsonl.statusCode).toBe(200);
    expect(resJsonl.headers['content-type']).toBe('application/x-ndjson');
    const lines = resJsonl.body.split('\n').filter(Boolean);
    expect(lines.length).toBe(1);
    expect(JSON.parse(lines[0]!).transactionId).toBe(tx.id);
    // no NormalizedEvent leak
    expect(resCsv.body).not.toContain('NormalizedEvent');
    expect(resJsonl.body).not.toContain('NormalizedEvent');
  });

  it('rate-limit 429 after exceeding tier (starter 60)', async () => {
    const ledgerStore = new MemoryLedgerStore();
    const tenantStore = new MemoryTenantStore([
      { id: 't1', tier: 'starter', apiKey: 'k1' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });
    for (let i = 0; i < 60; i++) {
      const res = mockRes();
      await gw.handleStatementRequest(
        mockReq({
          url: '/api/ledger/statement?accountId=acc1&asset=USDT',
          headers: { 'x-api-key': 'k1' },
        }) as never,
        res as never,
      );
      expect(res.statusCode).toBe(200);
    }
    const blocked = mockRes();
    await gw.handleStatementRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT',
        headers: { 'x-api-key': 'k1' },
      }) as never,
      blocked as never,
    );
    expect(blocked.statusCode).toBe(429);
    expect(blocked.headers['retry-after']).toBeDefined();
    expect(blocked.headers['x-ratelimit-remaining']).toBe('0');
  });

  it('supports Authorization Bearer and derives tenant isolation via handleRequest wrapper', async () => {
    const ledgerStore = new MemoryLedgerStore();
    const tx = makeTx({
      tenantId: 't1',
      accountId: 'acc1',
      asset: 'USDT',
      prevVersion: 0,
      fencingToken: 1,
      idempotencyKey: 'idem-bearer',
    });
    await ledgerStore.appendTransaction(tx);
    const tenantStore = new MemoryTenantStore([
      { id: 't1', tier: 'starter', apiKey: 'k1' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });
    const res = mockRes();
    await gw.handleRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc1&asset=USDT',
        headers: { authorization: 'Bearer k1' },
      }) as never,
      res as never,
    );
    expect(res.statusCode).toBe(200);
    // unknown path 404
    const res404 = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/unknown', headers: { 'x-api-key': 'k1' } }) as never,
      res404 as never,
    );
    expect(res404.statusCode).toBe(404);
  });

  it('per-tenant isolation across two tenants via gateway handleRequest', async () => {
    const ledgerStore = new MemoryLedgerStore();
    const tenantStore = new MemoryTenantStore([
      { id: 'tenant-x', tier: 'starter', apiKey: 'kx' },
      { id: 'tenant-y', tier: 'starter', apiKey: 'ky' },
    ]) as never;
    const gw = createLedgerGateway({
      tenantStore,
      ledgerStore,
      rateLimiter: createMemoryRateLimiter(),
    });
    // same account/asset different tenant
    await ledgerStore.appendTransaction(
      makeTx({
        tenantId: 'tenant-x',
        accountId: 'acc',
        asset: 'BTC',
        amount: 1,
        prevVersion: 0,
        fencingToken: 1,
        idempotencyKey: 'ix',
      }),
    );
    await ledgerStore.appendTransaction(
      makeTx({
        tenantId: 'tenant-y',
        accountId: 'acc',
        asset: 'BTC',
        amount: 2,
        prevVersion: 0,
        fencingToken: 1,
        idempotencyKey: 'iy',
      }),
    );
    const resX = mockRes();
    await gw.handleRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc&asset=BTC',
        headers: { 'x-api-key': 'kx' },
      }) as never,
      resX as never,
    );
    const bodyX = JSON.parse(resX.body);
    expect(bodyX.rows.length).toBe(1);
    expect(bodyX.rows[0].amount).toBe(1);
    const resY = mockRes();
    await gw.handleRequest(
      mockReq({
        url: '/api/ledger/statement?accountId=acc&asset=BTC',
        headers: { 'x-api-key': 'ky' },
      }) as never,
      resY as never,
    );
    const bodyY = JSON.parse(resY.body);
    expect(bodyY.rows.length).toBe(1);
    expect(bodyY.rows[0].amount).toBe(2);
    expect(bodyX.rows[0].transactionId).not.toBe(bodyY.rows[0].transactionId);
  });
});
