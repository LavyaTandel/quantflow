import { describe, it, expect } from 'vitest';
import { MemoryScenarioStore } from '../../src/scenariox/store.js';

describe('MemoryScenarioStore — per-tenant isolation', () => {
  it('second tenant Portfolio isolated from first (same name, different tenantId -> 0 cross)', async () => {
    const store = new MemoryScenarioStore();
    const t1 = 'tenant-a';
    const t2 = 'tenant-b';

    const p1 = await store.createPortfolio(t1, 'Alpha');
    await store.createPortfolio(t2, 'Alpha');

    const listT1 = await store.listPortfolios(t1);
    const listT2 = await store.listPortfolios(t2);

    expect(listT1).toHaveLength(1);
    expect(listT2).toHaveLength(1);
    expect(listT1[0]!.id).toBe(p1.id);
    expect(listT1[0]!.tenantId).toBe(t1);
    expect(listT2[0]!.tenantId).toBe(t2);
    expect(listT1[0]!.id).not.toBe(listT2[0]!.id);

    // cross-tenant get should be undefined (tenant isolation)
    const cross = await store.getPortfolio(t2, p1.id);
    expect(cross).toBeUndefined();
    const cross2 = await store.getPortfolio(t1, listT2[0]!.id);
    expect(cross2).toBeUndefined();
  });

  it('CRUD: createPortfolio, addPosition, listPositions, updatePosition, removePosition', async () => {
    const store = new MemoryScenarioStore();
    const tenant = 't-crud';
    const p = await store.createPortfolio(tenant, 'Growth');

    const pos1 = await store.addPosition(p.id, {
      tenantId: tenant,
      symbol: 'BTCUSDT',
      quantity: 2,
      avgEntryPrice: 30000,
      side: 'long',
    });
    expect(pos1.portfolioId).toBe(p.id);
    expect(pos1.tenantId).toBe(tenant);

    const pos2 = await store.addPosition(p.id, {
      tenantId: tenant,
      symbol: 'ETHUSDT',
      quantity: 10,
      avgEntryPrice: 2000,
      side: 'short',
    });

    const positions = await store.listPositions(tenant, p.id);
    expect(positions).toHaveLength(2);

    const fetched = await store.getPosition(tenant, p.id, pos1.id);
    expect(fetched?.symbol).toBe('BTCUSDT');

    const updated = await store.updatePosition(tenant, p.id, pos1.id, { quantity: 5 });
    expect(updated?.quantity).toBe(5);
    expect(updated?.symbol).toBe('BTCUSDT');

    const removed = await store.removePosition(tenant, p.id, pos2.id);
    expect(removed).toBe(true);
    const after = await store.listPositions(tenant, p.id);
    expect(after).toHaveLength(1);
    expect(after[0]!.id).toBe(pos1.id);
  });

  it('per-tenant isolation: positions not visible across tenants', async () => {
    const store = new MemoryScenarioStore();
    const t1 = 't-iso-1';
    const t2 = 't-iso-2';
    const p1 = await store.createPortfolio(t1, 'P1');
    const p2 = await store.createPortfolio(t2, 'P2');

    await store.addPosition(p1.id, {
      tenantId: t1,
      symbol: 'BTCUSDT',
      quantity: 1,
      avgEntryPrice: 30000,
      side: 'long',
    });
    await store.addPosition(p2.id, {
      tenantId: t2,
      symbol: 'BTCUSDT',
      quantity: 99,
      avgEntryPrice: 30000,
      side: 'long',
    });

    const listT1 = await store.listPositions(t1, p1.id);
    const listT2 = await store.listPositions(t2, p2.id);
    expect(listT1).toHaveLength(1);
    expect(listT2).toHaveLength(1);
    expect(listT1[0]!.quantity).toBe(1);
    expect(listT2[0]!.quantity).toBe(99);

    // Attempt to read t1 position via t2 tenant should be undefined / empty
    const crossList = await store.listPositions(t2, p1.id);
    expect(crossList).toHaveLength(0);
    const crossPos = await store.getPosition(t2, p1.id, listT1[0]!.id);
    expect(crossPos).toBeUndefined();
  });

  it('updatePosition tenant isolation — wrong tenant cannot mutate', async () => {
    const store = new MemoryScenarioStore();
    const t1 = 't-upd-1';
    const t2 = 't-upd-2';
    const p1 = await store.createPortfolio(t1, 'P1');
    const pos = await store.addPosition(p1.id, {
      tenantId: t1,
      symbol: 'BTCUSDT',
      quantity: 1,
      avgEntryPrice: 30000,
      side: 'long',
    });
    const badUpdate = await store.updatePosition(t2, p1.id, pos.id, { quantity: 999 });
    expect(badUpdate).toBeUndefined();
    const good = await store.getPosition(t1, p1.id, pos.id);
    expect(good?.quantity).toBe(1);
  });
});
