import { describe, expect, it } from 'vitest';
import { MemoryTenantStore, RedisTenantStore } from '../../src/billing/tenant.js';

describe('MemoryTenantStore', () => {
  it('creates tenants with distinct keys and correct apiKey format', async () => {
    const store = new MemoryTenantStore();
    const a = await store.create('Acme', 'starter');
    const b = await store.create('Beta', 'pro');

    expect(a.apiKey).toMatch(/^qf_[0-9a-f]{16}$/);
    expect(b.apiKey).toMatch(/^qf_[0-9a-f]{16}$/);
    expect(a.apiKey).not.toBe(b.apiKey);
    expect(a.id).not.toBe(b.id);
    expect(a.tier).toBe('starter');
    expect(b.tier).toBe('pro');
    expect(a.createdAt).toBeInstanceOf(Date);
  });

  it('findByApiKey returns the correct tenant', async () => {
    const store = new MemoryTenantStore();
    const created = await store.create('Acme', 'starter');
    const found = await store.findByApiKey(created.apiKey);
    expect(found?.id).toBe(created.id);
    expect(found?.name).toBe('Acme');
    expect(await store.findByApiKey('qf_0000000000000000')).toBeUndefined();
  });

  it('findById returns the correct tenant', async () => {
    const store = new MemoryTenantStore();
    const created = await store.create('Acme', 'starter');
    const found = await store.findById(created.id);
    expect(found?.apiKey).toBe(created.apiKey);
    expect(await store.findById('nope')).toBeUndefined();
  });

  it('updateTier changes tier and entitlements', async () => {
    const store = new MemoryTenantStore();
    const tenant = await store.create('Acme', 'starter');
    expect(tenant.entitlements.features).toEqual(['F1']);

    const updated = await store.updateTier(tenant.id, 'pro');
    expect(updated.tier).toBe('pro');
    expect(updated.entitlements.features).toEqual(['F1', 'F2']);
    expect(updated.apiKey).toBe(tenant.apiKey);

    const refetched = await store.findById(tenant.id);
    expect(refetched?.tier).toBe('pro');
  });

  it('list returns all tenants', async () => {
    const store = new MemoryTenantStore();
    await store.create('A', 'starter');
    await store.create('B', 'pro');
    await store.create('C', 'enterprise');
    const all = await store.list();
    expect(all).toHaveLength(3);
    expect(all.map((t) => t.name).sort()).toEqual(['A', 'B', 'C']);
  });

  it('updateTier throws for unknown tenant', async () => {
    const store = new MemoryTenantStore();
    await expect(store.updateTier('missing', 'pro')).rejects.toThrow(/tenant not found/);
  });
});

describe('RedisTenantStore stub', () => {
  it('delegates to in-memory and documents key pattern', async () => {
    const store = new RedisTenantStore();
    const t = await store.create('Acme', 'starter');
    expect(t.apiKey).toMatch(/^qf_/);
    expect(await store.findByApiKey(t.apiKey)).toBeDefined();
    expect(await store.findById(t.id)).toBeDefined();
    expect(await store.list()).toHaveLength(1);
    const updated = await store.updateTier(t.id, 'enterprise');
    expect(updated.entitlements.features).toEqual(['F1', 'F2', 'F4', 'F3']);
  });
});
