import { describe, it, expect, beforeEach } from 'vitest';
import {
  MemoryTenantStore,
  RedisTenantStore,
  FakeRedisTenantClient,
} from '../../src/billing/tenant.js';
import { createClient } from 'redis';

function createFakeClient(): FakeRedisTenantClient {
  return new FakeRedisTenantClient();
}

async function tryRealClient(): Promise<FakeRedisTenantClient | null> {
  try {
    const url = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
    const real = createClient({ url });
    await real.connect();
    // Use unique keys per test — do not flush global DB to avoid cross-test interference
    const adapter = {
      hSet: (k: string, f: Record<string, string>) =>
        (
          real as unknown as { hSet: (k: string, f: Record<string, string>) => Promise<number> }
        ).hSet(k, f),
      hGetAll: (k: string) =>
        (real as unknown as { hGetAll: (k: string) => Promise<Record<string, string>> }).hGetAll(k),
      set: (k: string, v: string, opts?: { NX?: true; EX?: number }) =>
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (real as any).set(k, v, opts),
      get: (k: string) =>
        (real as unknown as { get: (k: string) => Promise<string | null> }).get(k),
      sAdd: (k: string, m: string) =>
        (real as unknown as { sAdd: (k: string, m: string) => Promise<number> }).sAdd(k, m),
      sMembers: (k: string) =>
        (real as unknown as { sMembers: (k: string) => Promise<string[]> }).sMembers(k),
      sRem: (k: string, m: string) =>
        (real as unknown as { sRem: (k: string, m: string) => Promise<number> }).sRem(k, m),
      del: (k: string) => (real as unknown as { del: (k: string) => Promise<number> }).del(k),
      connect: async () => {
        return;
      },
      isOpen: true,
      _real: real,
      flushDbForTests: async () => {
        return;
      },
    } as unknown as FakeRedisTenantClient;
    (globalThis as unknown as { __realRedisForTenant?: typeof real }).__realRedisForTenant = real;
    return adapter;
  } catch {
    return null;
  }
}

describe('RedisTenantStore hermetic with FakeRedis', () => {
  let client: FakeRedisTenantClient;
  let store: RedisTenantStore;

  beforeEach(async () => {
    client = createFakeClient();
    store = new RedisTenantStore(client);
  });

  it('second tenant isolation', async () => {
    const a = await store.create('Acme', 'starter');
    const b = await store.create('Beta', 'pro');
    expect(a.apiKey).not.toBe(b.apiKey);
    expect(a.id).not.toBe(b.id);
    expect(a.tier).toBe('starter');
    expect(b.tier).toBe('pro');
    const foundA = await store.findByApiKey(a.apiKey);
    const foundB = await store.findByApiKey(b.apiKey);
    expect(foundA?.id).toBe(a.id);
    expect(foundB?.id).toBe(b.id);
    const all = await store.list();
    expect(all).toHaveLength(2);
  });

  it('findByApiKey survives simulated restart (new store instance with same FakeRedis)', async () => {
    const t1 = await store.create('Acme', 'starter');
    const apiKey = t1.apiKey;
    const id = t1.id;
    // Simulate restart: create new store instance with same underlying client
    const restarted = new RedisTenantStore(client);
    const found = await restarted.findByApiKey(apiKey);
    expect(found?.id).toBe(id);
    expect(found?.name).toBe('Acme');
    expect(await restarted.findById(id)).toBeDefined();
  });

  it('list() and updateTier() persisted', async () => {
    const t = await store.create('Acme', 'starter');
    expect(t.entitlements.features).toEqual(['F1']);
    const updated = await store.updateTier(t.id, 'pro');
    expect(updated.tier).toBe('pro');
    const restarted = new RedisTenantStore(client);
    const refetched = await restarted.findById(t.id);
    expect(refetched?.tier).toBe('pro');
    expect(refetched?.entitlements.features).toEqual(['F1', 'F2']);
    const all = await restarted.list();
    expect(all).toHaveLength(1);
  });

  it('keys: tenant:{id} hash + tenant:apikey:{apiKey} -> id + tenant:index set', async () => {
    const t = await store.create('Acme', 'starter');
    const hash = await client.hGetAll(`tenant:${t.id}`);
    expect(hash['id']).toBe(t.id);
    expect(hash['apiKey']).toBe(t.apiKey);
    const idx = await client.get(`tenant:apikey:${t.apiKey}`);
    expect(idx).toBe(t.id);
    const members = await client.sMembers('tenant:index');
    expect(members).toContain(t.id);
  });

  it('MemoryTenantStore vs RedisTenantStore parity', async () => {
    const mem = new MemoryTenantStore();
    const a = await mem.create('M', 'starter');
    const b = await store.create('R', 'starter');
    expect(a.apiKey).toMatch(/^qf_/);
    expect(b.apiKey).toMatch(/^qf_/);
  });
});

describe('RedisTenantStore with real redis if available', () => {
  it('uses real redis when reachable, else skips', async () => {
    const realClient = await tryRealClient();
    if (!realClient) {
      // No live redis — test gracefully falls back to fake and passes
      const fake = createFakeClient();
      const s = new RedisTenantStore(fake);
      const t = await s.create('Fallback', 'starter');
      expect(t.apiKey).toMatch(/^qf_/);
      // cleanup real if any
      const maybeReal = (
        globalThis as unknown as { __realRedisForTenant?: { quit: () => Promise<void> } }
      ).__realRedisForTenant;
      if (maybeReal) await maybeReal.quit().catch(() => {});
      return;
    }
    const s = new RedisTenantStore(realClient);
    const uniqueName = `RealAcme-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const t1 = await s.create(uniqueName, 'starter');
    const restarted = new RedisTenantStore(realClient);
    const found = await restarted.findByApiKey(t1.apiKey);
    expect(found?.id).toBe(t1.id);
    // teardown only this tenant keys
    const maybeReal = (
      globalThis as unknown as {
        __realRedisForTenant?: {
          quit: () => Promise<void>;
          del: (k: string) => Promise<number>;
          sRem: (k: string, m: string) => Promise<number>;
        };
      }
    ).__realRedisForTenant;
    if (maybeReal) {
      try {
        await (maybeReal as unknown as { del: (k: string) => Promise<number> }).del(
          `tenant:${t1.id}`,
        );
        await (maybeReal as unknown as { del: (k: string) => Promise<number> }).del(
          `tenant:apikey:${t1.apiKey}`,
        );
        await (maybeReal as unknown as { sRem: (k: string, m: string) => Promise<number> }).sRem(
          'tenant:index',
          t1.id,
        );
      } catch (_e) {
        void _e;
      }
      await maybeReal.quit().catch(() => {});
    }
  });
});
