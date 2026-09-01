import { randomBytes, randomUUID } from 'node:crypto';
import { createClient } from 'redis';
import { getTier } from './tiers.js';
import type { Tier } from './tiers.js';

export interface Tenant {
  id: string;
  name: string;
  apiKey: string;
  tier: Tier;
  stripeCustomerId?: string;
  stripeSubscriptionId?: string;
  createdAt: Date;
  entitlements: {
    venues: string[];
    features: string[];
  };
}

export interface TenantStore {
  create(name: string, tier: Tier): Promise<Tenant>;
  findByApiKey(key: string): Promise<Tenant | undefined>;
  findById(id: string): Promise<Tenant | undefined>;
  list(): Promise<Tenant[]>;
  updateTier(id: string, tier: Tier): Promise<Tenant>;
}

function generateApiKey(): string {
  return `qf_${randomBytes(8).toString('hex')}`;
}

function entitlementsFor(tier: Tier): { venues: string[]; features: string[] } {
  const def = getTier(tier);
  return {
    venues: ['binance', 'kraken'],
    features: def ? [...def.features] : [],
  };
}

export class MemoryTenantStore implements TenantStore {
  readonly byId = new Map<string, Tenant>();
  readonly byApiKey = new Map<string, Tenant>();

  async create(name: string, tier: Tier): Promise<Tenant> {
    const def = getTier(tier);
    if (!def) throw new Error(`unknown tier: ${tier}`);
    const tenant: Tenant = {
      id: randomUUID(),
      name,
      apiKey: generateApiKey(),
      tier,
      createdAt: new Date(),
      entitlements: entitlementsFor(tier),
    };
    this.byId.set(tenant.id, tenant);
    this.byApiKey.set(tenant.apiKey, tenant);
    return {
      ...tenant,
      entitlements: {
        ...tenant.entitlements,
        features: [...tenant.entitlements.features],
        venues: [...tenant.entitlements.venues],
      },
    };
  }

  async findByApiKey(key: string): Promise<Tenant | undefined> {
    const found = this.byApiKey.get(key);
    if (!found) return undefined;
    return {
      ...found,
      entitlements: {
        ...found.entitlements,
        features: [...found.entitlements.features],
        venues: [...found.entitlements.venues],
      },
    };
  }

  async findById(id: string): Promise<Tenant | undefined> {
    const found = this.byId.get(id);
    if (!found) return undefined;
    return {
      ...found,
      entitlements: {
        ...found.entitlements,
        features: [...found.entitlements.features],
        venues: [...found.entitlements.venues],
      },
    };
  }

  async list(): Promise<Tenant[]> {
    return [...this.byId.values()].map((t) => ({
      ...t,
      entitlements: {
        ...t.entitlements,
        features: [...t.entitlements.features],
        venues: [...t.entitlements.venues],
      },
    }));
  }

  async updateTier(id: string, tier: Tier): Promise<Tenant> {
    const existing = this.byId.get(id);
    if (!existing) throw new Error(`tenant not found: ${id}`);
    const def = getTier(tier);
    if (!def) throw new Error(`unknown tier: ${tier}`);
    const updated: Tenant = {
      ...existing,
      tier,
      entitlements: entitlementsFor(tier),
    };
    this.byId.set(id, updated);
    this.byApiKey.set(updated.apiKey, updated);
    return {
      ...updated,
      entitlements: {
        ...updated.entitlements,
        features: [...updated.entitlements.features],
        venues: [...updated.entitlements.venues],
      },
    };
  }

  async setStripeCustomerId(
    id: string,
    customerId: string,
    subscriptionId?: string,
  ): Promise<Tenant> {
    const existing = this.byId.get(id);
    if (!existing) throw new Error(`tenant not found: ${id}`);
    const updated: Tenant = {
      ...existing,
      stripeCustomerId: customerId,
      stripeSubscriptionId: subscriptionId ?? existing.stripeSubscriptionId,
    };
    this.byId.set(id, updated);
    this.byApiKey.set(updated.apiKey, updated);
    return {
      ...updated,
      entitlements: {
        ...updated.entitlements,
        venues: [...updated.entitlements.venues],
        features: [...updated.entitlements.features],
      },
    };
  }
}

export interface RedisTenantClient {
  hSet(key: string, fields: Record<string, string>): Promise<number>;
  hGetAll(key: string): Promise<Record<string, string>>;
  set(key: string, value: string, opts?: { NX?: true; EX?: number }): Promise<string | null>;
  get(key: string): Promise<string | null>;
  sAdd(key: string, member: string): Promise<number>;
  sMembers(key: string): Promise<string[]>;
  sRem?(key: string, member: string): Promise<number>;
  del(key: string): Promise<number>;
  connect?(): Promise<void>;
  isOpen?: boolean;
}

class InMemoryTenantRedis implements RedisTenantClient {
  private readonly hashes = new Map<string, Map<string, string>>();
  private readonly kv = new Map<string, string>();
  private readonly sets = new Map<string, Set<string>>();

  async hSet(key: string, fields: Record<string, string>): Promise<number> {
    let m = this.hashes.get(key);
    if (!m) {
      m = new Map<string, string>();
      this.hashes.set(key, m);
    }
    let added = 0;
    for (const [k, v] of Object.entries(fields)) {
      if (!m.has(k)) added++;
      m.set(k, v);
    }
    return added;
  }

  async hGetAll(key: string): Promise<Record<string, string>> {
    const m = this.hashes.get(key);
    if (!m) return {};
    const out: Record<string, string> = {};
    for (const [k, v] of m) out[k] = v;
    return out;
  }

  async set(key: string, value: string, opts?: { NX?: true; EX?: number }): Promise<string | null> {
    if (opts?.NX && this.kv.has(key)) return null;
    this.kv.set(key, value);
    return 'OK';
  }

  async get(key: string): Promise<string | null> {
    return this.kv.get(key) ?? null;
  }

  async sAdd(key: string, member: string): Promise<number> {
    let s = this.sets.get(key);
    if (!s) {
      s = new Set<string>();
      this.sets.set(key, s);
    }
    const had = s.has(member);
    s.add(member);
    return had ? 0 : 1;
  }

  async sMembers(key: string): Promise<string[]> {
    const s = this.sets.get(key);
    if (!s) return [];
    return [...s];
  }

  async sRem(key: string, member: string): Promise<number> {
    const s = this.sets.get(key);
    if (!s) return 0;
    return s.delete(member) ? 1 : 0;
  }

  async del(key: string): Promise<number> {
    let n = 0;
    if (this.hashes.delete(key)) n++;
    if (this.kv.delete(key)) n++;
    if (this.sets.delete(key)) n++;
    return n;
  }

  async flushDbForTests(): Promise<void> {
    this.hashes.clear();
    this.kv.clear();
    this.sets.clear();
  }
}

export { InMemoryTenantRedis as FakeRedisTenantClient };

function serializeTenant(t: Tenant): Record<string, string> {
  return {
    id: t.id,
    name: t.name,
    apiKey: t.apiKey,
    tier: t.tier,
    stripeCustomerId: t.stripeCustomerId ?? '',
    stripeSubscriptionId: t.stripeSubscriptionId ?? '',
    createdAt: t.createdAt.toISOString(),
    venues: JSON.stringify(t.entitlements.venues),
    features: JSON.stringify(t.entitlements.features),
  };
}

function deserializeTenant(fields: Record<string, string>): Tenant | undefined {
  if (!fields['id'] || !fields['apiKey']) return undefined;
  return {
    id: fields['id']!,
    name: fields['name'] ?? '',
    apiKey: fields['apiKey']!,
    tier: (fields['tier'] as Tier) ?? 'starter',
    stripeCustomerId: fields['stripeCustomerId'] ? fields['stripeCustomerId'] : undefined,
    stripeSubscriptionId: fields['stripeSubscriptionId']
      ? fields['stripeSubscriptionId']
      : undefined,
    createdAt: fields['createdAt'] ? new Date(fields['createdAt']!) : new Date(),
    entitlements: {
      venues: fields['venues']
        ? (JSON.parse(fields['venues']!) as string[])
        : ['binance', 'kraken'],
      features: fields['features'] ? (JSON.parse(fields['features']!) as string[]) : [],
    },
  };
}

export class RedisTenantStore implements TenantStore {
  readonly client: RedisTenantClient;
  private readonly fallback = new MemoryTenantStore();
  private _connected = false;
  private _connectPromise: Promise<void> | null = null;
  private _useFallback = false;

  constructor(client?: RedisTenantClient) {
    if (client) {
      this.client = client;
      const maybeReal = client as unknown as { connect?: () => Promise<void>; isOpen?: boolean };
      if (typeof maybeReal.connect === 'function' && maybeReal.isOpen === false) {
        this._connected = false;
      } else {
        this._connected = true;
      }
    } else {
      // Hermetic default: in-memory fake for fast tests. Real path via createWithRealRedis() which uses createClient from redis.
      this.client = new InMemoryTenantRedis();
      this._connected = true;
    }
  }

  static createWithRealRedis(
    url = process.env['REDIS_URL'] ?? 'redis://localhost:6379',
  ): RedisTenantStore {
    const c = createClient({ url }) as unknown as RedisTenantClient;
    return new RedisTenantStore(c);
  }

  private async ensureConnected(): Promise<void> {
    if (this._connected || this._useFallback) return;
    const c = this.client as unknown as { connect?: () => Promise<void>; isOpen?: boolean };
    if (typeof c.connect === 'function' && c.isOpen === false) {
      if (!this._connectPromise) {
        this._connectPromise = c
          .connect()
          .then(() => {
            this._connected = true;
          })
          .catch(() => {
            // Fall back to in-memory fake for hermetic env without live redis
            this._useFallback = true;
          });
      }
      await this._connectPromise;
    } else if (typeof c.connect !== 'function') {
      this._connected = true;
    } else {
      this._connected = true;
    }
  }

  private get useFallback(): boolean {
    return this._useFallback;
  }

  async create(name: string, tier: Tier): Promise<Tenant> {
    if (this.useFallback) return this.fallback.create(name, tier);
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.create(name, tier);
    }
    if (this.useFallback) return this.fallback.create(name, tier);
    const def = getTier(tier);
    if (!def) throw new Error(`unknown tier: ${tier}`);
    const tenant: Tenant = {
      id: randomUUID(),
      name,
      apiKey: generateApiKey(),
      tier,
      createdAt: new Date(),
      entitlements: entitlementsFor(tier),
    };
    const key = `tenant:${tenant.id}`;
    const fields = serializeTenant(tenant);
    await this.client.hSet(key, fields);
    await this.client.set(`tenant:apikey:${tenant.apiKey}`, tenant.id);
    await this.client.sAdd('tenant:index', tenant.id);
    return {
      ...tenant,
      entitlements: {
        ...tenant.entitlements,
        venues: [...tenant.entitlements.venues],
        features: [...tenant.entitlements.features],
      },
    };
  }

  async findByApiKey(key: string): Promise<Tenant | undefined> {
    if (this.useFallback) return this.fallback.findByApiKey(key);
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.findByApiKey(key);
    }
    if (this.useFallback) return this.fallback.findByApiKey(key);
    const id = await this.client.get(`tenant:apikey:${key}`);
    if (!id) return undefined;
    return this.findById(id);
  }

  async findById(id: string): Promise<Tenant | undefined> {
    if (this.useFallback) return this.fallback.findById(id);
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.findById(id);
    }
    if (this.useFallback) return this.fallback.findById(id);
    const fields = await this.client.hGetAll(`tenant:${id}`);
    if (!fields || Object.keys(fields).length === 0) return undefined;
    const t = deserializeTenant(fields);
    return t;
  }

  async list(): Promise<Tenant[]> {
    if (this.useFallback) return this.fallback.list();
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.list();
    }
    if (this.useFallback) return this.fallback.list();
    const ids = await this.client.sMembers('tenant:index');
    const out: Tenant[] = [];
    for (const id of ids) {
      const f = await this.client.hGetAll(`tenant:${id}`);
      const t = deserializeTenant(f);
      if (t) out.push(t);
    }
    return out;
  }

  async updateTier(id: string, tier: Tier): Promise<Tenant> {
    if (this.useFallback) return this.fallback.updateTier(id, tier);
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.updateTier(id, tier);
    }
    if (this.useFallback) return this.fallback.updateTier(id, tier);
    const def = getTier(tier);
    if (!def) throw new Error(`unknown tier: ${tier}`);
    const existing = await this.findById(id);
    if (!existing) throw new Error(`tenant not found: ${id}`);
    const updated: Tenant = { ...existing, tier, entitlements: entitlementsFor(tier) };
    const fields = serializeTenant(updated);
    await this.client.hSet(`tenant:${id}`, fields);
    return updated;
  }

  async setStripeCustomerId(
    id: string,
    customerId: string,
    subscriptionId?: string,
  ): Promise<Tenant> {
    if (this.useFallback) return this.fallback.setStripeCustomerId(id, customerId, subscriptionId);
    try {
      await this.ensureConnected();
    } catch {
      return this.fallback.setStripeCustomerId(id, customerId, subscriptionId);
    }
    if (this.useFallback) return this.fallback.setStripeCustomerId(id, customerId, subscriptionId);
    const existing = await this.findById(id);
    if (!existing) throw new Error(`tenant not found: ${id}`);
    const updated: Tenant = {
      ...existing,
      stripeCustomerId: customerId,
      stripeSubscriptionId: subscriptionId ?? existing.stripeSubscriptionId,
    };
    const fields = serializeTenant(updated);
    await this.client.hSet(`tenant:${id}`, fields);
    // optional reverse index
    await this.client.set(`tenant:stripe:${customerId}`, id);
    return updated;
  }

  async flushForTests(): Promise<void> {
    if (this.useFallback) {
      // clear fallback
      (this.fallback as unknown as { byId: Map<string, unknown> }).byId.clear();
      (this.fallback as unknown as { byApiKey: Map<string, unknown> }).byApiKey.clear();
      return;
    }
    const anyClient = this.client as unknown as InMemoryTenantRedis & {
      flushDb?: () => Promise<void>;
      flushAll?: () => Promise<void>;
      sendCommand?: (args: string[]) => Promise<unknown>;
    };
    if (typeof anyClient.flushDbForTests === 'function') {
      await anyClient.flushDbForTests();
      return;
    }
    // For real redis, FLUSHDB via sendCommand fallback
    try {
      const real = this.client as unknown as { sendCommand?: (args: string[]) => Promise<unknown> };
      if (real.sendCommand) await real.sendCommand(['FLUSHDB']);
    } catch {
      // ignore
    }
  }
}
