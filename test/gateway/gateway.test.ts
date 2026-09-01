import { describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { createGateway } from '../../src/gateway/index.js';
import { createMemoryRateLimiter } from '../../src/gateway/rateLimiter.js';
import type { Tenant } from '../../src/gateway/auth.js';

class MemoryTenantStore {
  readonly #byKey = new Map<string, Tenant>();
  constructor(tenants: Tenant[]) {
    for (const t of tenants) this.#byKey.set(t.apiKey, t);
  }
  async findByApiKey(apiKey: string): Promise<Tenant | undefined> {
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
      r.body = data;
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

class MockSocket extends EventEmitter {
  chunks: string[] = [];
  destroyed = false;
  write(data: string): boolean {
    this.chunks.push(String(data));
    return true;
  }
  destroy(): void {
    this.destroyed = true;
    this.emit('close');
  }
  get text(): string {
    return this.chunks.join('');
  }
}

describe('gateway handleRequest', () => {
  it('returns tenant-specific responses and proves isolation', async () => {
    const store = new MemoryTenantStore([
      { id: 'tenant-a', tier: 'starter', apiKey: 'key-a' },
      { id: 'tenant-b', tier: 'pro', apiKey: 'key-b' },
    ]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });

    const resA = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/api/market-data/BTCUSDT', headers: { 'x-api-key': 'key-a' } }) as never,
      resA as never,
    );
    expect(resA.statusCode).toBe(200);
    expect(JSON.parse(resA.body).tenant).toBe('tenant-a');
    expect(JSON.parse(resA.body).tier).toBe('starter');
    expect(JSON.parse(resA.body).derivedOnly).toBe(true);

    const resB = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/api/market-data/BTCUSDT', headers: { 'x-api-key': 'key-b' } }) as never,
      resB as never,
    );
    expect(resB.statusCode).toBe(200);
    expect(JSON.parse(resB.body).tenant).toBe('tenant-b');
    expect(JSON.parse(resB.body).derivedOnly).toBe(true);
    expect(resB.headers['x-ratelimit-remaining']).toBeDefined();
  });

  it('rate limits starter after 60 requests with 429 and Retry-After', async () => {
    const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });

    for (let i = 0; i < 60; i++) {
      const res = mockRes();
      await gw.handleRequest(
        mockReq({ url: '/api/alerts/btc', headers: { 'x-api-key': 'k1' } }) as never,
        res as never,
      );
      expect(res.statusCode).toBe(200);
    }
    const blocked = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/api/alerts/btc', headers: { 'x-api-key': 'k1' } }) as never,
      blocked as never,
    );
    expect(blocked.statusCode).toBe(429);
    expect(blocked.headers['retry-after']).toBeDefined();
    expect(blocked.headers['x-ratelimit-remaining']).toBe('0');
  });

  it('enterprise is unlimited', async () => {
    const store = new MemoryTenantStore([{ id: 'ent', tier: 'enterprise', apiKey: 'ent-key' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });
    for (let i = 0; i < 120; i++) {
      const res = mockRes();
      await gw.handleRequest(
        mockReq({ url: '/api/market-data/BTCUSDT', headers: { 'x-api-key': 'ent-key' } }) as never,
        res as never,
      );
      expect(res.statusCode).toBe(200);
    }
  });

  it('routes alerts and market-data, 404 unknown, 401 missing/invalid', async () => {
    const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });

    const resAlerts = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/api/alerts/BTCUSDT', headers: { 'x-api-key': 'k1' } }) as never,
      resAlerts as never,
    );
    expect(resAlerts.statusCode).toBe(200);
    expect(JSON.parse(resAlerts.body).derivedOnly).toBe(true);

    const res404 = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/unknown', headers: { 'x-api-key': 'k1' } }) as never,
      res404 as never,
    );
    expect(res404.statusCode).toBe(404);

    const res401Missing = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/api/market-data/BTCUSDT' }) as never,
      res401Missing as never,
    );
    expect(res401Missing.statusCode).toBe(401);

    const res401Bad = mockRes();
    await gw.handleRequest(
      mockReq({ url: '/api/market-data/BTCUSDT', headers: { 'x-api-key': 'bad' } }) as never,
      res401Bad as never,
    );
    expect(res401Bad.statusCode).toBe(401);

    const resBearer = mockRes();
    await gw.handleRequest(
      mockReq({
        url: '/api/market-data/BTCUSDT',
        headers: { authorization: 'Bearer k1' },
      }) as never,
      resBearer as never,
    );
    expect(resBearer.statusCode).toBe(200);
  });

  it('window reset allows again after 60s', async () => {
    vi.useFakeTimers();
    try {
      const base = 1_700_000_000_000;
      vi.setSystemTime(base);
      const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
      const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });
      for (let i = 0; i < 60; i++) {
        const res = mockRes();
        await gw.handleRequest(
          mockReq({ url: '/api/market-data/x', headers: { 'x-api-key': 'k1' } }) as never,
          res as never,
        );
        expect(res.statusCode).toBe(200);
      }
      const blocked = mockRes();
      await gw.handleRequest(
        mockReq({ url: '/api/market-data/x', headers: { 'x-api-key': 'k1' } }) as never,
        blocked as never,
      );
      expect(blocked.statusCode).toBe(429);
      vi.setSystemTime(base + 60_001);
      const after = mockRes();
      await gw.handleRequest(
        mockReq({ url: '/api/market-data/x', headers: { 'x-api-key': 'k1' } }) as never,
        after as never,
      );
      expect(after.statusCode).toBe(200);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('gateway handleUpgrade', () => {
  it('sends hello frame with derivedOnly and no raw NormalizedEvent', async () => {
    const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });
    const sock = new MockSocket();
    await gw.handleUpgrade(
      mockReq({ url: '/ws?apiKey=k1', headers: {} }) as never,
      sock as never,
      Buffer.alloc(0),
    );
    expect(sock.destroyed).toBe(false);
    expect(sock.text).toContain('"t":"hello"');
    expect(sock.text).toContain('"derivedOnly":true');
    expect(sock.text).not.toContain('NormalizedEvent');
    expect(sock.text).not.toContain('"kind":"depth"');
  });

  it('supports header x-api-key for WS', async () => {
    const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });
    const sock = new MockSocket();
    await gw.handleUpgrade(
      mockReq({ url: '/ws', headers: { 'x-api-key': 'k1' } }) as never,
      sock as never,
      Buffer.alloc(0),
    );
    expect(sock.text).toContain('"derivedOnly":true');
  });

  it('rejects unauthenticated upgrade', async () => {
    const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });
    const sock = new MockSocket();
    await gw.handleUpgrade(
      mockReq({ url: '/ws', headers: {} }) as never,
      sock as never,
      Buffer.alloc(0),
    );
    expect(sock.destroyed).toBe(true);
    expect(sock.text).toContain('401');
  });

  it('enforces WS connection limit per tier', async () => {
    const store = new MemoryTenantStore([{ id: 't1', tier: 'starter', apiKey: 'k1' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });

    const sockets: MockSocket[] = [];
    for (let i = 0; i < 5; i++) {
      const s = new MockSocket();
      await gw.handleUpgrade(
        mockReq({ url: '/ws?apiKey=k1' }) as never,
        s as never,
        Buffer.alloc(0),
      );
      expect(s.destroyed).toBe(false);
      sockets.push(s);
    }

    const s6 = new MockSocket();
    await gw.handleUpgrade(
      mockReq({ url: '/ws?apiKey=k1' }) as never,
      s6 as never,
      Buffer.alloc(0),
    );
    expect(s6.destroyed).toBe(true);
    expect(s6.text).toContain('429');

    sockets[0]!.destroy();
    const s7 = new MockSocket();
    await gw.handleUpgrade(
      mockReq({ url: '/ws?apiKey=k1' }) as never,
      s7 as never,
      Buffer.alloc(0),
    );
    expect(s7.destroyed).toBe(false);
  });

  it('enterprise WS unlimited', async () => {
    const store = new MemoryTenantStore([{ id: 'ent', tier: 'enterprise', apiKey: 'ek' }]);
    const gw = createGateway({ tenantStore: store, rateLimiter: createMemoryRateLimiter() });
    for (let i = 0; i < 5; i++) {
      const s = new MockSocket();
      await gw.handleUpgrade(
        mockReq({ url: '/ws?apiKey=ek' }) as never,
        s as never,
        Buffer.alloc(0),
      );
      expect(s.destroyed).toBe(false);
    }
  });
});
