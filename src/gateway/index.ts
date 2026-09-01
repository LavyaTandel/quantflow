import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Duplex } from 'node:stream';
import { extractApiKey, authenticate, type TenantStore } from './auth.js';
import type { RateLimiter } from './rateLimiter.js';
import { getTier } from '../billing/tiers.js';

// v2 AI gateway reuses ledger's hardened persistence without duplication:
// RedisTenantStore (tenant:{id} HASH, tenant:apikey:{apiKey} -> id, tenant:index SET)
// and RedisRateLimiter (ratelimit:{tenantId}:{windowStart} INCR+EXPIRE 60) are shared via src/billing/*
// and src/gateway/rateLimiter.ts. Wire the same instances for both ledger and v2 AI.
// See src/billing/tenant.ts:261 and src/gateway/rateLimiter.ts:110.

export type { RateLimiter, RateLimiterResult } from './rateLimiter.js';
export { MemoryRateLimiter, RedisRateLimiter, createMemoryRateLimiter } from './rateLimiter.js';
export { AuthError, extractApiKey, authenticate } from './auth.js';
export type { Tenant, TenantStore } from './auth.js';
export { RedisTenantStore, FakeRedisTenantClient } from '../billing/tenant.js';
export { FakeRedisRateLimiterClient } from './rateLimiter.js';

function wsLimitFor(tier: string): number {
  const def = getTier(tier);
  if (def) return def.rateLimits.wsConnections;
  return 5;
}

function getPathname(url: string | undefined, host: string | undefined): string {
  const raw = url ?? '/';
  try {
    return new URL(raw, `http://${host ?? 'localhost'}`).pathname;
  } catch {
    return raw.split('?')[0] ?? '/';
  }
}

function getQueryApiKey(url: string | undefined, host: string | undefined): string | undefined {
  const raw = url ?? '/';
  try {
    return new URL(raw, `http://${host ?? 'localhost'}`).searchParams.get('apiKey') ?? undefined;
  } catch {
    return undefined;
  }
}

function hostOf(req: IncomingMessage): string | undefined {
  const h = req.headers.host;
  if (Array.isArray(h)) return h[0];
  return h;
}

export interface GatewayOptions {
  tenantStore: TenantStore;
  rateLimiter: RateLimiter;
  alertStreamFactory?: unknown;
}

export function createGateway(options: GatewayOptions): {
  handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void>;
  handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): Promise<void>;
} {
  const { tenantStore, rateLimiter } = options;
  const wsCounts = new Map<string, number>();

  async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const apiKey = extractApiKey(req);
    if (!apiKey) {
      res.writeHead(401, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'missing api key' }));
      return;
    }

    const tenant = await authenticate(apiKey, tenantStore);
    if (!tenant) {
      res.writeHead(401, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'invalid api key' }));
      return;
    }

    const rl = await rateLimiter.check(tenant.id, tenant.tier);
    const remainingHeader = Number.isFinite(rl.remaining)
      ? String(rl.remaining)
      : String(rl.remaining);

    if (!rl.allowed) {
      const retryAfter = Math.max(1, Math.ceil((rl.resetAt - Date.now()) / 1000));
      res.writeHead(429, {
        'content-type': 'application/json',
        'Retry-After': String(retryAfter),
        'X-RateLimit-Remaining': '0',
      });
      res.end(JSON.stringify({ error: 'rate limit exceeded' }));
      return;
    }

    const pathname = getPathname(req.url, hostOf(req));

    if (
      req.method === 'GET' &&
      (pathname.startsWith('/api/market-data/') || pathname.startsWith('/api/alerts/'))
    ) {
      res.writeHead(200, {
        'content-type': 'application/json',
        'X-RateLimit-Remaining': remainingHeader,
      });
      res.end(
        JSON.stringify({
          tenant: tenant.id,
          tier: tenant.tier,
          message: 'ok',
          derivedOnly: true,
        }),
      );
      return;
    }

    res.writeHead(404, {
      'content-type': 'application/json',
      'X-RateLimit-Remaining': remainingHeader,
    });
    res.end(JSON.stringify({ error: 'not found' }));
  }

  async function handleUpgrade(req: IncomingMessage, socket: Duplex, _head: Buffer): Promise<void> {
    const headerKey = extractApiKey(req);
    const queryKey = getQueryApiKey(req.url, hostOf(req));
    const apiKey = queryKey ?? headerKey;

    if (!apiKey) {
      try {
        socket.write('HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n');
      } finally {
        socket.destroy();
      }
      return;
    }

    const tenant = await authenticate(apiKey, tenantStore);
    if (!tenant) {
      try {
        socket.write('HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n');
      } finally {
        socket.destroy();
      }
      return;
    }

    const limit = wsLimitFor(tenant.tier);
    if (Number.isFinite(limit)) {
      const current = wsCounts.get(tenant.id) ?? 0;
      if (current >= limit) {
        try {
          socket.write(
            'HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\nContent-Length: 0\r\n\r\n',
          );
        } finally {
          socket.destroy();
        }
        return;
      }
      wsCounts.set(tenant.id, current + 1);
      socket.on('close', () => {
        const c = wsCounts.get(tenant.id) ?? 1;
        if (c <= 1) wsCounts.delete(tenant.id);
        else wsCounts.set(tenant.id, c - 1);
      });
    }

    const hello = JSON.stringify({ t: 'hello', derivedOnly: true });
    try {
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n',
      );
      socket.write(hello);
    } catch {
      socket.destroy();
    }
  }

  return { handleRequest, handleUpgrade };
}
