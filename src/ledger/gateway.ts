import type { IncomingMessage, ServerResponse } from 'node:http';
import { extractApiKey, authenticate, type TenantStore } from '../gateway/auth.js';
import type { RateLimiter } from '../gateway/rateLimiter.js';
import type { LedgerStore } from './store.js';
import { buildStatement, toCSV, toJSONL } from './statement.js';

export interface LedgerGatewayOptions {
  tenantStore: TenantStore;
  ledgerStore: LedgerStore;
  rateLimiter: RateLimiter;
}

function hostOf(req: IncomingMessage): string | undefined {
  const h = req.headers.host;
  if (Array.isArray(h)) return h[0];
  return h;
}

function getPathname(url: string | undefined, host: string | undefined): string {
  const raw = url ?? '/';
  try {
    return new URL(raw, `http://${host ?? 'localhost'}`).pathname;
  } catch {
    return raw.split('?')[0] ?? '/';
  }
}

function getQueryParams(url: string | undefined, host: string | undefined): URLSearchParams {
  const raw = url ?? '/';
  try {
    return new URL(raw, `http://${host ?? 'localhost'}`).searchParams;
  } catch {
    return new URLSearchParams();
  }
}

export function createLedgerGateway(options: LedgerGatewayOptions): {
  handleStatementRequest(req: IncomingMessage, res: ServerResponse): Promise<void>;
  handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void>;
} {
  const { tenantStore, ledgerStore, rateLimiter } = options;

  async function handleStatementRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const apiKey = extractApiKey(
      req as unknown as { headers: Record<string, string | string[] | undefined> },
    );
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
    const remainingHeader = String(rl.remaining);
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

    // Entitlements check: perTenantLedger read.
    // Current tier entitlements use features: F1, F2 etc., no perTenantLedger field.
    // Decision: treat all tenants as ledger-read allowed for now; document via comment.
    // If we later add perTenantLedger, check here: if (!tenant.entitlements.perTenantLedger?.read) -> 403
    // Documented: allow-all for v1.

    const params = getQueryParams(req.url, hostOf(req));
    const queryTenantId = params.get('tenantId');
    if (queryTenantId && queryTenantId !== tenant.id) {
      res.writeHead(403, {
        'content-type': 'application/json',
        'X-RateLimit-Remaining': remainingHeader,
      });
      res.end(JSON.stringify({ error: 'tenant mismatch' }));
      return;
    }
    const accountId = params.get('accountId') ?? undefined;
    const asset = params.get('asset') ?? undefined;
    if (!accountId || !asset) {
      res.writeHead(400, {
        'content-type': 'application/json',
        'X-RateLimit-Remaining': remainingHeader,
      });
      res.end(JSON.stringify({ error: 'missing accountId or asset' }));
      return;
    }

    const cursorRaw = params.get('cursor');
    const limitRaw = params.get('limit');
    const fromVersionRaw = params.get('fromVersion');
    const toVersionRaw = params.get('toVersion');
    const formatRaw = (params.get('format') ?? 'json').toLowerCase();

    let cursor: number | undefined;
    let limit: number | undefined;
    let fromVersion: number | undefined;
    let toVersion: number | undefined;

    if (cursorRaw !== null && cursorRaw !== '') {
      const n = Number(cursorRaw);
      if (!Number.isFinite(n)) {
        res.writeHead(400, {
          'content-type': 'application/json',
          'X-RateLimit-Remaining': remainingHeader,
        });
        res.end(JSON.stringify({ error: 'invalid cursor' }));
        return;
      }
      cursor = n;
    }
    if (limitRaw !== null && limitRaw !== '') {
      const n = Number(limitRaw);
      if (!Number.isFinite(n) || n <= 0) {
        res.writeHead(400, {
          'content-type': 'application/json',
          'X-RateLimit-Remaining': remainingHeader,
        });
        res.end(JSON.stringify({ error: 'invalid limit' }));
        return;
      }
      limit = Math.min(Math.floor(n), 1000);
    }
    if (fromVersionRaw !== null && fromVersionRaw !== '') {
      const n = Number(fromVersionRaw);
      if (!Number.isFinite(n)) {
        res.writeHead(400, {
          'content-type': 'application/json',
          'X-RateLimit-Remaining': remainingHeader,
        });
        res.end(JSON.stringify({ error: 'invalid fromVersion' }));
        return;
      }
      fromVersion = Math.floor(n);
    }
    if (toVersionRaw !== null && toVersionRaw !== '') {
      const n = Number(toVersionRaw);
      if (!Number.isFinite(n)) {
        res.writeHead(400, {
          'content-type': 'application/json',
          'X-RateLimit-Remaining': remainingHeader,
        });
        res.end(JSON.stringify({ error: 'invalid toVersion' }));
        return;
      }
      toVersion = Math.floor(n);
    }

    const format = formatRaw as 'json' | 'csv' | 'jsonl';
    if (!['json', 'csv', 'jsonl'].includes(format)) {
      res.writeHead(400, {
        'content-type': 'application/json',
        'X-RateLimit-Remaining': remainingHeader,
      });
      res.end(JSON.stringify({ error: 'invalid format' }));
      return;
    }

    const { rows, nextCursor } = await buildStatement(ledgerStore, tenant.id, accountId, asset, {
      cursor,
      limit,
      fromVersion,
      toVersion,
    });

    const headers: Record<string, string> = {
      'X-RateLimit-Remaining': remainingHeader,
    };
    if (nextCursor !== undefined) headers['X-Next-Cursor'] = String(nextCursor);

    if (format === 'csv') {
      const body = toCSV(rows);
      headers['content-type'] = 'text/csv';
      res.writeHead(200, headers);
      res.end(body);
      return;
    }
    if (format === 'jsonl') {
      const body = toJSONL(rows);
      headers['content-type'] = 'application/x-ndjson';
      res.writeHead(200, headers);
      res.end(body);
      return;
    }

    headers['content-type'] = 'application/json';
    res.writeHead(200, headers);
    res.end(JSON.stringify({ rows, nextCursor }));
  }

  async function handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const pathname = getPathname(req.url, hostOf(req));
    if (
      req.method === 'GET' &&
      (pathname === '/api/ledger/statement' || pathname === '/api/ledger/export')
    ) {
      await handleStatementRequest(req, res);
      return;
    }
    res.writeHead(404, {
      'content-type': 'application/json',
    });
    res.end(JSON.stringify({ error: 'not found' }));
  }

  return { handleStatementRequest, handleRequest };
}
