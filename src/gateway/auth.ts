export class AuthError extends Error {
  statusCode = 401;
  constructor(message = 'Unauthorized') {
    super(message);
    this.name = 'AuthError';
  }
}

export interface Tenant {
  id: string;
  tier: string;
  apiKey: string;
}

export interface TenantStore {
  findByApiKey(apiKey: string): Promise<Tenant | undefined>;
}

function headerValue(
  headers: Record<string, string | string[] | undefined>,
  name: string,
): string | undefined {
  const v = headers[name] ?? headers[name.toLowerCase()];
  if (Array.isArray(v)) return v[0];
  if (typeof v === 'string') return v;
  return undefined;
}

export function extractApiKey(req: {
  headers: Record<string, string | string[] | undefined>;
}): string | undefined {
  const x = headerValue(req.headers, 'x-api-key');
  if (typeof x === 'string' && x.length > 0) return x;

  const auth = headerValue(req.headers, 'authorization');
  if (typeof auth === 'string' && auth.length > 0) {
    const m = auth.match(/^Bearer\s+(.+)$/i);
    if (m && m[1]) return m[1].trim();
  }

  return undefined;
}

export async function authenticate(
  apiKey: string,
  tenantStore: TenantStore,
): Promise<Tenant | undefined> {
  if (!apiKey) return undefined;
  return tenantStore.findByApiKey(apiKey);
}
