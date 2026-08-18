import type { BookSnapshot } from './bootstrap.js';
import type { Level } from './store.js';

export interface RestSnapshotOptions {
  baseUrl?: string;
  limit?: number;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
}

const DEFAULT_BASE_URL = 'https://api.binance.com';
const DEFAULT_LIMIT = 5000;

export async function fetchRestSnapshot(
  symbol: string,
  options: RestSnapshotOptions = {},
): Promise<BookSnapshot> {
  const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
  const limit = options.limit ?? DEFAULT_LIMIT;
  const url = `${baseUrl}/api/v3/depth?symbol=${encodeURIComponent(symbol)}&limit=${limit}`;
  const doFetch = options.fetchImpl ?? fetch;
  const response = await doFetch(url, { signal: options.signal });
  if (!response.ok) {
    throw new Error(`depth snapshot request failed for ${symbol}: HTTP ${response.status}`);
  }
  return parseRestSnapshot(await response.json());
}

function parseLevel(entry: unknown, side: string): Level {
  if (!Array.isArray(entry) || entry.length !== 2) {
    throw new Error(`malformed depth snapshot level in ${side}`);
  }
  const [rawPrice, rawQty] = entry;
  // Guard against Number() coercion holes: true -> 1, null -> 0, '' -> 0.
  if (typeof rawPrice !== 'string' && typeof rawPrice !== 'number') {
    throw new Error(`malformed depth snapshot level in ${side}`);
  }
  if (typeof rawQty !== 'string' && typeof rawQty !== 'number') {
    throw new Error(`malformed depth snapshot level in ${side}`);
  }
  const price = Number(rawPrice);
  const qty = Number(rawQty);
  if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(qty) || qty < 0) {
    throw new Error(`malformed depth snapshot level in ${side}`);
  }
  return { price, qty };
}

function parseSide(value: unknown, side: string, descending: boolean): Level[] {
  if (!Array.isArray(value)) {
    throw new Error(`malformed depth snapshot ${side}`);
  }
  const levels = value.map((entry) => parseLevel(entry, side));
  for (let i = 1; i < levels.length; i++) {
    const previous = levels[i - 1]!.price;
    const current = levels[i]!.price;
    if (descending ? previous <= current : previous >= current) {
      throw new Error(`depth snapshot ${side} not price-sorted`);
    }
  }
  return levels;
}

export function parseRestSnapshot(body: unknown): BookSnapshot {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    throw new Error('malformed depth snapshot payload');
  }
  const record = body as Record<string, unknown>;
  const lastUpdateId = record['lastUpdateId'];
  if (typeof lastUpdateId !== 'number' || !Number.isSafeInteger(lastUpdateId) || lastUpdateId < 0) {
    throw new Error('malformed depth snapshot lastUpdateId');
  }
  return {
    lastUpdateId,
    bids: parseSide(record['bids'], 'bids', true),
    asks: parseSide(record['asks'], 'asks', false),
  };
}
