import type { DepthDiff, DepthLevelChange, NormalizedEvent } from './types.js';

export interface NormalizeContext {
  readonly nowMs: number;
}

export interface FrameOutcome {
  readonly events: readonly NormalizedEvent[];
  readonly droppedUnknown: boolean;
}

export interface NormalizeSummary {
  readonly events: NormalizedEvent[];
  readonly droppedUnknown: number;
}

interface CombinedFrame {
  stream: string;
  data: Record<string, unknown>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isCombinedFrame(value: unknown): value is CombinedFrame {
  if (!isRecord(value)) return false;
  if (typeof value.stream !== 'string' || value.stream.length === 0) return false;
  return isRecord(value.data);
}

function safeInt(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function decimalField(
  data: Record<string, unknown>,
  key: string,
  min: 'positive' | 'nonNegative',
): number | undefined {
  const raw = data[key];
  if (typeof raw !== 'string' || raw.length === 0) return undefined;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return undefined;
  if (min === 'positive' ? parsed <= 0 : parsed < 0) return undefined;
  return parsed;
}

function parseLevels(raw: unknown, side: DepthLevelChange['side']): DepthLevelChange[] | undefined {
  if (!Array.isArray(raw)) return undefined;
  const levels: DepthLevelChange[] = [];
  for (const entry of raw) {
    if (!Array.isArray(entry) || entry.length !== 2) return undefined;
    const price = decimalField({ p: entry[0] }, 'p', 'positive');
    const newQty = decimalField({ q: entry[1] }, 'q', 'nonNegative');
    if (price === undefined || newQty === undefined) return undefined;
    levels.push({ side, price, newQty });
  }
  return levels;
}

function tryAggTrade(data: Record<string, unknown>): NormalizedEvent[] | undefined {
  if (data.e !== 'aggTrade') return undefined;
  const symbol = nonEmptyString(data.s);
  const price = decimalField(data, 'p', 'positive');
  const qty = decimalField(data, 'q', 'nonNegative');
  const ts = safeInt(data.T);
  const tradeId = safeInt(data.a);
  if (
    !symbol ||
    price === undefined ||
    qty === undefined ||
    ts === undefined ||
    tradeId === undefined
  ) {
    return [];
  }
  const tick = { kind: 'trade', symbol, price, qty, ts, tradeId: String(tradeId) } as const;
  return [Object.freeze(tick) as NormalizedEvent];
}

function tryDepthUpdate(
  data: Record<string, unknown>,
  ctx: NormalizeContext,
): NormalizedEvent[] | undefined {
  if (data.e !== undefined && data.e !== 'depthUpdate') return undefined;
  if (typeof data.b === 'string' || typeof data.a === 'string') return undefined;
  const symbol = nonEmptyString(data.s);
  const firstUpdateId = safeInt(data.U);
  const finalUpdateId = safeInt(data.u);
  const bids = parseLevels(data.b, 'bid');
  const asks = parseLevels(data.a, 'ask');
  if (
    !symbol ||
    firstUpdateId === undefined ||
    finalUpdateId === undefined ||
    firstUpdateId > finalUpdateId ||
    bids === undefined ||
    asks === undefined
  ) {
    return [];
  }
  const eventTime = safeInt(data.E);
  const diff: DepthDiff = {
    kind: 'depth',
    symbol,
    ts: eventTime ?? ctx.nowMs,
    firstUpdateId,
    finalUpdateId,
    levels: [...bids, ...asks],
  };
  return [diff];
}

function tryBookTicker(
  data: Record<string, unknown>,
  ctx: NormalizeContext,
): NormalizedEvent[] | undefined {
  if (typeof data.b !== 'string') return undefined;
  const symbol = nonEmptyString(data.s);
  if (!symbol || safeInt(data.u) === undefined) return [];
  const bidPrice = decimalField(data, 'b', 'positive');
  const bidQty = decimalField(data, 'B', 'nonNegative');
  const askPrice = decimalField(data, 'a', 'positive');
  const askQty = decimalField(data, 'A', 'nonNegative');
  if (
    bidPrice === undefined ||
    bidQty === undefined ||
    askPrice === undefined ||
    askQty === undefined
  ) {
    return [];
  }
  const transactionTime = safeInt(data.T);
  const quote = {
    kind: 'bbo',
    symbol,
    bidPrice,
    bidQty,
    askPrice,
    askQty,
    ts: transactionTime ?? ctx.nowMs,
  } as const;
  return [Object.freeze(quote) as NormalizedEvent];
}

function unwrap(frame: unknown): Record<string, unknown> | undefined {
  if (isCombinedFrame(frame)) return frame.data;
  if (isRecord(frame)) return frame;
  return undefined;
}

export function normalizeFrame(frame: unknown, ctx: NormalizeContext): FrameOutcome {
  const data = unwrap(frame);
  if (!data) return { events: [], droppedUnknown: true };
  const events = tryAggTrade(data) ?? tryDepthUpdate(data, ctx) ?? tryBookTicker(data, ctx) ?? [];
  return { events, droppedUnknown: events.length === 0 };
}

export function normalizeFrames(
  frames: readonly unknown[],
  ctx: NormalizeContext,
): NormalizeSummary {
  const events: NormalizedEvent[] = [];
  let droppedUnknown = 0;
  for (const frame of frames) {
    const outcome = normalizeFrame(frame, ctx);
    for (const event of outcome.events) events.push(event);
    if (outcome.droppedUnknown) droppedUnknown += 1;
  }
  return { events, droppedUnknown };
}
