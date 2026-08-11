export interface TradeTick {
  kind: 'trade';
  symbol: string;
  price: number;
  qty: number;
  ts: number;
  tradeId: string;
}

export interface BestQuote {
  kind: 'bbo';
  symbol: string;
  bidPrice: number;
  bidQty: number;
  askPrice: number;
  askQty: number;
  ts: number;
}

export interface DepthLevelChange {
  side: 'bid' | 'ask';
  price: number;
  newQty: number;
}

export interface DepthDiff {
  kind: 'depth';
  symbol: string;
  ts: number;
  firstUpdateId: number;
  finalUpdateId: number;
  levels: DepthLevelChange[];
}

export type NormalizedEvent = TradeTick | BestQuote | DepthDiff;

export type EventKind = NormalizedEvent['kind'];

export const EVENT_KINDS: readonly EventKind[] = ['trade', 'bbo', 'depth'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isSafeIndex(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function asDepthLevel(value: unknown): DepthLevelChange | undefined {
  if (!isRecord(value)) return undefined;
  if (value.side !== 'bid' && value.side !== 'ask') return undefined;
  if (!isFiniteNumber(value.price) || value.price <= 0) return undefined;
  if (!isFiniteNumber(value.newQty) || value.newQty < 0) return undefined;
  return { side: value.side, price: value.price, newQty: value.newQty };
}

function asTradeTick(value: Record<string, unknown>): TradeTick | undefined {
  if (!isNonEmptyString(value.symbol)) return undefined;
  if (!isFiniteNumber(value.price) || value.price <= 0) return undefined;
  if (!isFiniteNumber(value.qty) || value.qty < 0) return undefined;
  if (!isFiniteNumber(value.ts)) return undefined;
  if (!isNonEmptyString(value.tradeId)) return undefined;
  return {
    kind: 'trade',
    symbol: value.symbol,
    price: value.price,
    qty: value.qty,
    ts: value.ts,
    tradeId: value.tradeId,
  };
}

function asBestQuote(value: Record<string, unknown>): BestQuote | undefined {
  if (!isNonEmptyString(value.symbol)) return undefined;
  for (const key of ['bidPrice', 'bidQty', 'askPrice', 'askQty', 'ts'] as const) {
    if (!isFiniteNumber(value[key])) return undefined;
  }
  if ((value.bidPrice as number) <= 0 || (value.askPrice as number) <= 0) return undefined;
  if ((value.bidQty as number) < 0 || (value.askQty as number) < 0) return undefined;
  return {
    kind: 'bbo',
    symbol: value.symbol,
    bidPrice: value.bidPrice as number,
    bidQty: value.bidQty as number,
    askPrice: value.askPrice as number,
    askQty: value.askQty as number,
    ts: value.ts as number,
  };
}

function asDepthDiff(value: Record<string, unknown>): DepthDiff | undefined {
  if (!isNonEmptyString(value.symbol)) return undefined;
  if (!isFiniteNumber(value.ts)) return undefined;
  if (!isSafeIndex(value.firstUpdateId) || !isSafeIndex(value.finalUpdateId)) return undefined;
  if (!Array.isArray(value.levels)) return undefined;
  const levels: DepthLevelChange[] = [];
  for (const raw of value.levels) {
    const level = asDepthLevel(raw);
    if (!level) return undefined;
    levels.push(level);
  }
  return {
    kind: 'depth',
    symbol: value.symbol,
    ts: value.ts,
    firstUpdateId: value.firstUpdateId,
    finalUpdateId: value.finalUpdateId,
    levels,
  };
}

export function isNormalizedEvent(value: unknown): value is NormalizedEvent {
  if (!isRecord(value)) return false;
  switch (value.kind) {
    case 'trade':
      return asTradeTick(value) !== undefined;
    case 'bbo':
      return asBestQuote(value) !== undefined;
    case 'depth':
      return asDepthDiff(value) !== undefined;
    default:
      return false;
  }
}

export function isTradeTick(event: NormalizedEvent): event is TradeTick {
  return event.kind === 'trade';
}

export function isBestQuote(event: NormalizedEvent): event is BestQuote {
  return event.kind === 'bbo';
}

export function isDepthDiff(event: NormalizedEvent): event is DepthDiff {
  return event.kind === 'depth';
}
