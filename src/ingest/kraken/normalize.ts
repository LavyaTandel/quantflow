// Kraken Spot WS v2 -> NormalizedEvent normalization.
// Integrity note: Kraken's book channel has no update IDs (unlike Binance U/u);
// integrity is the per-frame CRC32 over the top 10 levels. The adapter stamps an
// internal monotonically-increasing per-symbol sequence into DepthDiff
// firstUpdateId/finalUpdateId so downstream BookStore continuity stays functional.
import { buildChecksumString, crc32, type ChecksumLevel } from './crc32.js';
import type {
  BestQuote,
  DepthDiff,
  DepthLevelChange,
  NormalizedEvent,
  TradeTick,
} from '../types.js';

export const KRAKEN_SYMBOLS: readonly string[] = [
  'BTC/USD',
  'ETH/USD',
  'SOL/USD',
  'XRP/USD',
  'DOGE/USD',
  'ADA/USD',
];

const VENUE_TO_NORMALIZED = new Map<string, string>();
for (const venue of KRAKEN_SYMBOLS) VENUE_TO_NORMALIZED.set(venue, toNormalizedSymbol(venue));
const NORMALIZED_TO_VENUE = new Map<string, string>(
  [...VENUE_TO_NORMALIZED].map(([venue, normalized]) => [normalized, venue]),
);

export function toNormalizedSymbol(venueSymbol: string): string {
  return venueSymbol.replace('/', '');
}

export function toVenueSymbol(normalizedSymbol: string): string | undefined {
  return NORMALIZED_TO_VENUE.get(normalizedSymbol);
}

export interface KrakenNormalizeContext {
  readonly nowMs: number;
}

export interface KrakenFrameOutcome {
  readonly events: readonly NormalizedEvent[];
  readonly droppedUnknown: boolean;
  readonly checksumFailed: boolean;
}

export interface KrakenChecksumFailure {
  readonly symbol: string;
  readonly expected: number;
  readonly actual: number;
}

export interface KrakenNormalizerOptions {
  depth?: number;
  onChecksumFailure?: (failure: KrakenChecksumFailure) => void;
}

export interface KrakenNormalizerCounters {
  readonly framesProcessed: number;
  readonly droppedUnknown: number;
  readonly checksumFailures: number;
}

export interface KrakenBookLevel {
  price: string;
  qty: string;
}

export interface KrakenBookEntry {
  symbol: string;
  bids: KrakenBookLevel[];
  asks: KrakenBookLevel[];
  checksum: number;
  timestamp?: string;
}

interface RawBookLevel {
  side: 'bid' | 'ask';
  priceText: string;
  qtyText: string;
  price: number;
  qty: number;
}

class ShadowBook {
  private readonly sides: Record<'bid' | 'ask', Map<number, ChecksumLevel>> = {
    bid: new Map(),
    ask: new Map(),
  };
  private readonly depth: number;

  constructor(depth: number) {
    this.depth = depth;
  }

  replace(levels: readonly RawBookLevel[]): void {
    this.sides.bid.clear();
    this.sides.ask.clear();
    for (const level of levels) this.upsert(level);
    this.truncate();
  }

  apply(levels: readonly RawBookLevel[]): void {
    for (const level of levels) this.upsert(level);
    this.truncate();
  }

  checksumString(): string {
    return buildChecksumString([...this.sides.bid.values()], [...this.sides.ask.values()]);
  }

  private upsert(level: RawBookLevel): void {
    if (level.qty === 0) {
      this.sides[level.side].delete(level.price);
      return;
    }
    this.sides[level.side].set(level.price, { price: level.priceText, qty: level.qtyText });
  }

  private truncate(): void {
    this.truncateSide(this.sides.bid, true);
    this.truncateSide(this.sides.ask, false);
  }

  private truncateSide(book: Map<number, ChecksumLevel>, keepHighest: boolean): void {
    if (book.size <= this.depth) return;
    const ascending = [...book.keys()].sort((a, b) => a - b);
    const excess = book.size - this.depth;
    const drop = keepHighest ? ascending.slice(0, excess) : ascending.slice(-excess);
    for (const price of drop) book.delete(price);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function decimalText(value: unknown): string | undefined {
  if (typeof value !== 'string' || value.length === 0) return undefined;
  if (!Number.isFinite(Number(value))) return undefined;
  return value;
}

function parseRawLevel(raw: unknown, side: 'bid' | 'ask'): RawBookLevel | undefined {
  if (!isRecord(raw)) return undefined;
  const priceText = decimalText(raw.price);
  const qtyText = decimalText(raw.qty);
  if (!priceText || !qtyText) return undefined;
  return {
    side,
    priceText,
    qtyText,
    price: Number(priceText),
    qty: Number(qtyText),
  };
}

function parseTimestampMs(data: Record<string, unknown>): number | undefined {
  if (isFiniteNumber(data.timestamp_ms) && data.timestamp_ms > 0) return data.timestamp_ms;
  if (typeof data.timestamp === 'string' && data.timestamp.length > 0) {
    const parsed = Date.parse(data.timestamp);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function parseChannel(frame: Record<string, unknown>): string | undefined {
  return typeof frame.channel === 'string' ? frame.channel : undefined;
}

function parseType(frame: Record<string, unknown>): string | undefined {
  return typeof frame.type === 'string' ? frame.type : undefined;
}

function parseDataArray(frame: Record<string, unknown>): readonly unknown[] | undefined {
  return Array.isArray(frame.data) ? frame.data : undefined;
}

function tryTradeEntry(entry: Record<string, unknown>, nowMs: number): TradeTick | undefined {
  const venueSymbol = typeof entry.symbol === 'string' ? entry.symbol : undefined;
  const priceText = decimalText(entry.price);
  const qtyText = decimalText(entry.qty);
  const tradeId =
    typeof entry.trade_id === 'string' || isFiniteNumber(entry.trade_id)
      ? String(entry.trade_id)
      : undefined;
  if (!venueSymbol || !priceText || !qtyText || !tradeId) return undefined;
  const price = Number(priceText);
  const qty = Number(qtyText);
  if (price <= 0 || qty < 0) return undefined;
  const tick: TradeTick = {
    kind: 'trade',
    symbol: toNormalizedSymbol(venueSymbol),
    price,
    qty,
    ts: parseTimestampMs(entry) ?? nowMs,
    tradeId,
  };
  return tick;
}

function tryTickerEntry(entry: Record<string, unknown>, nowMs: number): BestQuote | undefined {
  const venueSymbol = typeof entry.symbol === 'string' ? entry.symbol : undefined;
  const bidPriceText = decimalText(entry.bid);
  const bidQtyText = decimalText(entry.bid_qty);
  const askPriceText = decimalText(entry.ask);
  const askQtyText = decimalText(entry.ask_qty);
  if (!venueSymbol || !bidPriceText || !bidQtyText || !askPriceText || !askQtyText) {
    return undefined;
  }
  const bidPrice = Number(bidPriceText);
  const askPrice = Number(askPriceText);
  if (bidPrice <= 0 || askPrice <= 0) return undefined;
  const quote: BestQuote = {
    kind: 'bbo',
    symbol: toNormalizedSymbol(venueSymbol),
    bidPrice,
    bidQty: Number(bidQtyText),
    askPrice,
    askQty: Number(askQtyText),
    ts: parseTimestampMs(entry) ?? nowMs,
  };
  return quote;
}

export class KrakenNormalizer {
  private readonly depth: number;
  private readonly onChecksumFailure?: (failure: KrakenChecksumFailure) => void;
  private readonly sequences = new Map<string, number>();
  private readonly books = new Map<string, ShadowBook>();
  private framesProcessed = 0;
  private droppedUnknown = 0;
  private checksumFailures = 0;

  constructor(options: KrakenNormalizerOptions = {}) {
    this.depth = Math.max(options.depth ?? 100, 10);
    this.onChecksumFailure = options.onChecksumFailure;
  }

  counters(): KrakenNormalizerCounters {
    return {
      framesProcessed: this.framesProcessed,
      droppedUnknown: this.droppedUnknown,
      checksumFailures: this.checksumFailures,
    };
  }

  reset(): void {
    this.sequences.clear();
    this.books.clear();
    this.framesProcessed = 0;
    this.droppedUnknown = 0;
    this.checksumFailures = 0;
  }

  sequenceFor(symbol: string): number {
    return this.sequences.get(symbol) ?? 0;
  }

  normalize(frame: unknown, ctx: KrakenNormalizeContext): KrakenFrameOutcome {
    this.framesProcessed += 1;
    if (!isRecord(frame)) return this.reject();
    switch (parseChannel(frame)) {
      case 'book':
        return this.normalizeBook(frame, ctx);
      case 'trade':
        return this.normalizeTrades(frame, ctx.nowMs);
      case 'ticker':
        return this.normalizeTicker(frame, ctx.nowMs);
      default:
        return this.reject();
    }
  }

  private reject(): KrakenFrameOutcome {
    this.droppedUnknown += 1;
    return { events: [], droppedUnknown: true, checksumFailed: false };
  }

  private nextSequence(symbol: string): number {
    const next = (this.sequences.get(symbol) ?? 0) + 1;
    this.sequences.set(symbol, next);
    return next;
  }

  private normalizeBook(
    frame: Record<string, unknown>,
    ctx: KrakenNormalizeContext,
  ): KrakenFrameOutcome {
    const kind = parseType(frame);
    if (kind !== 'snapshot' && kind !== 'update') return this.reject();
    const entries = parseDataArray(frame);
    if (!entries || entries.length === 0) return this.reject();

    const events: NormalizedEvent[] = [];
    let checksumFailed = false;
    let sawValidEntry = false;

    for (const raw of entries) {
      if (!isRecord(raw)) continue;
      const venueSymbol = typeof raw.symbol === 'string' ? raw.symbol : undefined;
      const expected = raw.checksum;
      if (!venueSymbol || !isFiniteNumber(expected)) continue;
      const symbol = toNormalizedSymbol(venueSymbol);

      const bids = collectLevels(raw.bids, 'bid');
      const asks = collectLevels(raw.asks, 'ask');
      if (!bids || !asks) continue;
      sawValidEntry = true;

      let book = this.books.get(venueSymbol);
      if (!book || kind === 'snapshot') {
        book = new ShadowBook(this.depth);
        this.books.set(venueSymbol, book);
      }
      if (kind === 'snapshot') book.replace([...bids, ...asks]);
      else book.apply([...bids, ...asks]);

      const actual = crc32(book.checksumString());
      if (actual !== expected >>> 0) {
        checksumFailed = true;
        this.checksumFailures += 1;
        this.onChecksumFailure?.({ symbol: venueSymbol, expected: expected >>> 0, actual });
      }

      const seq = this.nextSequence(symbol);
      const diff: DepthDiff = {
        kind: 'depth',
        symbol,
        ts: parseTimestampMs(raw) ?? ctx.nowMs,
        firstUpdateId: seq,
        finalUpdateId: seq,
        levels: [...toChanges(bids), ...toChanges(asks)],
      };
      events.push(diff);
    }

    if (!sawValidEntry) return this.reject();
    return { events, droppedUnknown: false, checksumFailed };
  }

  private normalizeTrades(frame: Record<string, unknown>, nowMs: number): KrakenFrameOutcome {
    if (parseType(frame) !== 'snapshot' && parseType(frame) !== 'update') return this.reject();
    const entries = parseDataArray(frame);
    if (!entries || entries.length === 0) return this.reject();
    const events: NormalizedEvent[] = [];
    for (const raw of entries) {
      if (!isRecord(raw)) continue;
      const tick = tryTradeEntry(raw, nowMs);
      if (tick) events.push(tick);
    }
    if (events.length === 0) return this.reject();
    return { events, droppedUnknown: false, checksumFailed: false };
  }

  private normalizeTicker(frame: Record<string, unknown>, nowMs: number): KrakenFrameOutcome {
    if (parseType(frame) !== 'snapshot' && parseType(frame) !== 'update') return this.reject();
    const entries = parseDataArray(frame);
    if (!entries || entries.length === 0) return this.reject();
    const events: NormalizedEvent[] = [];
    for (const raw of entries) {
      if (!isRecord(raw)) continue;
      const quote = tryTickerEntry(raw, nowMs);
      if (quote) events.push(quote);
    }
    if (events.length === 0) return this.reject();
    return { events, droppedUnknown: false, checksumFailed: false };
  }
}

function collectLevels(raw: unknown, side: 'bid' | 'ask'): RawBookLevel[] | undefined {
  if (!Array.isArray(raw)) return undefined;
  const levels: RawBookLevel[] = [];
  for (const entry of raw) {
    const level = parseRawLevel(entry, side);
    if (!level) return undefined;
    levels.push(level);
  }
  return levels;
}

function toChanges(levels: readonly RawBookLevel[]): DepthLevelChange[] {
  return levels.map((level) => ({
    side: level.side,
    price: level.price,
    newQty: level.qty,
  }));
}
