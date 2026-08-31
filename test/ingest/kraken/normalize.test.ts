import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  KRAKEN_SYMBOLS,
  KrakenNormalizer,
  toNormalizedSymbol,
  toVenueSymbol,
  type KrakenChecksumFailure,
} from '../../../src/ingest/kraken/normalize.js';
import {
  isDepthDiff,
  isNormalizedEvent,
  isTradeTick,
  type NormalizedEvent,
} from '../../../src/ingest/types.js';

const NOW_MS = Date.parse('2026-08-26T00:00:00.000Z');

function fixtureLine(name: string, line: number): unknown {
  const path = fileURLToPath(new URL(`../../fixtures/kraken/${name}`, import.meta.url));
  return JSON.parse(readFileSync(path, 'utf8').split('\n')[line]!);
}

const BTC_SNAPSHOT = () => fixtureLine('book-two-symbols.jsonl', 0);
const ETH_SNAPSHOT = () => fixtureLine('book-two-symbols.jsonl', 1);
const BTC_UPDATE_1 = () => fixtureLine('book-two-symbols.jsonl', 2);
const ETH_UPDATE_BATCHED = () => fixtureLine('book-two-symbols.jsonl', 3);
const BTC_UPDATE_2 = () => fixtureLine('book-two-symbols.jsonl', 4);
const CORRUPTED_SNAPSHOT = () => fixtureLine('book-corrupted-checksum.jsonl', 0);
const CORRUPTED_UPDATE = () => fixtureLine('book-corrupted-checksum.jsonl', 1);

function kinds(events: readonly NormalizedEvent[]): string[] {
  return events.map((event) => event.kind);
}

describe('KrakenNormalizer symbols', () => {
  it('maps the venue universe both directions', () => {
    expect(KRAKEN_SYMBOLS).toEqual([
      'BTC/USD',
      'ETH/USD',
      'SOL/USD',
      'XRP/USD',
      'DOGE/USD',
      'ADA/USD',
    ]);
    expect(toNormalizedSymbol('BTC/USD')).toBe('BTCUSD');
    expect(toVenueSymbol('BTCUSD')).toBe('BTC/USD');
    expect(toVenueSymbol('NOPE')).toBeUndefined();
    expect(toNormalizedSymbol('FOO/BAR')).toBe('FOOBAR');
  });
});

describe('KrakenNormalizer book channel', () => {
  it('normalizes a snapshot into a stamped DepthDiff with absolute quantities', () => {
    const normalizer = new KrakenNormalizer();
    const outcome = normalizer.normalize(BTC_SNAPSHOT(), { nowMs: NOW_MS });

    expect(outcome.droppedUnknown).toBe(false);
    expect(outcome.checksumFailed).toBe(false);
    expect(outcome.events).toHaveLength(1);
    const diff = outcome.events[0]!;
    expect(isDepthDiff(diff)).toBe(true);
    if (!isDepthDiff(diff)) return;
    expect(diff.symbol).toBe('BTCUSD');
    expect(diff.firstUpdateId).toBe(1);
    expect(diff.finalUpdateId).toBe(1);
    expect(diff.ts).toBe(Date.parse('2026-08-26T12:00:00.000Z'));
    expect(diff.levels).toHaveLength(20);
    expect(diff.levels[0]).toEqual({ side: 'bid', price: 45283.5, newQty: 0.1 });
    expect(diff.levels[10]).toEqual({ side: 'ask', price: 45285.2, newQty: 0.001 });
  });

  it('stamps a strictly increasing per-symbol sequence across updates and keeps symbols independent', () => {
    const normalizer = new KrakenNormalizer();
    for (const frame of [BTC_SNAPSHOT(), BTC_UPDATE_1(), BTC_UPDATE_2()]) {
      const outcome = normalizer.normalize(frame, { nowMs: NOW_MS });
      expect(outcome.events).toHaveLength(1);
    }
    expect(normalizer.sequenceFor('BTCUSD')).toBe(3);

    const ethOutcome = normalizer.normalize(ETH_SNAPSHOT(), { nowMs: NOW_MS });
    expect((ethOutcome.events[0] as { firstUpdateId: number }).firstUpdateId).toBe(1);
    expect(normalizer.counters().checksumFailures).toBe(0);
  });

  it('maps qty 0 updates to delete semantics (newQty 0) and preserves batched same-level changes', () => {
    const normalizer = new KrakenNormalizer();
    normalizer.normalize(ETH_SNAPSHOT(), { nowMs: NOW_MS });
    const outcome = normalizer.normalize(ETH_UPDATE_BATCHED(), { nowMs: NOW_MS });
    const diff = outcome.events[0]!;
    if (!isDepthDiff(diff)) return;

    // two batched bid entries at the same price + one ask deletion
    expect(diff.levels).toHaveLength(3);
    expect(diff.levels[0]).toEqual({ side: 'bid', price: 2450, newQty: 2 });
    expect(diff.levels[1]).toEqual({ side: 'bid', price: 2450, newQty: 1.5 });
    expect(diff.levels[2]).toEqual({ side: 'ask', price: 2451.45, newQty: 0 });
    expect(normalizer.counters().checksumFailures).toBe(0);
  });

  it('flags corrupted checksums via counter and callback while still emitting valid events', () => {
    const failures: KrakenChecksumFailure[] = [];
    const normalizer = new KrakenNormalizer({ onChecksumFailure: (f) => failures.push(f) });

    const snapshotOutcome = normalizer.normalize(CORRUPTED_SNAPSHOT(), { nowMs: NOW_MS });
    expect(snapshotOutcome.checksumFailed).toBe(true);
    expect(snapshotOutcome.events).toHaveLength(1);
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ symbol: 'SOL/USD' });
    expect(failures[0]!.expected).not.toBe(failures[0]!.actual);

    const updateOutcome = normalizer.normalize(CORRUPTED_UPDATE(), { nowMs: NOW_MS });
    expect(updateOutcome.checksumFailed).toBe(true);
    expect(updateOutcome.events).toHaveLength(1);
    expect(failures).toHaveLength(2);
    expect(normalizer.counters().checksumFailures).toBe(2);
  });

  it('re-seeds integrity state from a fresh mid-stream snapshot', () => {
    const normalizer = new KrakenNormalizer();
    normalizer.normalize(BTC_SNAPSHOT(), { nowMs: NOW_MS });
    normalizer.normalize(BTC_UPDATE_1(), { nowMs: NOW_MS });
    // re-snapshot with the pre-update levels: checksum must validate against the reset book
    const outcome = normalizer.normalize(BTC_SNAPSHOT(), { nowMs: NOW_MS });
    expect(outcome.checksumFailed).toBe(false);
    expect(normalizer.counters().checksumFailures).toBe(0);
  });

  it('rejects malformed book frames without throwing', () => {
    const normalizer = new KrakenNormalizer();
    const cases = [
      null,
      42,
      'nope',
      {},
      { channel: 'book', type: 'snapshot' },
      { channel: 'book', type: 'snapshot', data: [] },
      {
        channel: 'book',
        type: 'nonsense',
        data: [{ symbol: 'BTC/USD', bids: [], asks: [], checksum: 1 }],
      },
      {
        channel: 'book',
        type: 'snapshot',
        data: [{ symbol: 'BTC/USD', bids: [['x']], asks: [], checksum: 1 }],
      },
      {
        channel: 'book',
        type: 'snapshot',
        data: [{ symbol: 'BTC/USD', bids: [], asks: [], checksum: 'x' }],
      },
    ];
    let dropped = 0;
    for (const frame of cases) {
      const outcome = normalizer.normalize(frame, { nowMs: NOW_MS });
      expect(outcome.events).toEqual([]);
      dropped += outcome.droppedUnknown ? 1 : 0;
    }
    expect(dropped).toBe(cases.length);
    expect(normalizer.counters().droppedUnknown).toBe(cases.length);
  });

  it('counts subscribe acknowledgements as dropped-unknown non-data frames', () => {
    const normalizer = new KrakenNormalizer();
    const outcome = normalizer.normalize(
      { method: 'subscribe', success: true, data: { channel: 'book' } },
      { nowMs: NOW_MS },
    );
    expect(outcome.droppedUnknown).toBe(true);
    expect(normalizer.counters().droppedUnknown).toBe(1);
  });
});

describe('KrakenNormalizer trade and ticker channels', () => {
  const TRADE_FRAME = {
    channel: 'trade',
    type: 'update',
    data: [
      {
        symbol: 'BTC/USD',
        side: 'buy',
        qty: '0.00010000',
        price: '45285.2',
        ord_type: 'market',
        trade_id: 777,
        timestamp: '2026-08-26T12:00:04.500000Z',
      },
      {
        symbol: 'ETH/USD',
        side: 'sell',
        qty: '2.5',
        price: '2450.10',
        ord_type: 'limit',
        trade_id: 888,
        timestamp_ms: Date.parse('2026-08-26T12:00:05.000Z'),
      },
    ],
  };

  const TICKER_FRAME = {
    channel: 'ticker',
    type: 'snapshot',
    data: [
      {
        symbol: 'XRP/USD',
        bid: '0.5810',
        bid_qty: '1500.0',
        ask: '0.5812',
        ask_qty: '2200.0',
        timestamp: '2026-08-26T12:00:06.000000Z',
      },
    ],
  };

  it('maps trades into TradeTick events with normalized symbols', () => {
    const normalizer = new KrakenNormalizer();
    const outcome = normalizer.normalize(TRADE_FRAME, { nowMs: NOW_MS });
    expect(kinds(outcome.events)).toEqual(['trade', 'trade']);
    for (const event of outcome.events) expect(isTradeTick(event)).toBe(true);
    const first = outcome.events[0]!;
    const second = outcome.events[1]!;
    if (!isTradeTick(first) || !isTradeTick(second)) throw new Error('expected trade ticks');
    expect(first).toMatchObject({
      kind: 'trade',
      symbol: 'BTCUSD',
      price: 45285.2,
      qty: 0.0001,
      tradeId: '777',
      ts: Date.parse('2026-08-26T12:00:04.500Z'),
    });
    expect(second).toMatchObject({ symbol: 'ETHUSD', tradeId: '888' });
  });

  it('maps ticker snapshots into BestQuote events', () => {
    const normalizer = new KrakenNormalizer();
    const outcome = normalizer.normalize(TICKER_FRAME, { nowMs: NOW_MS });
    expect(kinds(outcome.events)).toEqual(['bbo']);
    expect(outcome.events[0]).toMatchObject({
      kind: 'bbo',
      symbol: 'XRPUSD',
      bidPrice: 0.581,
      bidQty: 1500,
      askPrice: 0.5812,
      askQty: 2200,
      ts: Date.parse('2026-08-26T12:00:06.000Z'),
    });
  });

  it('falls back to ctx.nowMs when timestamps are absent or unparseable', () => {
    const normalizer = new KrakenNormalizer();
    const outcome = normalizer.normalize(
      {
        channel: 'trade',
        type: 'update',
        data: [
          { symbol: 'BTC/USD', side: 'buy', qty: '1', price: '10', trade_id: 1 },
          {
            symbol: 'BTC/USD',
            side: 'buy',
            qty: '1',
            price: '10',
            trade_id: 2,
            timestamp: 'garbage',
          },
          { symbol: 'BTC/USD', side: 'buy', qty: '1', price: 'bad', trade_id: 3 },
        ],
      },
      { nowMs: NOW_MS },
    );
    expect(outcome.droppedUnknown).toBe(false);
    expect(outcome.events).toHaveLength(2);
    expect(outcome.events.every((event) => event.kind === 'trade' && event.ts === NOW_MS)).toBe(
      true,
    );
  });

  it('drops malformed trade/ticker envelopes as unknown frames', () => {
    const normalizer = new KrakenNormalizer();
    const cases = [
      { channel: 'trade', type: 'update', data: [{ symbol: 'BTC/USD' }] },
      { channel: 'trade', type: 'update', data: 'nope' },
      { channel: 'ticker', type: 'update', data: [{ symbol: 'XRP/USD', bid: '1' }] },
      { channel: 'ohlc', type: 'update', data: [] },
    ];
    for (const frame of cases) {
      expect(normalizer.normalize(frame, { nowMs: NOW_MS }).droppedUnknown).toBe(true);
    }
    expect(normalizer.counters().droppedUnknown).toBe(cases.length);
  });
});

describe('KrakenNormalizer downstream contract', () => {
  it('emits only events that satisfy the venue-neutral guards', () => {
    const normalizer = new KrakenNormalizer();
    const frames = [BTC_SNAPSHOT(), ETH_SNAPSHOT(), BTC_UPDATE_1(), ETH_UPDATE_BATCHED()];
    const events: NormalizedEvent[] = [];
    for (const frame of frames) {
      events.push(...normalizer.normalize(frame, { nowMs: NOW_MS }).events);
    }
    expect(events).toHaveLength(4);
    for (const event of events) expect(isNormalizedEvent(event)).toBe(true);
  });
});
