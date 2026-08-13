import { describe, expect, it } from 'vitest';
import { normalizeFrame, normalizeFrames } from '../../src/ingest/normalize.js';
import { isBestQuote, isDepthDiff, isTradeTick } from '../../src/ingest/types.js';

const CTX = { nowMs: 1_756_200_009_999 };

describe('normalizeFrame', () => {
  it('maps a combined aggTrade frame onto a TradeTick golden value', () => {
    const frame = {
      stream: 'btcusdt@aggTrade',
      data: {
        e: 'aggTrade',
        E: 1756200000000,
        s: 'BTCUSDT',
        a: 9001,
        p: '65000.10',
        q: '0.250',
        f: 100,
        l: 105,
        T: 1756200000000,
        m: false,
        M: true,
      },
    };
    const outcome = normalizeFrame(frame, CTX);
    expect(outcome.droppedUnknown).toBe(false);
    expect(outcome.events).toHaveLength(1);
    const event = outcome.events[0];
    expect(event).toEqual({
      kind: 'trade',
      symbol: 'BTCUSDT',
      price: 65000.1,
      qty: 0.25,
      ts: 1756200000000,
      tradeId: '9001',
    });
    expect(isTradeTick(event!)).toBe(true);
  });

  it('accepts unwrapped raw frames without a combined-stream envelope', () => {
    const frame = {
      e: 'aggTrade',
      E: 5,
      s: 'ADAUSDT',
      a: 7,
      p: '0.4500',
      q: '1000',
      T: 6,
      m: true,
      M: true,
    };
    const outcome = normalizeFrame(frame, CTX);
    expect(outcome.events[0]).toMatchObject({ symbol: 'ADAUSDT', price: 0.45, qty: 1000 });
  });

  it('maps bookTicker using transaction time when present', () => {
    const frame = {
      stream: 'ethusdt@bookTicker',
      data: {
        u: 401000220,
        s: 'ETHUSDT',
        b: '3120.10',
        B: '12.000',
        a: '3120.80',
        A: '8.500',
        T: 42,
      },
    };
    const event = normalizeFrame(frame, CTX).events[0];
    expect(event).toEqual({
      kind: 'bbo',
      symbol: 'ETHUSDT',
      bidPrice: 3120.1,
      bidQty: 12,
      askPrice: 3120.8,
      askQty: 8.5,
      ts: 42,
    });
    expect(isBestQuote(event!)).toBe(true);
  });

  it('falls back to context time when bookTicker lacks T', () => {
    const frame = {
      stream: 'ethusdt@bookTicker',
      data: { u: 1, s: 'ETHUSDT', b: '10.00', B: '1', a: '10.01', A: '2' },
    };
    expect(normalizeFrame(frame, CTX).events[0]).toMatchObject({ ts: CTX.nowMs });
  });

  it('maps depthUpdate preserving level order and zero-qty deletions', () => {
    const frame = {
      stream: 'btcusdt@depth@100ms',
      data: {
        e: 'depthUpdate',
        E: 1756200000010,
        s: 'BTCUSDT',
        U: 157,
        u: 160,
        b: [
          ['64999.50', '0.400'],
          ['64999.00', '0'],
        ],
        a: [
          ['65000.50', '1.250'],
          ['65000.60', '0'],
        ],
      },
    };
    const event = normalizeFrame(frame, CTX).events[0];
    expect(isDepthDiff(event!)).toBe(true);
    expect(event).toEqual({
      kind: 'depth',
      symbol: 'BTCUSDT',
      ts: 1756200000010,
      firstUpdateId: 157,
      finalUpdateId: 160,
      levels: [
        { side: 'bid', price: 64999.5, newQty: 0.4 },
        { side: 'bid', price: 64999.0, newQty: 0 },
        { side: 'ask', price: 65000.5, newQty: 1.25 },
        { side: 'ask', price: 65000.6, newQty: 0 },
      ],
    });
  });

  it('uses context time for depth frames missing an event timestamp', () => {
    const frame = {
      s: 'XRPUSDT',
      U: 1,
      u: 2,
      b: [['0.5', '10']],
      a: [['0.51', '20']],
    };
    expect(normalizeFrame(frame, CTX).events[0]).toMatchObject({ ts: CTX.nowMs });
  });

  it.each([
    ['null', null],
    ['number', 42],
    ['string', 'hello'],
    ['array', []],
    ['empty object', {}],
    ['envelope without data payload', { stream: 'btcusdt@aggTrade' }],
    ['unknown event type', { stream: 'btcusdt@kline_1m', data: { e: 'kline', s: 'BTCUSDT' } }],
    [
      'aggTrade with unparseable price',
      { data: { e: 'aggTrade', s: 'BTCUSDT', a: 1, p: 'abc', q: '1', T: 1 } },
    ],
    [
      'aggTrade with negative qty',
      { data: { e: 'aggTrade', s: 'BTCUSDT', a: 1, p: '1', q: '-1', T: 1 } },
    ],
    ['aggTrade missing trade id', { data: { e: 'aggTrade', s: 'BTCUSDT', p: '1', q: '1', T: 1 } }],
    [
      'bookTicker with non-string best bid',
      { data: { u: 1, s: 'BTCUSDT', b: 5, B: '1', a: '2', A: '1' } },
    ],
    [
      'bookTicker with zero ask price',
      { data: { u: 1, s: 'BTCUSDT', b: '1', B: '1', a: '0', A: '1' } },
    ],
    [
      'depthUpdate with inverted id range',
      { data: { e: 'depthUpdate', s: 'BTCUSDT', U: 10, u: 9, b: [], a: [] } },
    ],
    [
      'depthUpdate with malformed level tuple',
      { data: { e: 'depthUpdate', s: 'BTCUSDT', U: 1, u: 2, b: [['only-price']], a: [] } },
    ],
    [
      'depthUpdate with NaN level qty',
      { data: { e: 'depthUpdate', s: 'BTCUSDT', U: 1, u: 2, b: [['1', 'x']], a: [] } },
    ],
  ])('drops %s without throwing', (_label, frame) => {
    const outcome = normalizeFrame(frame, CTX);
    expect(outcome.events).toHaveLength(0);
    expect(outcome.droppedUnknown).toBe(true);
  });
});

describe('normalizeFrames', () => {
  it('aggregates events and unknown-drop counts across a batch', () => {
    const frames: unknown[] = [
      {
        stream: 'btcusdt@aggTrade',
        data: { e: 'aggTrade', s: 'BTCUSDT', a: 1, p: '1', q: '1', T: 1 },
      },
      'garbage',
      null,
      {
        stream: 'ethusdt@bookTicker',
        data: { u: 1, s: 'ETHUSDT', b: '2', B: '1', a: '3', A: '1', T: 2 },
      },
    ];
    const summary = normalizeFrames(frames, CTX);
    expect(summary.events).toHaveLength(2);
    expect(summary.droppedUnknown).toBe(2);
  });
});
