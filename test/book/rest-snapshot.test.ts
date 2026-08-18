import { describe, expect, it } from 'vitest';
import { parseRestSnapshot } from '../../src/book/rest-snapshot.js';

const VALID = {
  lastUpdateId: 1607756080726,
  bids: [
    ['34665.10000000', '1.20320000'],
    ['34665.00000000', '2.50000000'],
  ],
  asks: [
    ['34665.20000000', '0.90000000'],
    ['34666.10000000', '4.20100000'],
  ],
};

function clone(): Record<string, unknown> {
  return JSON.parse(JSON.stringify(VALID)) as Record<string, unknown>;
}

describe('parseRestSnapshot', () => {
  it('parses a realistic Binance /api/v3/depth payload into numeric levels', () => {
    const snapshot = parseRestSnapshot(clone());
    expect(snapshot.lastUpdateId).toBe(1607756080726);
    expect(snapshot.bids).toEqual([
      { price: 34665.1, qty: 1.2032 },
      { price: 34665, qty: 2.5 },
    ]);
    expect(snapshot.asks).toEqual([
      { price: 34665.2, qty: 0.9 },
      { price: 34666.1, qty: 4.201 },
    ]);
  });

  it('accepts empty sides', () => {
    const snapshot = parseRestSnapshot({ lastUpdateId: 1, bids: [], asks: [] });
    expect(snapshot).toEqual({ lastUpdateId: 1, bids: [], asks: [] });
  });

  it.each([
    ['null body', null],
    ['array body', []],
    ['string body', '{"lastUpdateId":1}'],
    ['number body', 42],
    ['boolean body', true],
  ])('rejects %s', (_label, body) => {
    expect(() => parseRestSnapshot(body)).toThrowError('malformed depth snapshot payload');
  });

  describe('lastUpdateId validation', () => {
    it('rejects a missing lastUpdateId', () => {
      const body = clone();
      delete body['lastUpdateId'];
      expect(() => parseRestSnapshot(body)).toThrowError('malformed depth snapshot lastUpdateId');
    });

    it.each([
      ['string id', '1607756080726'],
      ['float id', 1607756080726.5],
      ['negative id', -1],
      ['null id', null],
    ])('rejects %s', (_label, id) => {
      const body = clone();
      body['lastUpdateId'] = id;
      expect(() => parseRestSnapshot(body)).toThrowError('malformed depth snapshot lastUpdateId');
    });
  });

  describe('side validation', () => {
    it.each(['bids', 'asks'] as const)('rejects missing %s', (side) => {
      const body = clone();
      delete body[side];
      expect(() => parseRestSnapshot(body)).toThrowError(`malformed depth snapshot ${side}`);
    });

    it.each(['bids', 'asks'] as const)('rejects non-array %s', (side) => {
      for (const value of [{ 0: ['1', '1'] }, 'not-an-array', 17]) {
        const body = clone();
        body[side] = value;
        expect(() => parseRestSnapshot(body)).toThrowError(`malformed depth snapshot ${side}`);
      }
    });

    it.each(['bids', 'asks'] as const)('rejects malformed levels in %s', (side) => {
      const cases: unknown[] = [
        [['1']],
        [['1', '1', '1']],
        [[undefined, '1']],
        [['1', undefined]],
        [['abc', '1']],
        [['0', '1']],
        [['-0.001', '1']],
        [['1', '-0.5']],
        [['1', 'NaN']],
        [[true, '1']],
        [['1', null]],
        [[null, null]],
      ];
      for (const levels of cases) {
        const body = clone();
        body[side] = levels;
        expect(() => parseRestSnapshot(body)).toThrowError(
          `malformed depth snapshot level in ${side}`,
        );
      }
    });

    it('rejects bids that are not strictly descending', () => {
      const body = clone();
      body['bids'] = [
        ['100.5', '1'],
        ['100.9', '1'],
      ];
      expect(() => parseRestSnapshot(body)).toThrowError('depth snapshot bids not price-sorted');
    });

    it('rejects duplicate bid prices', () => {
      const body = clone();
      body['bids'] = [
        ['100.5', '1'],
        ['100.5', '2'],
      ];
      expect(() => parseRestSnapshot(body)).toThrowError('depth snapshot bids not price-sorted');
    });

    it('rejects asks that are not strictly ascending', () => {
      const body = clone();
      body['asks'] = [
        ['101.5', '1'],
        ['101.0', '1'],
      ];
      expect(() => parseRestSnapshot(body)).toThrowError('depth snapshot asks not price-sorted');
    });
  });
});
