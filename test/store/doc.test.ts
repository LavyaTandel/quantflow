import { describe, expect, it } from 'vitest';
import { toDoc } from '../../src/store/doc.js';
import { mkBbo, mkDepth, mkTrade } from './helpers.js';

describe('toDoc', () => {
  it('maps a trade to the golden tick document', () => {
    const trade = mkTrade({ ts: 1_700_000_000_123 });
    expect(toDoc(trade)).toEqual({
      ts: new Date(1_700_000_000_123),
      symbol: 'BTCUSDT',
      kind: 'trade',
      price: trade.price,
      qty: trade.qty,
      tradeId: trade.tradeId,
    });
  });

  it('maps a bbo to the golden tick document', () => {
    const quote = mkBbo({ symbol: 'ETHUSDT', ts: 1_700_000_000_456 });
    expect(toDoc(quote)).toEqual({
      ts: new Date(1_700_000_000_456),
      symbol: 'ETHUSDT',
      kind: 'bbo',
      bidPrice: quote.bidPrice,
      bidQty: quote.bidQty,
      askPrice: quote.askPrice,
      askQty: quote.askQty,
    });
  });

  it('summarizes depth as nLevels and never persists full levels', () => {
    const diff = mkDepth({}, 7);
    const doc = toDoc(diff);
    expect(doc).toEqual({
      ts: new Date(diff.ts),
      symbol: 'BTCUSDT',
      kind: 'depth',
      nLevels: 7,
    });
    expect(doc).not.toHaveProperty('levels');
    expect(doc).not.toHaveProperty('firstUpdateId');
    expect(Object.keys(doc)).toEqual(['ts', 'symbol', 'kind', 'nLevels']);
  });

  it('keeps identical field order across kinds (timeField/metaField first)', () => {
    for (const event of [mkTrade(), mkBbo(), mkDepth()]) {
      const keys = Object.keys(toDoc(event));
      expect(keys[0]).toBe('ts');
      expect(keys[1]).toBe('symbol');
      expect(keys[2]).toBe('kind');
    }
  });

  it('preserves millisecond precision in ts', () => {
    const doc = toDoc(mkTrade({ ts: 1_700_000_000_999 }));
    expect((doc as { ts: Date }).ts.getTime()).toBe(1_700_000_000_999);
  });
});
