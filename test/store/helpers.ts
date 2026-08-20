import type { BestQuote, DepthDiff, NormalizedEvent, TradeTick } from '../../src/ingest/types.js';

let seq = 0;

export function mkTrade(overrides: Partial<TradeTick> = {}): TradeTick {
  seq += 1;
  return {
    kind: 'trade',
    symbol: 'BTCUSDT',
    price: 50_000 + seq,
    qty: 0.1,
    ts: 1_700_000_000_000 + seq,
    tradeId: `t${seq}`,
    ...overrides,
  };
}

export function mkBbo(overrides: Partial<BestQuote> = {}): BestQuote {
  seq += 1;
  return {
    kind: 'bbo',
    symbol: 'BTCUSDT',
    bidPrice: 49_999,
    bidQty: 1,
    askPrice: 50_001,
    askQty: 2,
    ts: 1_700_000_000_000 + seq,
    ...overrides,
  };
}

export function mkDepth(
  overrides: Partial<Omit<DepthDiff, 'kind'>> = {},
  levelCount = 3,
): DepthDiff {
  seq += 1;
  return {
    kind: 'depth',
    symbol: 'BTCUSDT',
    ts: 1_700_000_000_000 + seq,
    firstUpdateId: seq,
    finalUpdateId: seq + levelCount,
    levels: Array.from({ length: levelCount }, (_, i) => ({
      side: i % 2 === 0 ? ('bid' as const) : ('ask' as const),
      price: 50_000 - i,
      newQty: i,
    })),
    ...overrides,
  };
}

export function mkBurst(n: number): NormalizedEvent[] {
  return Array.from({ length: n }, (_, i) =>
    i % 5 === 0 ? mkDepth() : i % 3 === 0 ? mkBbo() : mkTrade(),
  );
}

export async function settle(turns = 4): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }
}
