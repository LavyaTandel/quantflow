import { describe, it, expect } from 'vitest';
import {
  computeVaR,
  computeSharpe,
  historicalVaR,
  createRiskEngine,
} from '../../src/scenariox/risk.js';
import type { Portfolio, Position } from '../../src/scenariox/types.js';

function makePortfolio(id = 'p1', tenantId = 't1'): Portfolio {
  return { id, tenantId, name: 'Test', createdAt: new Date('2026-01-01T00:00:00Z') };
}

function makePosition(over: Partial<Position> = {}): Position {
  return {
    id: 'pos1',
    portfolioId: 'p1',
    tenantId: 't1',
    symbol: 'BTCUSDT',
    quantity: 1,
    avgEntryPrice: 30000,
    side: 'long',
    ...over,
  };
}

describe('computeVaR deterministic', () => {
  it('known returns array -> expected var95/var99 (historical sorted percentile)', () => {
    // Returns sorted asc: -0.04, -0.03, -0.02, -0.01, 0, 0.01, 0.02, 0.03, 0.04, 0.05
    const returns = [-0.04, -0.03, -0.02, -0.01, 0, 0.01, 0.02, 0.03, 0.04, 0.05];
    // tail 5% of 10 = floor(0.05*10)=0 => idx0 => -(-0.04)=0.04
    expect(computeVaR(returns, 0.95)).toBeCloseTo(0.04, 10);
    // tail 1% of 10 = floor(0.01*10)=0 => same 0.04 (small n). Need larger n to differentiate.
    expect(computeVaR(returns, 0.99)).toBeCloseTo(0.04, 10);

    // Larger array: 100 values from -0.05 to 0.049 step 0.001 (sorted)
    const large: number[] = [];
    for (let i = 0; i < 100; i++) large.push(-0.05 + i * 0.001);
    // idx95 = floor(0.05*100)=5 -> sorted[5]= -0.045 -> var95=0.045
    expect(computeVaR(large, 0.95)).toBeCloseTo(0.045, 10);
    // idx99 = floor(0.01*100)=1 -> sorted[1]= -0.049 -> var99=0.049
    expect(computeVaR(large, 0.99)).toBeCloseTo(0.049, 10);
  });

  it('empty returns -> 0', () => {
    expect(computeVaR([], 0.95)).toBe(0);
    expect(computeVaR([], 0.99)).toBe(0);
  });
});

describe('computeSharpe deterministic', () => {
  it('known series -> expected sharpe', () => {
    const returns = [0.01, 0.02, -0.01, 0.03, 0.0];
    const sharpe = computeSharpe(returns, 0);
    // manual: mean 0.01, std ~0.0158, dailySharpe ~0.632, annualized ~10.03
    expect(sharpe).toBeGreaterThan(0);
    // deterministic: same input -> same output
    expect(computeSharpe(returns, 0)).toBe(sharpe);
    expect(computeSharpe(returns, 0.001)).toBeLessThan(sharpe);
  });

  it('zero variance -> 0', () => {
    expect(computeSharpe([0.01, 0.01, 0.01], 0)).toBe(0);
    expect(computeSharpe([], 0)).toBe(0);
    expect(computeSharpe([0.05], 0)).toBe(0);
  });
});

describe('historicalVaR', () => {
  it('engine=historical aggregates weighted returns and computes var/sharpe', () => {
    const portfolio = makePortfolio('p-hist', 't1');
    const positions: Position[] = [
      makePosition({
        id: 'pos-btc',
        symbol: 'BTCUSDT',
        quantity: 1,
        avgEntryPrice: 30000,
        side: 'long',
      }),
      makePosition({
        id: 'pos-eth',
        symbol: 'ETHUSDT',
        quantity: 10,
        avgEntryPrice: 2000,
        side: 'long',
      }),
    ];
    // BTC returns and ETH returns (5 days)
    const historicalReturns = new Map<string, number[]>([
      ['BTCUSDT', [0.01, -0.02, 0.03, -0.01, 0.02]],
      ['ETHUSDT', [0.02, -0.01, 0.01, -0.03, 0.015]],
    ]);
    const result = historicalVaR(portfolio, positions, historicalReturns, 'historical');
    expect(result.horizonDays).toBe(1);
    expect(typeof result.var95).toBe('number');
    expect(typeof result.var99).toBe('number');
    expect(typeof result.sharpe).toBe('number');
    // Deterministic: second call same
    const result2 = historicalVaR(portfolio, positions, historicalReturns, 'historical');
    expect(result).toEqual(result2);

    // Check var99 >= var95 (more extreme tail)
    expect(result.var99).toBeGreaterThanOrEqual(result.var95);
  });

  it("engine='monte-carlo' shape — same RiskResult shape, deterministic", () => {
    const portfolio = makePortfolio('p-mc', 't1');
    const positions: Position[] = [
      makePosition({ symbol: 'BTCUSDT', quantity: 1, avgEntryPrice: 30000, side: 'long' }),
    ];
    const historicalReturns = new Map<string, number[]>([
      ['BTCUSDT', [0.01, -0.02, 0.03, -0.04, 0.02, -0.01, 0.015]],
    ]);
    const hist = historicalVaR(portfolio, positions, historicalReturns, 'historical');
    const mc = historicalVaR(portfolio, positions, historicalReturns, 'monte-carlo');
    // Both return same shape; in hermetic mode they are equal (documented Monte Carlo not executed)
    expect(mc.horizonDays).toBe(1);
    expect(typeof mc.var95).toBe('number');
    expect(typeof mc.var99).toBe('number');
    expect(typeof mc.sharpe).toBe('number');
    // Shape preserved; value equality holds for hermetic deterministic fallback
    expect(mc).toEqual(hist);
  });

  it('synthetic fallback when no historical returns', () => {
    const portfolio = makePortfolio('p-synth', 't1');
    const positions: Position[] = [
      makePosition({ symbol: 'BTCUSDT', quantity: 1, avgEntryPrice: 30000, side: 'long' }),
    ];
    const empty = new Map<string, number[]>();
    const r1 = historicalVaR(portfolio, positions, empty, 'historical');
    const r2 = historicalVaR(portfolio, positions, empty, 'historical');
    expect(r1).toEqual(r2);
    expect(r1.var95).not.toBe(0);
  });

  it('short position inverts returns', () => {
    const portfolio = makePortfolio('p-short', 't1');
    const longPos = makePosition({ side: 'long', quantity: 1, avgEntryPrice: 100 });
    const shortPos = makePosition({ side: 'short', quantity: 1, avgEntryPrice: 100 });
    const rets = new Map<string, number[]>([['BTCUSDT', [0.02, -0.01, 0.03]]]);
    const rLong = historicalVaR(portfolio, [longPos], rets, 'historical');
    const rShort = historicalVaR(portfolio, [shortPos], rets, 'historical');
    // Sharpe should be opposite sign
    expect(rLong.sharpe).toBeCloseTo(-rShort.sharpe, 10);
  });
});

describe('RiskEngine worker wrapper', () => {
  it('returns same as pure (hermetic fallback determinism)', async () => {
    const engine = createRiskEngine();
    const portfolio = makePortfolio('p-worker', 't1');
    const positions: Position[] = [
      makePosition({ symbol: 'BTCUSDT', quantity: 2, avgEntryPrice: 30000, side: 'long' }),
    ];
    const historicalReturns = new Map<string, number[]>([
      ['BTCUSDT', [0.01, -0.03, 0.02, -0.02, 0.015, -0.005]],
    ]);
    const pure = historicalVaR(portfolio, positions, historicalReturns, 'historical');
    const viaWorker = await engine.calculate(portfolio, positions, historicalReturns, 'historical');
    expect(viaWorker.var95).toBeCloseTo(pure.var95, 10);
    expect(viaWorker.var99).toBeCloseTo(pure.var99, 10);
    expect(viaWorker.sharpe).toBeCloseTo(pure.sharpe, 10);
    expect(viaWorker.horizonDays).toBe(1);
    // Sync also matches
    const sync = engine.calculateSync(portfolio, positions, historicalReturns, 'historical');
    expect(sync).toEqual(pure);
    await engine.terminate();
  });
});
