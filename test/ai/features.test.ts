import { describe, expect, it } from 'vitest';
import {
  computeImbalance,
  computeOFI,
  detectSweep,
  computeDWP,
  buildFeatureVector,
  imbalanceFromBook,
} from '../../src/ai/features.js';
import type { BookSide } from '../../src/book/store.js';

function book(bids: Array<[number, number]>, asks: Array<[number, number]>): BookSide {
  return {
    bids: bids.map(([price, qty]) => ({ price, qty })),
    asks: asks.map(([price, qty]) => ({ price, qty })),
  };
}

describe('computeImbalance', () => {
  it('computes I = (Qb - Qa)/(Qb+Qa) in [-1,1]', () => {
    expect(computeImbalance(120, 60)).toBeCloseTo((120 - 60) / 180); // 0.333
    expect(computeImbalance(60, 120)).toBeCloseTo(-0.333, 3);
    expect(computeImbalance(100, 100)).toBe(0);
    expect(computeImbalance(100, 0)).toBe(1);
    expect(computeImbalance(0, 100)).toBe(-1);
  });

  it('handles empty book zero depth => 0', () => {
    expect(computeImbalance(0, 0)).toBe(0);
  });

  it('handles one-sided and zero depth', () => {
    expect(computeImbalance(0, 50)).toBe(-1);
    expect(computeImbalance(50, 0)).toBe(1);
    expect(computeImbalance(0, 0)).toBe(0);
  });

  it('clamps non-finite to 0 and <1ms pure', () => {
    expect(computeImbalance(NaN, 10)).toBe(0);
    expect(computeImbalance(10, Infinity)).toBe(0);
    const s = performance.now();
    for (let i = 0; i < 1000; i++) computeImbalance(100, 50);
    expect(performance.now() - s).toBeLessThan(10);
  });

  it('imbalanceFromBook sums quantities', () => {
    const b = book(
      [
        [100, 2],
        [99, 1],
      ],
      [[101, 1]],
    );
    // Qb=3, Qa=1 => I=0.5
    expect(imbalanceFromBook(b)).toBeCloseTo(0.5);
    expect(imbalanceFromBook(book([], []))).toBe(0);
  });
});

describe('computeOFI', () => {
  it('returns 0 for null prev', () => {
    expect(computeOFI(null, book([[100, 10]], [[101, 10]]))).toBe(0);
  });

  it('handles empty books both sides => 0', () => {
    expect(computeOFI(book([], []), book([], []))).toBe(0);
  });

  it('one-sided: bid appears => +qty, ask appears => -qty (OFI = Ib - Ia)', () => {
    // prev empty, curr has bid 5 => Ib=5, Ia=0 => OFI=5
    expect(computeOFI(book([], []), book([[100, 5]], []))).toBe(5);
    // prev empty, curr has ask 5 => Ib=0, Ia=5 => OFI=-5
    expect(computeOFI(book([], []), book([], [[101, 5]]))).toBe(-5);
  });

  it('bid price up => +currQty, bid price down => -prevQty', () => {
    const prev = book([[100, 10]], [[101, 10]]);
    const up = book([[101, 7]], [[101, 10]]);
    expect(computeOFI(prev, up)).toBe(7); // Ib=7, Ia=0 =>7
    const down = book([[99, 10]], [[101, 10]]);
    // Ib = -10, Ia=0 => -10
    expect(computeOFI(prev, down)).toBe(-10);
  });

  it('bid unchanged qty delta => curr-prev', () => {
    const prev = book([[100, 10]], [[101, 10]]);
    const curr = book([[100, 15]], [[101, 10]]);
    expect(computeOFI(prev, curr)).toBe(5);
    const curr2 = book([[100, 5]], [[101, 10]]);
    expect(computeOFI(prev, curr2)).toBe(-5);
  });

  it('ask mirrored: ask down => +currQty (bearish), ask up => -prevQty (bullish)', () => {
    const prev = book([[100, 10]], [[101, 10]]);
    // ask down from 101 to 100.5 with qty 8 => Ia=8 => OFI=-8
    const down = book([[100, 10]], [[100.5, 8]]);
    expect(computeOFI(prev, down)).toBe(-8);
    // ask up from 101 to 102 with qty 6 => Ia=-10 => OFI= +10
    const up = book([[100, 10]], [[102, 6]]);
    expect(computeOFI(prev, up)).toBe(10);
  });

  it('ask unchanged qty delta', () => {
    const prev = book([[100, 10]], [[101, 10]]);
    const curr = book([[100, 10]], [[101, 20]]);
    // Ia=10 => OFI=-10
    expect(computeOFI(prev, curr)).toBe(-10);
  });

  it('bid and ask both move combine => Ib - Ia', () => {
    const prev = book([[100, 10]], [[101, 10]]);
    const curr = book([[101, 5]], [[102, 5]]); // Ib=5, Ia=-10 => OFI=15
    expect(computeOFI(prev, curr)).toBe(15);
  });

  it('large OFI bucket magnitude correlates with pressure', () => {
    // Simulate net buying: bid qty up, ask qty down
    const prev = book([[100, 5]], [[101, 10]]);
    const curr = book([[100, 15]], [[101, 2]]); // Ib=+10, Ia=-8 => OFI=18
    // Ia = 2-10=-8, OFI=10 - (-8)=18
    expect(computeOFI(prev, curr)).toBe(18);
  });
});

describe('detectSweep', () => {
  it('empty book => sweep (depletion)', () => {
    expect(detectSweep(book([], [[101, 10]]), 0.5)).toBe(true);
    expect(detectSweep(book([[100, 10]], []), 0.5)).toBe(true);
    expect(detectSweep(book([], []), 1)).toBe(true);
  });

  it('zero depth totals => sweep', () => {
    expect(detectSweep(book([[100, 0]], [[101, 0]]), 0.1)).toBe(true);
  });

  it('normal spread not sweep', () => {
    // mid 100.5 spread 0.1 => 9.9 bps <100
    expect(detectSweep(book([[100, 10]], [[101, 10]]), 0.1)).toBe(false);
    // spread 1 => ~99 bps <100 => not sweep
    expect(detectSweep(book([[100, 10]], [[101, 10]]), 1)).toBe(false);
  });

  it('spread widen beyond 100 bps => sweep', () => {
    const b = book([[100, 10]], [[101, 10]]);
    // mid 100.5, spread 1.2 => 119 bps => true
    expect(detectSweep(b, 1.2)).toBe(true);
    expect(detectSweep(b, 0.1)).toBe(false);
  });

  it('one-sided ratio alone does not flag sweep when spread tight', () => {
    // totalBid 10, totalAsk 1 => imbalance but not sweep (needs spread widen)
    expect(detectSweep(book([[100, 10]], [[101, 1]]), 0.1)).toBe(false);
    expect(detectSweep(book([[100, 1]], [[101, 10]]), 0.1)).toBe(false);
    // balanced not sweep
    expect(detectSweep(book([[100, 10]], [[101, 10]]), 0.1)).toBe(false);
  });

  it('Level[] overload: empty => sweep', () => {
    expect(detectSweep([], 1)).toBe(true);
    expect(detectSweep([{ price: 100, qty: 1 }], 0.1)).toBe(false);
  });
});

describe('computeDWP', () => {
  it('empty book => 0', () => {
    expect(computeDWP(book([], []))).toBe(0);
    expect(computeDWP(book([], []), 5)).toBe(0);
  });

  it('one-sided: bid only => +1, ask only => -1', () => {
    expect(computeDWP(book([[100, 10]], []), 5)).toBeCloseTo(1);
    expect(computeDWP(book([], [[101, 10]]), 5)).toBeCloseTo(-1);
  });

  it('balanced top5 => 0', () => {
    const b = book(
      [
        [100, 10],
        [99, 10],
        [98, 10],
        [97, 10],
        [96, 10],
      ],
      [
        [101, 10],
        [102, 10],
        [103, 10],
        [104, 10],
        [105, 10],
      ],
    );
    expect(computeDWP(b, 5)).toBeCloseTo(0);
  });

  it('depth-weighted pressure with exponential decay favors near levels', () => {
    // Near bid heavy, far ask modestly heavy => DWP positive (near weight dominates)
    const b = book(
      [
        [100, 20],
        [99, 1],
        [98, 1],
        [97, 1],
        [96, 1],
      ],
      [
        [101, 1],
        [102, 5],
        [103, 5],
        [104, 5],
        [105, 5],
      ],
    );
    expect(computeDWP(b, 5)).toBeGreaterThan(0);
    // Opposite
    const b2 = book(
      [
        [100, 1],
        [99, 5],
        [98, 5],
        [97, 5],
        [96, 5],
      ],
      [
        [101, 20],
        [102, 1],
        [103, 1],
        [104, 1],
        [105, 1],
      ],
    );
    expect(computeDWP(b2, 5)).toBeLessThan(0);
  });

  it('zero depth at all levels => 0', () => {
    expect(computeDWP(book([[100, 0]], [[101, 0]]))).toBe(0);
  });

  it('K=5 clamped and <1ms', () => {
    const b = book(
      [
        [100, 5],
        [99, 5],
      ],
      [[101, 5]],
    );
    const s = performance.now();
    for (let i = 0; i < 1000; i++) computeDWP(b, 5);
    expect(performance.now() - s).toBeLessThan(10);
    expect(computeDWP(b, 0)).toBe(0);
  });
});

describe('buildFeatureVector', () => {
  it('builds FeatureVector with all fields <1ms', () => {
    const curr = book([[100, 10]], [[101, 5]]);
    const prev = book([[100, 8]], [[101, 7]]);
    const s = performance.now();
    const fv = buildFeatureVector(curr, prev, { windowMs: 100, spread: 1 });
    expect(performance.now() - s).toBeLessThan(5);
    expect(fv.I).toBeCloseTo((10 - 5) / 15); // qb 10, qa 5 => I=0.333
    expect(typeof fv.ofi).toBe('number');
    expect(typeof fv.dwp).toBe('number');
    expect(typeof fv.sweepFlag).toBe('boolean');
    expect(fv.windowMs).toBe(100);
  });
});
