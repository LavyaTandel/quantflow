import { describe, expect, it } from 'vitest';
import { assessContinuity } from '../../src/book/continuity.js';
import type { DepthDiff } from '../../src/ingest/types.js';

type Status = 'applied' | 'stale' | 'gap';

function diff(firstUpdateId: number, finalUpdateId: number): DepthDiff {
  return {
    kind: 'depth',
    symbol: 'BTCUSDT',
    ts: 0,
    firstUpdateId,
    finalUpdateId,
    levels: [],
  };
}

describe('assessContinuity', () => {
  it('decides per Binance U <= last+1 <= u semantics including boundary equalities', () => {
    const cases: Array<[last: number, first: number, final: number, expected: Status]> = [
      [100, 101, 101, 'applied'],
      [100, 101, 105, 'applied'],
      [100, 100, 101, 'applied'],
      [100, 99, 101, 'applied'],
      [100, 97, 104, 'applied'],
      [0, 1, 1, 'applied'],
      [7, 8, 8, 'applied'],
      [7, 9, 9, 'gap'],
      [0, 5, 9, 'gap'],
      [100, 102, 110, 'gap'],
      [100, 102, 102, 'gap'],
      [100, 200, 250, 'gap'],
      [100, 99, 100, 'stale'],
      [100, 100, 100, 'stale'],
      [100, 50, 99, 'stale'],
      [100, 1, 99, 'stale'],
    ];
    for (const [last, first, final, expected] of cases) {
      expect(assessContinuity(last, diff(first, final))).toBe(expected);
    }
  });

  it('treats a range spanning the frontier as applicable exactly once', () => {
    expect(assessContinuity(50, diff(40, 60))).toBe('applied');
    expect(assessContinuity(59, diff(40, 60))).toBe('applied');
    expect(assessContinuity(60, diff(40, 60))).toBe('stale');
  });
});
