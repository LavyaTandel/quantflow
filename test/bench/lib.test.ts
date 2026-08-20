import { describe, expect, it, vi } from 'vitest';
import { LatencyRecorder, PacedEmitter, makeRng, percentile } from '../../scripts/bench/lib.js';

describe('percentile', () => {
  it('returns 0 for an empty sample', () => {
    expect(percentile([], 50)).toBe(0);
  });

  it('uses nearest-rank: idx = ceil(p/100 * n) - 1', () => {
    const samples = [10, 20, 30, 40]; // n=4
    expect(percentile(samples, 25)).toBe(10); // ceil(1)-1 = idx 0
    expect(percentile(samples, 50)).toBe(20); // ceil(2)-1 = idx 1
    expect(percentile(samples, 51)).toBe(30); // ceil(2.04)-1 = idx 2
    expect(percentile(samples, 99)).toBe(40); // ceil(3.96)-1 = idx 3
    expect(percentile(samples, 100)).toBe(40); // idx 3
  });

  it('clamps p outside [0,100]', () => {
    const samples = [1, 2, 3];
    expect(percentile(samples, -5)).toBe(1);
    expect(percentile(samples, 150)).toBe(3);
  });

  it('matches the single-sample identity', () => {
    expect(percentile([42], 50)).toBe(42);
    expect(percentile([42], 99)).toBe(42);
  });
});

describe('LatencyRecorder', () => {
  it('summarizes sorted percentiles and sample count', () => {
    const recorder = new LatencyRecorder();
    for (const ms of [5, 1, 3, 2, 4]) recorder.record(ms);
    expect(recorder.summarize()).toEqual({ samples: 5, p50: 3, p99: 5, max: 5 });
  });

  it('exports raw samples in insertion order for subsampling', () => {
    const recorder = new LatencyRecorder();
    recorder.record(7);
    recorder.record(8);
    expect(recorder.exportSamples()).toEqual([7, 8]);
  });
});

describe('makeRng', () => {
  it('is deterministic for a given seed', () => {
    const a = makeRng(123);
    const b = makeRng(123);
    const seqA = Array.from({ length: 5 }, () => a());
    const seqB = Array.from({ length: 5 }, () => b());
    expect(seqA).toEqual(seqB);
  });

  it('produces values in [0,1)', () => {
    const rng = makeRng(20260826);
    for (let i = 0; i < 1000; i++) {
      const value = rng();
      expect(value).toBeGreaterThanOrEqual(0);
      expect(value).toBeLessThan(1);
    }
  });
});

describe('PacedEmitter', () => {
  it('emits batches tracking the target rate with drift correction', async () => {
    vi.useFakeTimers();
    try {
      let emitted = 0;
      const emitter = new PacedEmitter(
        1000,
        (count) => {
          emitted += count;
        },
        10,
      );
      emitter.start();
      await vi.advanceTimersByTimeAsync(100);
      // ~100ms at 1000/s → ~100 events (first tick fires immediately-ish).
      expect(emitted).toBeGreaterThan(50);
      expect(emitted).toBeLessThanOrEqual(110);
      emitter.stop();
      expect(emitter.running).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not emit after stop()', async () => {
    vi.useFakeTimers();
    try {
      let emitted = 0;
      const emitter = new PacedEmitter(
        5000,
        (count) => {
          emitted += count;
        },
        5,
      );
      emitter.start();
      await vi.advanceTimersByTimeAsync(20);
      emitter.stop();
      const frozen = emitted;
      await vi.advanceTimersByTimeAsync(100);
      expect(emitted).toBe(frozen);
    } finally {
      vi.useRealTimers();
    }
  });

  it('rejects non-positive rates', () => {
    expect(() => new PacedEmitter(0, () => {})).toThrow(RangeError);
    expect(() => new PacedEmitter(-1, () => {})).toThrow(RangeError);
  });
});
