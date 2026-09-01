import { describe, expect, it } from 'vitest';
import { FakeModel, LlamaCppModel } from '../../src/ai/model.js';
import type { FeatureVector } from '../../src/ai/features.js';

function fv(over: Partial<FeatureVector> = {}): FeatureVector {
  return {
    I: 0,
    ofi: 0,
    dwp: 0,
    sweepFlag: false,
    windowMs: 100,
    ...over,
  };
}

describe('FakeModel', () => {
  it('lean = sign(I) if |I|>0.3 else sign(ofi)', async () => {
    const m = new FakeModel({ latencyMs: 0 });
    expect((await m.classify(fv({ I: 0.5, ofi: 0 }))).lean).toBe('buy');
    expect((await m.classify(fv({ I: -0.5, ofi: 0 }))).lean).toBe('sell');
    expect((await m.classify(fv({ I: 0.1, ofi: 20 }))).lean).toBe('buy');
    expect((await m.classify(fv({ I: 0.1, ofi: -20 }))).lean).toBe('sell');
    expect((await m.classify(fv({ I: 0, ofi: 0 }))).lean).toBe('neutral');
    expect((await m.classify(fv({ I: 0.3, ofi: 0 }))).lean).toBe('neutral'); // boundary not >0.3
    expect((await m.classify(fv({ I: 0.31, ofi: 0 }))).lean).toBe('buy');
  });

  it('confidence = |I| and score = I', async () => {
    const m = new FakeModel({ latencyMs: 0 });
    const r1 = await m.classify(fv({ I: 0.8, ofi: 0 }));
    expect(r1.confidence).toBeCloseTo(0.8);
    expect(r1.score).toBeCloseTo(0.8);
    const r2 = await m.classify(fv({ I: -0.2, ofi: -10 }));
    expect(r2.confidence).toBeCloseTo(0.2);
    expect(r2.score).toBeCloseTo(-0.2);
    const r3 = await m.classify(fv({ I: 0, ofi: 5 }));
    expect(r3.confidence).toBe(0);
  });

  it('latency configurable 5-15 ms deterministic', async () => {
    const fast = new FakeModel({ latencyMs: 5 });
    const t0 = Date.now();
    await fast.classify(fv({ I: 0.5 }));
    const d0 = Date.now() - t0;
    expect(d0).toBeGreaterThanOrEqual(4);
    expect(d0).toBeLessThan(30);

    const slow = new FakeModel({ latencyMs: 15 });
    const t1 = Date.now();
    await slow.classify(fv({ I: 0.5 }));
    const d1 = Date.now() - t1;
    expect(d1).toBeGreaterThanOrEqual(14);
    expect(d1).toBeLessThan(40);
  });

  it('jitter latency within 5-15 ms range', async () => {
    const m = new FakeModel({ minLatencyMs: 5, maxLatencyMs: 15 });
    const t0 = Date.now();
    await m.classify(fv({ I: 0.2 }));
    const d = Date.now() - t0;
    expect(d).toBeGreaterThanOrEqual(4);
    expect(d).toBeLessThan(40);
  });

  it('neutral when both I and OFI neutral', async () => {
    const m = new FakeModel({ latencyMs: 0 });
    const r = await m.classify(fv({ I: 0, ofi: 0 }));
    expect(r.lean).toBe('neutral');
    expect(r.confidence).toBe(0);
  });
});

describe('LlamaCppModel stub', () => {
  it('documents real hook and throws hermetic', async () => {
    const m = new LlamaCppModel('http://127.0.0.1:8080');
    expect(m.endpoint).toBe('http://127.0.0.1:8080');
    await expect(m.classify(fv({ I: 0.5 }))).rejects.toThrow(/llama-server/);
    await expect(m.classify(fv({ I: 0.5 }))).rejects.toThrow(/Q4_K_M/);
    await expect(m.classify(fv({ I: 0.5 }))).rejects.toThrow(/max_tokens=1|FakeModel/);
  });

  it('default endpoint contains n-gpu-layers 0 in error hint via class docs', async () => {
    const m = new LlamaCppModel();
    await expect(m.classify(fv())).rejects.toThrow(/not connected/);
  });
});
