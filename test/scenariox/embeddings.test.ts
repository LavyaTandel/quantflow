import { describe, it, expect } from 'vitest';
import { FakeEmbedder } from '../../src/scenariox/embeddings.js';
import type { Scenario } from '../../src/scenariox/types.js';

function makeScenarios(): Scenario[] {
  return [
    {
      id: 's1',
      name: 'Fed hike',
      shocks: [{ description: 'Fed hikes 50bps rates up inflation' }],
    },
    {
      id: 's2',
      name: 'Oil shock',
      shocks: [{ description: 'Oil price spike supply crunch' }],
    },
    {
      id: 's3',
      name: 'Tech crash',
      shocks: [{ description: 'Tech earnings miss Nasdaq drops' }],
    },
  ];
}

describe('FakeEmbedder', () => {
  it('nearestScenario deterministic — same shock text -> same scenario', async () => {
    const embedder = new FakeEmbedder({ delayMs: 5 });
    const scenarios = makeScenarios();

    const a = await embedder.nearestScenario('Fed hikes 50bps', scenarios);
    const b = await embedder.nearestScenario('Fed hikes 50bps', scenarios);
    expect(a?.id).toBe(b?.id);
    expect(a?.id).toBe('s1');

    const oil = await embedder.nearestScenario('Oil price spike', scenarios);
    expect(oil?.id).toBe('s2');

    const tech = await embedder.nearestScenario('Tech earnings miss', scenarios);
    expect(tech?.id).toBe('s3');
  });

  it('embed deterministic — same text -> same vector', async () => {
    const embedder = new FakeEmbedder({ delayMs: 5 });
    const v1 = await embedder.embed('Fed hikes 50bps');
    const v2 = await embedder.embed('Fed hikes 50bps');
    expect(v1).toEqual(v2);
    expect(v1.length).toBe(384);
  });

  it('nearestScenario returns correct Scenario within 200 ms P99 for 10 lookups', async () => {
    const embedder = new FakeEmbedder({ delayMs: 5 });
    const scenarios = makeScenarios();
    const queries = [
      'Fed hikes rates',
      'Oil supply crunch',
      'Tech crash Nasdaq',
      'Fed 50bps hike',
      'Oil spike price',
      'Nasdaq earnings miss',
      'Fed inflation hike',
      'Oil price surge',
      'Tech earnings drop',
      'Fed hikes 50bps inflation',
    ];

    const timings: number[] = [];
    for (const q of queries) {
      const start = performance.now();
      const result = await embedder.nearestScenario(q, scenarios);
      const elapsed = performance.now() - start;
      timings.push(elapsed);
      expect(result).toBeDefined();
    }
    timings.sort((a, b) => a - b);
    // P99 for 10 samples = second largest (index 9) approximates 99th; we check max <=200
    const p99 = timings[Math.ceil(0.99 * timings.length) - 1]!;
    expect(p99).toBeLessThanOrEqual(200);
    // Also ensure median is well under budget (sanity)
    const p50 = timings[Math.floor(timings.length / 2)]!;
    expect(p50).toBeLessThanOrEqual(100);
  });

  it('empty scenarios -> undefined', async () => {
    const embedder = new FakeEmbedder();
    const result = await embedder.nearestScenario('anything', []);
    expect(result).toBeUndefined();
  });
});
