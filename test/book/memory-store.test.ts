import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { MemoryBookStore } from '../../src/book/memory-store.js';
import type { Level } from '../../src/book/store.js';
import { readReplayFile } from '../../src/ingest/replay.js';
import type { DepthDiff } from '../../src/ingest/types.js';

const INGEST_FIXTURES = fileURLToPath(new URL('../fixtures/ingest', import.meta.url));

function levels(entries: Array<[number, number]>): Level[] {
  return entries.map(([price, qty]) => ({ price, qty }));
}

async function gappedDiffs(): Promise<DepthDiff[]> {
  const { events } = await readReplayFile(join(INGEST_FIXTURES, 'depth-gapped.jsonl'));
  return events.filter((event): event is DepthDiff => event.kind === 'depth');
}

describe('MemoryBookStore', () => {
  it('seeds from a snapshot, orders topN per side, and drops zero-qty seeds', async () => {
    const store = new MemoryBookStore();
    await store.applySnapshot(
      'BTCUSDT',
      levels([
        [100.5, 2],
        [100.25, 1],
        [100.75, 0],
      ]),
      levels([
        [101, 1],
        [102, 3],
      ]),
      10,
    );
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 10 });
    expect(await store.topN('BTCUSDT', 2)).toEqual({
      bids: levels([
        [100.5, 2],
        [100.25, 1],
      ]),
      asks: levels([
        [101, 1],
        [102, 3],
      ]),
    });
  });

  it('returns empty views for unknown symbols and reports their diffs as gaps', async () => {
    const store = new MemoryBookStore();
    expect(await store.getMeta('BTCUSDT')).toBeUndefined();
    expect(await store.topN('BTCUSDT', 5)).toEqual({ bids: [], asks: [] });
    const status = await store.applyDepthDiff({
      kind: 'depth',
      symbol: 'BTCUSDT',
      ts: 0,
      firstUpdateId: 1,
      finalUpdateId: 2,
      levels: [],
    });
    expect(status).toBe('gap');
  });

  it('keeps symbols isolated from each other', async () => {
    const store = new MemoryBookStore();
    await store.applySnapshot('BTCUSDT', levels([[100, 1]]), levels([[101, 1]]), 5);
    await store.applySnapshot('ETHUSDT', levels([[50, 2]]), levels([[51, 2]]), 6);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 5 });
    expect(await store.getMeta('ETHUSDT')).toEqual({ lastUpdateId: 6 });
    expect(await store.topN('ETHUSDT', 1)).toEqual({
      bids: levels([[50, 2]]),
      asks: levels([[51, 2]]),
    });
  });

  it('removes levels whose diff carries qty 0', async () => {
    const store = new MemoryBookStore();
    await store.applySnapshot(
      'BTCUSDT',
      levels([
        [100, 1],
        [99, 2],
      ]),
      levels([[101, 1]]),
      1,
    );
    const status = await store.applyDepthDiff({
      kind: 'depth',
      symbol: 'BTCUSDT',
      ts: 0,
      firstUpdateId: 2,
      finalUpdateId: 2,
      levels: [{ side: 'bid', price: 100, newQty: 0 }],
    });
    expect(status).toBe('applied');
    expect(await store.topN('BTCUSDT', 10).then((side) => side.bids)).toEqual(levels([[99, 2]]));
  });

  it('replays depth-gapped.jsonl golden: contiguous prefix applies, gaps and staleness reject, fresh snapshot resyncs', async () => {
    const store = new MemoryBookStore();
    await store.applySnapshot(
      'ETHUSDT',
      levels([
        [3120, 2],
        [3119.5, 1],
      ]),
      levels([
        [3121, 1.5],
        [3122, 2.5],
      ]),
      99,
    );
    const diffs = await gappedDiffs();
    expect(diffs.map((d) => [d.firstUpdateId, d.finalUpdateId])).toEqual([
      [100, 103],
      [104, 107],
      [120, 124],
      [125, 126],
      [90, 95],
      [200, 205],
    ]);

    const statuses = [];
    for (const single of diffs) statuses.push(await store.applyDepthDiff(single));
    expect(statuses).toEqual(['applied', 'applied', 'gap', 'gap', 'stale', 'gap']);

    expect(await store.getMeta('ETHUSDT')).toEqual({ lastUpdateId: 107 });
    expect(await store.topN('ETHUSDT', 10)).toEqual({
      bids: levels([
        [3120, 2],
        [3119.5, 2],
        [3118.75, 1.5],
      ]),
      asks: levels([
        [3120.5, 3],
        [3121, 1.5],
        [3122, 2.5],
      ]),
    });

    await store.applySnapshot(
      'ETHUSDT',
      levels([
        [3118.75, 4],
        [3117.25, 2],
      ]),
      levels([[3121.5, 1.25]]),
      199,
    );
    expect(await store.applyDepthDiff(diffs[4]!)).toBe('stale');
    expect(await store.applyDepthDiff(diffs[5]!)).toBe('applied');

    expect(await store.getMeta('ETHUSDT')).toEqual({ lastUpdateId: 205 });
    expect(await store.topN('ETHUSDT', 3)).toEqual({
      bids: levels([
        [3118.75, 4],
        [3117.25, 2],
        [3116.5, 4],
      ]),
      asks: levels([
        [3121.5, 1.25],
        [3124, 2],
      ]),
    });
  });

  it('clamps degenerate topN requests to empty sides', async () => {
    const store = new MemoryBookStore();
    await store.applySnapshot('BTCUSDT', levels([[100, 1]]), levels([[101, 1]]), 1);
    expect(await store.topN('BTCUSDT', 0)).toEqual({ bids: [], asks: [] });
    expect(await store.topN('BTCUSDT', -3)).toEqual({ bids: [], asks: [] });
  });
});
