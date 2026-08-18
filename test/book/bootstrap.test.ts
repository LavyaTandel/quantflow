import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  bufferDiffsWhileBootstrapping,
  type BookSnapshot,
  type BootstrapEvent,
} from '../../src/book/bootstrap.js';
import { MemoryBookStore } from '../../src/book/memory-store.js';
import type { Level } from '../../src/book/store.js';
import { readReplayFile } from '../../src/ingest/replay.js';
import type { DepthDiff, DepthLevelChange } from '../../src/ingest/types.js';

const INGEST_FIXTURES = fileURLToPath(new URL('../fixtures/ingest', import.meta.url));
const BOOK_FIXTURES = fileURLToPath(new URL('../fixtures/book', import.meta.url));

interface RawSnapshotFile {
  lastUpdateId: number;
  bids: [string, string][];
  asks: [string, string][];
}

function diff(first: number, final: number, levels: DepthLevelChange[]): DepthDiff {
  return {
    kind: 'depth',
    symbol: 'BTCUSDT',
    ts: 0,
    firstUpdateId: first,
    finalUpdateId: final,
    levels,
  };
}

function levels(entries: Array<[number, number]>): Level[] {
  return entries.map(([price, qty]) => ({ price, qty }));
}

function snap(
  lastUpdateId: number,
  bids: Array<[number, number]>,
  asks: Array<[number, number]>,
): BookSnapshot {
  return { lastUpdateId, bids: levels(bids), asks: levels(asks) };
}

async function* iterableOf(diffs: DepthDiff[]): AsyncGenerator<DepthDiff> {
  for (const single of diffs) yield single;
}

async function* emptySource(): AsyncGenerator<DepthDiff> {}

function deferredGate(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function flushUntil(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 100_000 && !condition(); i += 1) await Promise.resolve();
  if (!condition()) throw new Error('condition never reached');
}

function steppingClock(stepMs = 7): { now: () => number; stepMs: number } {
  let tick = 0;
  return { now: () => (tick += stepMs), stepMs };
}

async function gappedDiffs(): Promise<DepthDiff[]> {
  const { events } = await readReplayFile(join(INGEST_FIXTURES, 'depth-gapped.jsonl'));
  return events.filter((event): event is DepthDiff => event.kind === 'depth');
}

describe('bufferDiffsWhileBootstrapping', () => {
  it('buffers diffs while the snapshot is in flight, discards stale ones, and applies the rest in order', async () => {
    const gate = deferredGate();
    const events: BootstrapEvent[] = [];
    const clock = steppingClock();
    const store = new MemoryBookStore();
    const runPromise = bufferDiffsWhileBootstrapping({
      symbol: 'BTCUSDT',
      diffs: iterableOf([
        diff(100, 103, [{ side: 'bid', price: 100, newQty: 1 }]),
        diff(104, 107, [{ side: 'ask', price: 200, newQty: 2 }]),
        diff(120, 124, [{ side: 'bid', price: 300, newQty: 3 }]),
      ]),
      store,
      fetchSnapshot: async () => {
        await gate.promise;
        return snap(119, [[65_000, 1]], [[65_001, 1]]);
      },
      nowMs: clock.now,
      onEvent: (event) => events.push(event),
    });
    await flushUntil(() => events.filter((event) => event.kind === 'diff-buffered').length === 3);
    gate.resolve();
    const result = await runPromise;

    expect(result.status).toBe('synced');
    expect(result.attempts).toBe(1);
    expect(result.snapshotsFetched).toBe(1);
    expect(result.bufferedWhileFetching).toBe(3);
    expect(result.discardedStale).toBe(2);
    expect(result.skippedStale).toBe(0);
    expect(result.applied).toBe(1);
    expect(result.gapsDetected).toBe(0);
    expect(result.lastUpdateId).toBe(124);
    expect(result.elapsedMs).toBe(clock.stepMs);
    expect(events.map((event) => event.kind)).toEqual([
      'diff-buffered',
      'diff-buffered',
      'diff-buffered',
      'snapshot-fetched',
      'diff-discarded',
      'diff-discarded',
      'diff-applied',
    ]);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 124 });
    expect(await store.topN('BTCUSDT', 5)).toEqual({
      bids: levels([
        [65_000, 1],
        [300, 3],
      ]),
      asks: levels([[65_001, 1]]),
    });
  });

  it('resyncs via a fresh snapshot when buffered continuity breaks and applies the previously rejected diff', async () => {
    const gate = deferredGate();
    const events: BootstrapEvent[] = [];
    let calls = 0;
    const snapshots = [snap(105, [[500, 7]], [[600, 1]]), snap(132, [[400, 8]], [[601, 2]])];
    const store = new MemoryBookStore();
    const runPromise = bufferDiffsWhileBootstrapping({
      symbol: 'BTCUSDT',
      diffs: iterableOf([
        diff(100, 103, [{ side: 'bid', price: 100, newQty: 1 }]),
        diff(104, 107, [{ side: 'bid', price: 500, newQty: 7 }]),
        diff(130, 133, [{ side: 'bid', price: 350, newQty: 9 }]),
      ]),
      store,
      fetchSnapshot: async () => {
        const index = calls;
        calls += 1;
        if (index === 0) await gate.promise;
        const snapshot = snapshots[index];
        if (!snapshot) throw new Error('unexpected extra fetch');
        return snapshot;
      },
      onEvent: (event) => events.push(event),
    });
    await flushUntil(() => events.filter((event) => event.kind === 'diff-buffered').length === 3);
    gate.resolve();
    const result = await runPromise;

    expect(result.status).toBe('synced');
    expect(result.attempts).toBe(2);
    expect(result.snapshotsFetched).toBe(2);
    expect(result.bufferedWhileFetching).toBe(3);
    expect(result.discardedStale).toBe(1);
    expect(result.applied).toBe(2);
    expect(result.gapsDetected).toBe(1);
    expect(result.lastUpdateId).toBe(133);
    expect(events.map((event) => event.kind)).toEqual([
      'diff-buffered',
      'diff-buffered',
      'diff-buffered',
      'snapshot-fetched',
      'diff-discarded',
      'diff-applied',
      'continuity-gap',
      'resync-started',
      'snapshot-fetched',
      'diff-applied',
    ]);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 133 });
    expect(await store.topN('BTCUSDT', 10)).toEqual({
      bids: levels([
        [400, 8],
        [350, 9],
      ]),
      asks: levels([[601, 2]]),
    });
  });

  it('gives up with unsynced after exhausting resync attempts against an unreachable frontier', async () => {
    const gate = deferredGate();
    const events: BootstrapEvent[] = [];
    let calls = 0;
    const runPromise = bufferDiffsWhileBootstrapping({
      symbol: 'BTCUSDT',
      diffs: iterableOf([diff(10, 12, []), diff(50, 52, [])]),
      store: new MemoryBookStore(),
      fetchSnapshot: async () => {
        calls += 1;
        if (calls === 1) await gate.promise;
        return snap(40, [], []);
      },
      maxAttempts: 3,
      onEvent: (event) => events.push(event),
    });
    await flushUntil(() => events.filter((event) => event.kind === 'diff-buffered').length === 2);
    gate.resolve();
    const result = await runPromise;

    expect(result.status).toBe('unsynced');
    expect(result.reason).toBe('max-resync-attempts-exceeded');
    expect(result.attempts).toBe(3);
    expect(result.snapshotsFetched).toBe(3);
    expect(result.discardedStale).toBe(1);
    expect(result.applied).toBe(0);
    expect(result.gapsDetected).toBe(3);
    expect(result.lastUpdateId).toBe(40);
    expect(events.filter((event) => event.kind === 'continuity-gap')).toHaveLength(3);
    expect(events.filter((event) => event.kind === 'resync-started')).toHaveLength(3);
  });

  it('keeps the buffer intact across snapshot fetch failures and syncs once one succeeds', async () => {
    const events: BootstrapEvent[] = [];
    let calls = 0;
    const result = await bufferDiffsWhileBootstrapping({
      symbol: 'BTCUSDT',
      diffs: iterableOf([diff(100, 103, [{ side: 'bid', price: 1_000, newQty: 5 }])]),
      store: new MemoryBookStore(),
      fetchSnapshot: async () => {
        calls += 1;
        if (calls < 3) throw new Error('boom');
        return snap(102, [], []);
      },
      maxAttempts: 4,
      onEvent: (event) => events.push(event),
    });

    expect(result.status).toBe('synced');
    expect(result.attempts).toBe(3);
    expect(result.snapshotsFetched).toBe(1);
    expect(result.applied).toBe(1);
    expect(result.lastUpdateId).toBe(103);
    const failures = events.filter((event) => event.kind === 'snapshot-fetch-failed');
    expect(failures).toHaveLength(2);
    expect(
      failures.every((event) => event.kind !== 'snapshot-fetch-failed' || event.message === 'boom'),
    ).toBe(true);
  });

  it('syncs from the snapshot alone when the source ends before any diff arrives', async () => {
    const result = await bufferDiffsWhileBootstrapping({
      symbol: 'BTCUSDT',
      diffs: emptySource(),
      store: new MemoryBookStore(),
      fetchSnapshot: async () => snap(777, [[1, 1]], [[2, 1]]),
    });
    expect(result.status).toBe('synced');
    expect(result.attempts).toBe(1);
    expect(result.applied).toBe(0);
    expect(result.lastUpdateId).toBe(777);
  });

  it('replays depth-gapped.jsonl through bootstrap: discard, apply, skip-stale, gap, then resync to fully synced', async () => {
    const gate = deferredGate();
    const events: BootstrapEvent[] = [];
    let snapCalls = 0;
    const store = new MemoryBookStore();
    const runPromise = bufferDiffsWhileBootstrapping({
      symbol: 'ETHUSDT',
      diffs: iterableOf(await gappedDiffs()),
      store,
      fetchSnapshot: async () => {
        snapCalls += 1;
        if (snapCalls === 1) {
          await gate.promise;
          return snap(119, [[3_119, 1]], [[3_120, 1]]);
        }
        return snap(199, [[3_130, 10]], [[3_140, 0.5]]);
      },
      onEvent: (event) => events.push(event),
    });
    await flushUntil(() => events.filter((event) => event.kind === 'diff-buffered').length === 6);
    gate.resolve();
    const result = await runPromise;

    expect(result.status).toBe('synced');
    expect(result.attempts).toBe(2);
    expect(result.bufferedWhileFetching).toBe(6);
    expect(result.discardedStale).toBe(2);
    expect(result.skippedStale).toBe(1);
    expect(result.applied).toBe(3);
    expect(result.gapsDetected).toBe(1);
    expect(result.lastUpdateId).toBe(205);
    expect(await store.getMeta('ETHUSDT')).toEqual({ lastUpdateId: 205 });
    expect(await store.topN('ETHUSDT', 10)).toEqual({
      bids: levels([
        [3_130, 10],
        [3_116.5, 4],
      ]),
      asks: levels([
        [3_124, 2],
        [3_140, 0.5],
      ]),
    });
  });

  it('bootstraps from the committed snapshot-btcusdt.json fixture and applies post-snapshot mutations', async () => {
    const raw = JSON.parse(
      await readFile(join(BOOK_FIXTURES, 'snapshot-btcusdt.json'), 'utf8'),
    ) as RawSnapshotFile;
    const fixtureSnapshot: BookSnapshot = {
      lastUpdateId: raw.lastUpdateId,
      bids: raw.bids.map(([price, qty]) => ({ price: Number(price), qty: Number(qty) })),
      asks: raw.asks.map(([price, qty]) => ({ price: Number(price), qty: Number(qty) })),
    };
    const gate = deferredGate();
    const events: BootstrapEvent[] = [];
    const store = new MemoryBookStore();
    const runPromise = bufferDiffsWhileBootstrapping({
      symbol: 'BTCUSDT',
      diffs: iterableOf([
        diff(4_999_990, 4_999_992, [{ side: 'bid', price: Number('64999.90'), newQty: 9 }]),
        diff(5_000_001, 5_000_002, [
          { side: 'bid', price: Number('64999.90'), newQty: 5.55 },
          { side: 'ask', price: Number('65002.30'), newQty: 0 },
        ]),
        diff(5_000_003, 5_000_003, [{ side: 'ask', price: Number('65003.00'), newQty: 1.25 }]),
      ]),
      store,
      fetchSnapshot: async () => {
        await gate.promise;
        return fixtureSnapshot;
      },
      onEvent: (event) => events.push(event),
    });
    await flushUntil(() => events.filter((event) => event.kind === 'diff-buffered').length === 3);
    gate.resolve();
    const result = await runPromise;

    expect(result.status).toBe('synced');
    expect(result.attempts).toBe(1);
    expect(result.bufferedWhileFetching).toBe(3);
    expect(result.discardedStale).toBe(1);
    expect(result.applied).toBe(2);
    expect(result.lastUpdateId).toBe(5_000_003);

    const top = await store.topN('BTCUSDT', 50);
    expect(top.bids).toHaveLength(25);
    expect(top.bids[0]).toEqual({ price: Number('64999.90'), qty: 5.55 });
    expect(top.bids[1]).toEqual({
      price: Number(raw.bids[1]![0]),
      qty: Number(raw.bids[1]![1]),
    });
    const askPrices = top.asks.map((level) => level.price);
    expect(askPrices).not.toContain(Number('65002.30'));
    expect(top.asks.filter((level) => level.price === Number('65003.00'))).toEqual([
      { price: Number('65003.00'), qty: 1.25 },
    ]);
    expect(top.asks).toHaveLength(25);
    expect(await store.getMeta('BTCUSDT')).toEqual({ lastUpdateId: 5_000_003 });
  });
});
