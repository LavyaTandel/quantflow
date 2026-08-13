import { beforeEach, describe, expect, it } from 'vitest';
import { EventPipeline } from '../../src/ingest/pipeline.js';
import type { DepthDiff, NormalizedEvent, TradeTick } from '../../src/ingest/types.js';

let seq = 0;

function trade(symbol = 'BTCUSDT'): TradeTick {
  seq += 1;
  return { kind: 'trade', symbol, price: 1, qty: 1, ts: seq, tradeId: `t${seq}` };
}

function bbo(symbol = 'BTCUSDT'): NormalizedEvent {
  return { kind: 'bbo', symbol, bidPrice: 1, bidQty: 1, askPrice: 2, askQty: 1, ts: ++seq };
}

function depth(first: number, final: number, symbol = 'BTCUSDT'): DepthDiff {
  return {
    kind: 'depth',
    symbol,
    ts: first,
    firstUpdateId: first,
    finalUpdateId: final,
    levels: [{ side: 'bid', price: 1, newQty: 2 }],
  };
}

function drainAll(pipeline: EventPipeline): NormalizedEvent[] {
  const out: NormalizedEvent[] = [];
  for (let event = pipeline.take(); event; event = pipeline.take()) out.push(event);
  return out;
}

describe('EventPipeline', () => {
  beforeEach(() => {
    seq = 0;
  });

  it('accepts events up to capacity and reports counters', () => {
    const pipeline = new EventPipeline({ capacity: 3 });
    expect(pipeline.pushMany([trade(), trade(), depth(1, 2)])).toBe(3);
    expect(pipeline.counters()).toEqual({
      accepted: 3,
      droppedByKind: { trade: 0, bbo: 0, depth: 0 },
      currentDepth: 3,
      capacity: 3,
    });
  });

  it('rejects non-positive capacity', () => {
    expect(() => new EventPipeline({ capacity: 0 })).toThrow(RangeError);
  });

  it('sheds the oldest trade first under overflow', () => {
    const pipeline = new EventPipeline({ capacity: 5 });
    for (let i = 0; i < 5; i++) pipeline.push(trade(`S${i}`));
    const incoming = trade('new');
    expect(pipeline.push(incoming)).toBe(true);
    const counters = pipeline.counters();
    expect(counters.droppedByKind.trade).toBe(1);
    expect(counters.currentDepth).toBe(5);
    const drained = drainAll(pipeline).map((e) => (e.kind === 'trade' ? e.symbol : '?'));
    expect(drained).toEqual(['S1', 'S2', 'S3', 'S4', 'new']);
  });

  it('sheds oldest droppable across kinds by arrival order', () => {
    const pipeline = new EventPipeline({ capacity: 3 });
    pipeline.push(trade('first-trade'));
    pipeline.push({
      kind: 'bbo',
      symbol: 'X',
      bidPrice: 1,
      bidQty: 1,
      askPrice: 2,
      askQty: 1,
      ts: 1,
    });
    pipeline.push(depth(1, 2));
    pipeline.push(trade('second-trade'));
    expect(pipeline.counters().droppedByKind).toMatchObject({ trade: 1 });
    expect(drainAll(pipeline).map((e) => e.symbol)).toEqual(['X', 'BTCUSDT', 'second-trade']);
  });

  it('skips leading buffered depth when shedding and never drops depth', () => {
    const pipeline = new EventPipeline({ capacity: 4 });
    pipeline.push(depth(10, 11));
    pipeline.push(trade('a'));
    pipeline.push(trade('b'));
    pipeline.push(trade('c'));
    pipeline.push(trade('d'));
    expect(pipeline.counters().droppedByKind).toEqual({ trade: 1, bbo: 0, depth: 0 });
    const drained = drainAll(pipeline);
    expect(drained[0]).toMatchObject({ kind: 'depth', firstUpdateId: 10 });
    expect(drained.map((e) => ('tradeId' in e ? e.tradeId : 'D'))).toEqual(['D', 't2', 't3', 't4']);
  });

  it('sheds buffered trades to admit incoming depth diffs under overload', () => {
    const pipeline = new EventPipeline({ capacity: 50 });
    const pushedDepths: number[] = [];
    let nextId = 100;
    let pushed = 0;
    for (let i = 0; i < 200; i++) {
      if (i % 5 === 0) {
        const d = depth(nextId, nextId + 3);
        nextId += 4;
        pushedDepths.push(d.firstUpdateId);
        pipeline.push(d);
      } else {
        pipeline.push(i % 2 === 1 ? trade() : bbo());
      }
      pushed += 1;
    }
    const counters = pipeline.counters();
    expect(counters.currentDepth).toBe(50);
    expect(counters.accepted).toBeLessThanOrEqual(pushed);
    expect(counters.droppedByKind.depth).toBe(0);
    expect(counters.droppedByKind.trade).toBeGreaterThan(0);
    expect(counters.droppedByKind.bbo).toBeGreaterThan(0);
    const survivedDepthIds = drainAll(pipeline)
      .filter((e): e is DepthDiff => e.kind === 'depth')
      .map((e) => e.firstUpdateId);
    expect(survivedDepthIds).toEqual(pushedDepths);
  });

  it('refuses an incoming depth when the buffer is saturated with depth only', () => {
    const pipeline = new EventPipeline({ capacity: 2 });
    pipeline.push(depth(1, 2));
    pipeline.push(depth(3, 4));
    expect(pipeline.push(depth(5, 6))).toBe(false);
    expect(pipeline.counters()).toMatchObject({
      currentDepth: 2,
      droppedByKind: { trade: 0, bbo: 0, depth: 1 },
    });
    expect(drainAll(pipeline).map((e) => (e as DepthDiff).firstUpdateId)).toEqual([1, 3]);
  });

  it('refuses an incoming bbo when the buffer holds nothing droppable', () => {
    const pipeline = new EventPipeline({ capacity: 1 });
    pipeline.push(depth(1, 2));
    const bbo = {
      kind: 'bbo' as const,
      symbol: 'X',
      bidPrice: 1,
      bidQty: 1,
      askPrice: 2,
      askQty: 1,
      ts: 9,
    };
    expect(pipeline.push(bbo)).toBe(false);
    expect(pipeline.counters().droppedByKind.bbo).toBe(1);
  });

  it('drains FIFO then completes after close', async () => {
    const pipeline = new EventPipeline({ capacity: 8 });
    const a = trade('a');
    const b = trade('b');
    const c = trade('c');
    pipeline.pushMany([a, b, c]);
    pipeline.close();
    const collected: NormalizedEvent[] = [];
    for await (const event of pipeline.drain()) collected.push(event);
    expect(collected).toEqual([a, b, c]);
    expect(pipeline.isClosed()).toBe(true);
  });

  it('wakes a waiting consumer on push', async () => {
    const pipeline = new EventPipeline({ capacity: 4 });
    const iterator = pipeline.drain()[Symbol.asyncIterator]();
    const pending = iterator.next();
    const t = trade('late');
    pipeline.push(t);
    await expect(pending).resolves.toEqual({ value: t, done: false });
    pipeline.close();
    await expect(iterator.next()).resolves.toMatchObject({ done: true });
  });

  it('ignores pushes after close', () => {
    const pipeline = new EventPipeline({ capacity: 4 });
    pipeline.close();
    expect(pipeline.push(trade())).toBe(false);
    expect(pipeline.counters()).toMatchObject({ accepted: 0, currentDepth: 0 });
  });
});
