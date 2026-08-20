import { describe, expect, it } from 'vitest';
import type { NormalizedEvent } from '../../src/ingest/types.js';
import { DEFAULT_TICK_BUFFER_CAPACITY, TickBuffer } from '../../src/store/buffer.js';
import { mkBurst, mkBbo, mkDepth, mkTrade } from './helpers.js';

function drainedAreFifo(buffer: TickBuffer<NormalizedEvent>, n: number): NormalizedEvent[] {
  const out = buffer.drainUpTo(n);
  for (let i = 1; i < out.length; i++) {
    expect(out[i]!.ts).toBeGreaterThanOrEqual(out[i - 1]!.ts);
  }
  return out;
}

describe('TickBuffer', () => {
  it('admits under capacity and drains FIFO', () => {
    const buffer = new TickBuffer({ capacity: 10 });
    for (const event of mkBurst(7)) expect(buffer.push(event)).toBe(true);
    expect(buffer.size()).toBe(7);
    const out = drainedAreFifo(buffer, 3);
    expect(out).toHaveLength(3);
    expect(buffer.size()).toBe(4);
    expect(drainedAreFifo(buffer, 100)).toHaveLength(4);
    expect(buffer.size()).toBe(0);
  });

  it('uses the default 50_000 capacity', () => {
    expect(new TickBuffer().capacity).toBe(DEFAULT_TICK_BUFFER_CAPACITY);
    expect(DEFAULT_TICK_BUFFER_CAPACITY).toBe(50_000);
  });

  it('sheds the OLDEST droppable entry to admit a depth diff', () => {
    const buffer = new TickBuffer({ capacity: 4 });
    const first = mkTrade();
    buffer.push(first);
    buffer.push(mkTrade());
    buffer.push(mkDepth());
    buffer.push(mkBbo());
    const depth = mkDepth();
    expect(buffer.push(depth)).toBe(true);

    const counters = buffer.counters();
    expect(counters.size).toBe(4);
    expect(counters.droppedOldest).toBe(1);
    expect(counters.droppedOldestByKind.trade).toBe(1);
    expect(counters.accepted).toBe(5);

    const out = buffer.drainUpTo(4);
    expect(out[0]).not.toBe(first);
    expect(out).not.toContain(first);
    expect(out[3]).toBe(depth);
  });

  it('sheds the oldest trade/bbo even when depth sits behind it', () => {
    const buffer = new TickBuffer({ capacity: 3 });
    const oldest = mkTrade();
    buffer.push(oldest);
    const keptDepth = mkDepth();
    buffer.push(keptDepth);
    buffer.push(mkTrade());
    expect(buffer.push(mkBbo())).toBe(true);

    const out = buffer.drainUpTo(3);
    expect(out[0]).toBe(keptDepth);
    expect(out).not.toContain(oldest);
    expect(buffer.counters().droppedOldestByKind.trade).toBe(1);
  });

  it('refuses newest depth loudly when only depth remains', () => {
    const buffer = new TickBuffer<NormalizedEvent>({ capacity: 3 });
    const refused: NormalizedEvent[] = [];
    const guarded = new TickBuffer<NormalizedEvent>({
      capacity: 3,
      onRefuse: (e) => refused.push(e),
    });
    for (const d of [mkDepth(), mkDepth(), mkDepth()]) {
      expect(guarded.push(d)).toBe(true);
      expect(buffer.push(d)).toBe(true);
    }
    const late = mkDepth();
    expect(guarded.push(late)).toBe(false);
    expect(refused).toEqual([late]);

    expect(guarded.size()).toBe(3);
    expect(guarded.counters().refusedDepth).toBe(1);
  });

  it('refuses droppable events when only depth remains', () => {
    const buffer = new TickBuffer<NormalizedEvent>({ capacity: 2 });
    buffer.push(mkDepth());
    buffer.push(mkDepth());
    expect(buffer.push(mkTrade())).toBe(false);
    expect(buffer.counters().refusedDroppable).toBe(1);
    expect(buffer.size()).toBe(2);
  });

  it('holds the 50K bound under a 30K burst on top of a full buffer', () => {
    const buffer = new TickBuffer();
    for (const event of mkBurst(45_000)) buffer.push(event);
    expect(buffer.size()).toBe(45_000);

    let pushed = 0;
    for (const event of mkBurst(30_000)) {
      buffer.push(event);
      pushed += 1;
      if (pushed % 1_000 === 0) expect(buffer.size()).toBeLessThanOrEqual(50_000);
    }
    expect(buffer.size()).toBeLessThanOrEqual(50_000);
    expect(buffer.size()).toBe(50_000);

    const counters = buffer.counters();
    expect(counters.droppedOldest + counters.refusedDroppable + counters.refusedDepth).toBe(
      75_000 - 50_000,
    );
  });

  it('keeps per-symbol ts monotone across shed and drain', () => {
    const buffer = new TickBuffer();
    for (const event of mkBurst(60_000)) buffer.push(event);
    const out = buffer.drainUpTo(50_000);
    expect(out).toHaveLength(50_000);
    for (let i = 1; i < out.length; i++) {
      expect(out[i]!.ts).toBeGreaterThanOrEqual(out[i - 1]!.ts);
    }
  });

  it('drainUpTo(0) returns an empty array', () => {
    const buffer = new TickBuffer({ capacity: 2 });
    buffer.push(mkTrade());
    expect(buffer.drainUpTo(0)).toEqual([]);
    expect(buffer.size()).toBe(1);
  });

  it('rejects invalid capacities', () => {
    expect(() => new TickBuffer({ capacity: 0 })).toThrow(RangeError);
    expect(() => new TickBuffer({ capacity: -1 })).toThrow(RangeError);
    expect(() => new TickBuffer({ capacity: 1.5 })).toThrow(RangeError);
  });
});
