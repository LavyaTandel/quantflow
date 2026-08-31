import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { beforeEach, describe, expect, it } from 'vitest';
import { KrakenNormalizer } from '../../../src/ingest/kraken/normalize.js';
import { EventPipeline } from '../../../src/ingest/pipeline.js';
import { isNormalizedEvent, type NormalizedEvent } from '../../../src/ingest/types.js';

const NOW_MS = Date.parse('2026-08-26T00:00:00.000Z');

function fixturePath(name: string): string {
  return fileURLToPath(new URL(`../../fixtures/kraken/${name}`, import.meta.url));
}

async function replayFixture(
  name: string,
  normalizer: KrakenNormalizer,
): Promise<{ totalLines: number; events: NormalizedEvent[] }> {
  const rl = createInterface({
    input: createReadStream(fixturePath(name), { encoding: 'utf8' }),
  });
  const events: NormalizedEvent[] = [];
  let totalLines = 0;
  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    totalLines += 1;
    const outcome = normalizer.normalize(JSON.parse(trimmed), { nowMs: NOW_MS });
    events.push(...outcome.events);
  }
  return { totalLines, events };
}

describe('Kraken fixture replay round-trip', () => {
  let pipeline: EventPipeline;

  beforeEach(() => {
    pipeline = new EventPipeline({ capacity: 10_000 });
  });

  it('replays the two-symbol valid sequence with zero checksum failures', async () => {
    const normalizer = new KrakenNormalizer();
    const { totalLines, events } = await replayFixture('book-two-symbols.jsonl', normalizer);

    expect(totalLines).toBe(5);
    expect(events).toHaveLength(5);
    expect(events.every((event) => event.kind === 'depth')).toBe(true);
    expect(normalizer.counters()).toEqual({
      framesProcessed: 5,
      droppedUnknown: 0,
      checksumFailures: 0,
    });

    const btcSequences = events
      .filter((event) => event.kind === 'depth' && event.symbol === 'BTCUSD')
      .map((event) => (event.kind === 'depth' ? [event.firstUpdateId, event.finalUpdateId] : []));
    expect(btcSequences).toEqual([
      [1, 1],
      [2, 2],
      [3, 3],
    ]);

    for (const event of events) expect(isNormalizedEvent(event)).toBe(true);
    expect(pipeline.pushMany(events)).toBe(5);
    expect(pipeline.counters().accepted).toBe(5);
  });

  it('replays the corrupted sequence with flagged failures but no lost frames', async () => {
    const failures: Array<{ symbol: string; expected: number; actual: number }> = [];
    const normalizer = new KrakenNormalizer({ onChecksumFailure: (f) => failures.push(f) });
    const { totalLines, events } = await replayFixture('book-corrupted-checksum.jsonl', normalizer);

    expect(totalLines).toBe(2);
    expect(events).toHaveLength(2); // corrupted checksums never drop frames
    expect(normalizer.counters().checksumFailures).toBe(2);
    expect(failures).toHaveLength(2);
    for (const failure of failures) {
      expect(failure.symbol).toBe('SOL/USD');
      expect(failure.expected).not.toBe(failure.actual);
    }

    expect(pipeline.pushMany(events)).toBe(2);
    const drained: NormalizedEvent[] = [];
    for (let i = 0; i < 2; i += 1) {
      const event = pipeline.take();
      if (event) drained.push(event);
    }
    expect(drained.map((event) => (event.kind === 'depth' ? event.symbol : ''))).toEqual([
      'SOLUSD',
      'SOLUSD',
    ]);
  });

  it('keeps sequence stamps gapless across interleaved symbols', async () => {
    const normalizer = new KrakenNormalizer();
    const { events } = await replayFixture('book-two-symbols.jsonl', normalizer);
    const sequences = new Map<string, number[]>();
    for (const event of events) {
      if (event.kind !== 'depth') continue;
      expect(event.finalUpdateId).toBe(event.firstUpdateId); // one stamp per frame
      const list = sequences.get(event.symbol) ?? [];
      list.push(event.firstUpdateId);
      sequences.set(event.symbol, list);
    }
    for (const [, list] of sequences) {
      for (let i = 0; i < list.length - 1; i += 1) {
        expect(list[i + 1]!).toBe(list[i]! + 1); // continuity: no gaps, no regressions
      }
    }
  });
});
