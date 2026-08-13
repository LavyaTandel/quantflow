import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, describe, expect, it } from 'vitest';
import { readReplayFile, replayEvents } from '../../src/ingest/replay.js';
import type { DepthDiff } from '../../src/ingest/types.js';

const FIXTURES = fileURLToPath(new URL('../fixtures/ingest', import.meta.url));
const NOW = () => 1_756_200_009_999;

const tmpDirs: string[] = [];

afterAll(async () => {
  await Promise.all(tmpDirs.map((dir) => rm(dir, { recursive: true, force: true })));
});

async function writeTempJsonl(lines: string[]): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'qf-replay-'));
  tmpDirs.push(dir);
  const file = join(dir, 'input.jsonl');
  await writeFile(file, lines.join('\n'), 'utf8');
  return file;
}

describe('readReplayFile', () => {
  it('replays the mixed-frame fixture into golden kind counts', async () => {
    const { events, stats } = await readReplayFile(join(FIXTURES, 'mixed-frames.jsonl'), {
      nowMs: NOW,
    });
    expect(stats).toEqual({
      totalLines: 18,
      malformedJson: 0,
      rawFrames: 18,
      preNormalized: 0,
      droppedUnknown: 0,
      eventsEmitted: 18,
    });
    const kinds = { trade: 0, bbo: 0, depth: 0 };
    for (const event of events) kinds[event.kind] += 1;
    expect(kinds).toEqual({ trade: 9, bbo: 5, depth: 4 });
    expect(events[0]).toEqual({
      kind: 'trade',
      symbol: 'BTCUSDT',
      price: 65000.1,
      qty: 0.25,
      ts: 1756200000000,
      tradeId: '9001',
    });
  });

  it('surfaces the gapped depth update-id sequence for continuity detection downstream', async () => {
    const { events, stats } = await readReplayFile(join(FIXTURES, 'depth-gapped.jsonl'));
    expect(stats.eventsEmitted).toBe(6);
    expect(stats.droppedUnknown).toBe(0);
    const diffs = events.filter((e): e is DepthDiff => e.kind === 'depth');
    expect(diffs.map((d) => [d.firstUpdateId, d.finalUpdateId])).toEqual([
      [100, 103],
      [104, 107],
      [120, 124],
      [125, 126],
      [90, 95],
      [200, 205],
    ]);
    const gaps: Array<{ at: number; expected: number; got: number }> = [];
    for (let i = 1; i < diffs.length; i++) {
      const expected = diffs[i - 1]!.finalUpdateId + 1;
      if (diffs[i]!.firstUpdateId !== expected) {
        gaps.push({ at: i, expected, got: diffs[i]!.firstUpdateId });
      }
    }
    expect(gaps).toEqual([
      { at: 2, expected: 108, got: 120 },
      { at: 4, expected: 127, got: 90 },
      { at: 5, expected: 96, got: 200 },
    ]);
  });

  it('round-trips pre-normalized event lines losslessly', async () => {
    const source = await readReplayFile(join(FIXTURES, 'mixed-frames.jsonl'), { nowMs: NOW });
    const normalizedLines = source.events.map((event) => JSON.stringify(event));
    const roundTripped = await writeTempJsonl(normalizedLines);
    const result = await readReplayFile(roundTripped);
    expect(result.stats.preNormalized).toBe(18);
    expect(result.stats.rawFrames).toBe(0);
    expect(result.stats.droppedUnknown).toBe(0);
    expect(result.events).toEqual(source.events);
  });

  it('counts malformed JSON lines without throwing', async () => {
    const good =
      '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":5,"p":"2","q":"3","T":1,"m":true,"M":true}}';
    const file = await writeTempJsonl([good, '{not json at all', '', '   ', '{"kind":"trade"}']);
    const { events, stats } = await readReplayFile(file, { nowMs: NOW });
    expect(stats).toMatchObject({
      totalLines: 3,
      malformedJson: 1,
      rawFrames: 2,
      droppedUnknown: 1,
      eventsEmitted: 1,
    });
    expect(events).toHaveLength(1);
  });

  it('reads fixture files from disk with trailing newline intact', async () => {
    const raw = await readFile(join(FIXTURES, 'mixed-frames.jsonl'), 'utf8');
    expect(raw.endsWith('\n')).toBe(true);
  });
});

describe('replayEvents', () => {
  it('yields the identical sequence as readReplayFile', async () => {
    const path = join(FIXTURES, 'depth-gapped.jsonl');
    const eager = await readReplayFile(path);
    const lazy: Awaited<ReturnType<typeof readReplayFile>>['events'] = [];
    for await (const event of replayEvents(path)) lazy.push(event);
    expect(lazy).toEqual(eager.events);
  });

  it('streams a pre-normalized file lazily line by line', async () => {
    const source = await readReplayFile(join(FIXTURES, 'depth-gapped.jsonl'));
    const file = await writeTempJsonl(source.events.map((e) => JSON.stringify(e)));
    let count = 0;
    for await (const event of replayEvents(file)) {
      expect(event.kind).toBe('depth');
      count += 1;
    }
    expect(count).toBe(6);
  });
});
