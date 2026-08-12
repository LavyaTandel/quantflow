import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { normalizeFrame, type NormalizeContext } from './normalize.js';
import { isNormalizedEvent, type NormalizedEvent } from './types.js';

export interface ReplayOptions {
  nowMs?: () => number;
}

export interface ReplayStats {
  readonly totalLines: number;
  readonly malformedJson: number;
  readonly rawFrames: number;
  readonly preNormalized: number;
  readonly droppedUnknown: number;
  readonly eventsEmitted: number;
}

export interface ReplayResult {
  readonly events: NormalizedEvent[];
  readonly stats: ReplayStats;
}

interface LineTally {
  malformedJson: number;
  rawFrames: number;
  preNormalized: number;
  droppedUnknown: number;
  events: NormalizedEvent[];
}

function emptyTally(): LineTally {
  return { malformedJson: 0, rawFrames: 0, preNormalized: 0, droppedUnknown: 0, events: [] };
}

function processLine(line: string, ctx: NormalizeContext, tally: LineTally): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    tally.malformedJson += 1;
    return;
  }
  if (isNormalizedEvent(parsed)) {
    tally.preNormalized += 1;
    tally.events.push(parsed);
    return;
  }
  tally.rawFrames += 1;
  const outcome = normalizeFrame(parsed, ctx);
  for (const event of outcome.events) tally.events.push(event);
  if (outcome.droppedUnknown) tally.droppedUnknown += 1;
}

function finalize(tally: LineTally, totalLines: number): ReplayResult {
  const stats: ReplayStats = {
    totalLines,
    malformedJson: tally.malformedJson,
    rawFrames: tally.rawFrames,
    preNormalized: tally.preNormalized,
    droppedUnknown: tally.droppedUnknown,
    eventsEmitted: tally.events.length,
  };
  return { events: tally.events, stats };
}

export async function readReplayFile(
  filePath: string,
  opts: ReplayOptions = {},
): Promise<ReplayResult> {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const rl = createInterface({ input: createReadStream(filePath, { encoding: 'utf8' }) });
  const tally = emptyTally();
  let totalLines = 0;
  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    totalLines += 1;
    processLine(trimmed, { nowMs: nowMs() }, tally);
  }
  return finalize(tally, totalLines);
}

export async function* replayEvents(
  filePath: string,
  opts: ReplayOptions = {},
): AsyncGenerator<NormalizedEvent> {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const rl = createInterface({ input: createReadStream(filePath, { encoding: 'utf8' }) });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    const tally = emptyTally();
    processLine(trimmed, { nowMs: nowMs() }, tally);
    for (const event of tally.events) yield event;
  }
}
