import type { DepthDiff } from '../ingest/types.js';
import { assessContinuity } from './continuity.js';
import type { BookStore, Level } from './store.js';

export interface BookSnapshot {
  lastUpdateId: number;
  bids: Level[];
  asks: Level[];
}

export type SnapshotFetcher = (symbol: string) => Promise<BookSnapshot>;

export type BootstrapEvent =
  | { kind: 'diff-buffered'; buffered: number }
  | { kind: 'snapshot-fetched'; attempt: number; lastUpdateId: number }
  | { kind: 'snapshot-fetch-failed'; attempt: number; message: string }
  | { kind: 'diff-discarded'; finalUpdateId: number }
  | { kind: 'diff-applied'; firstUpdateId: number; finalUpdateId: number }
  | { kind: 'continuity-gap'; expectedFrom: number; gotFirst: number; gotFinal: number }
  | { kind: 'resync-started'; nextAttempt: number };

export interface BootstrapInput {
  symbol: string;
  diffs: AsyncIterable<DepthDiff>;
  store: BookStore;
  fetchSnapshot: SnapshotFetcher;
  maxAttempts?: number;
  nowMs?: () => number;
  onEvent?: (event: BootstrapEvent) => void;
}

export interface BootstrapResult {
  status: 'synced' | 'unsynced';
  reason?: 'max-resync-attempts-exceeded' | 'snapshot-fetch-failed';
  attempts: number;
  snapshotsFetched: number;
  bufferedWhileFetching: number;
  discardedStale: number;
  skippedStale: number;
  applied: number;
  gapsDetected: number;
  lastUpdateId?: number;
  elapsedMs: number;
}

type FetchOutcome = { ok: true; snapshot: BookSnapshot } | { ok: false; error: unknown };

function wrapFetch(promise: Promise<BookSnapshot>): Promise<FetchOutcome> {
  return promise.then(
    (snapshot) => ({ ok: true as const, snapshot }),
    (error: unknown) => ({ ok: false as const, error }),
  );
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function emitGap(
  onEvent: (event: BootstrapEvent) => void,
  currentLast: number,
  candidate: DepthDiff,
): void {
  onEvent({
    kind: 'continuity-gap',
    expectedFrom: currentLast + 1,
    gotFirst: candidate.firstUpdateId,
    gotFinal: candidate.finalUpdateId,
  });
}

export async function bufferDiffsWhileBootstrapping(
  input: BootstrapInput,
): Promise<BootstrapResult> {
  const nowMs = input.nowMs ?? (() => Date.now());
  const maxAttempts = input.maxAttempts ?? 3;
  const onEvent = input.onEvent ?? (() => {});
  const startedAtMs = nowMs();
  const iterator = input.diffs[Symbol.asyncIterator]();

  let sourceDone = false;
  let stashedNext: Promise<IteratorResult<DepthDiff>> | undefined;
  let pending: DepthDiff[] = [];
  let snapshot: BookSnapshot | undefined;
  let attempts = 0;
  let snapshotsFetched = 0;
  let bufferedWhileFetching = 0;
  let discardedStale = 0;
  let skippedStale = 0;
  let applied = 0;
  let gapsDetected = 0;
  let lastUpdateId: number | undefined;
  let lastFailureWasFetch = false;

  const installSnapshot = async (fresh: BookSnapshot): Promise<BookSnapshot> => {
    await input.store.applySnapshot(input.symbol, fresh.bids, fresh.asks, fresh.lastUpdateId);
    snapshotsFetched += 1;
    onEvent({ kind: 'snapshot-fetched', attempt: attempts, lastUpdateId: fresh.lastUpdateId });
    return fresh;
  };

  while (attempts < maxAttempts) {
    attempts += 1;
    lastFailureWasFetch = false;
    snapshot = undefined;
    const fetchOutcome = wrapFetch(input.fetchSnapshot(input.symbol));

    for (;;) {
      if (snapshot) {
        if (sourceDone || stashedNext === undefined) break;
        const res = await stashedNext;
        stashedNext = undefined;
        if (res.done) sourceDone = true;
        else pending.push(res.value);
        continue;
      }
      if (sourceDone && stashedNext === undefined) {
        const outcome = await fetchOutcome;
        if (!outcome.ok) {
          lastFailureWasFetch = true;
          onEvent({
            kind: 'snapshot-fetch-failed',
            attempt: attempts,
            message: messageOf(outcome.error),
          });
          break;
        }
        snapshot = await installSnapshot(outcome.snapshot);
        continue;
      }
      if (!sourceDone && stashedNext === undefined) {
        stashedNext = iterator.next();
      }
      const race = await Promise.race([
        fetchOutcome.then((outcome) => ({ type: 'fetch' as const, outcome })),
        stashedNext!.then((res) => ({ type: 'item' as const, res })),
      ]);
      if (race.type === 'item') {
        stashedNext = undefined;
        if (race.res.done) sourceDone = true;
        else {
          pending.push(race.res.value);
          bufferedWhileFetching += 1;
          onEvent({ kind: 'diff-buffered', buffered: bufferedWhileFetching });
        }
      } else if (race.outcome.ok) {
        snapshot = await installSnapshot(race.outcome.snapshot);
      } else {
        lastFailureWasFetch = true;
        onEvent({
          kind: 'snapshot-fetch-failed',
          attempt: attempts,
          message: messageOf(race.outcome.error),
        });
        break;
      }
    }

    if (!snapshot) continue;

    let idx = 0;
    while (idx < pending.length && pending[idx]!.finalUpdateId <= snapshot.lastUpdateId) {
      discardedStale += 1;
      onEvent({ kind: 'diff-discarded', finalUpdateId: pending[idx]!.finalUpdateId });
      idx += 1;
    }

    let currentLast = snapshot.lastUpdateId;
    lastUpdateId = currentLast;
    let gapDetected = false;

    while (idx < pending.length) {
      const candidate = pending[idx]!;
      idx += 1;
      const verdict = assessContinuity(currentLast, candidate);
      if (verdict === 'gap') {
        gapsDetected += 1;
        gapDetected = true;
        emitGap(onEvent, currentLast, candidate);
        break;
      }
      if (verdict === 'stale') {
        skippedStale += 1;
        continue;
      }
      const outcome = await input.store.applyDepthDiff(candidate);
      if (outcome !== 'applied') {
        gapsDetected += 1;
        gapDetected = true;
        emitGap(onEvent, currentLast, candidate);
        break;
      }
      applied += 1;
      currentLast = candidate.finalUpdateId;
      lastUpdateId = currentLast;
      onEvent({
        kind: 'diff-applied',
        firstUpdateId: candidate.firstUpdateId,
        finalUpdateId: candidate.finalUpdateId,
      });
    }

    if (gapDetected) {
      pending = pending.slice(idx - 1);
      onEvent({ kind: 'resync-started', nextAttempt: attempts + 1 });
      continue;
    }

    return {
      status: 'synced',
      attempts,
      snapshotsFetched,
      bufferedWhileFetching,
      discardedStale,
      skippedStale,
      applied,
      gapsDetected,
      lastUpdateId,
      elapsedMs: nowMs() - startedAtMs,
    };
  }

  return {
    status: 'unsynced',
    reason: lastFailureWasFetch ? 'snapshot-fetch-failed' : 'max-resync-attempts-exceeded',
    attempts,
    snapshotsFetched,
    bufferedWhileFetching,
    discardedStale,
    skippedStale,
    applied,
    gapsDetected,
    lastUpdateId,
    elapsedMs: nowMs() - startedAtMs,
  };
}
