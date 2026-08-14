import type { DepthDiff } from '../ingest/types.js';

export type ContinuityStatus = 'applied' | 'stale' | 'gap';

export function assessContinuity(lastUpdateId: number, diff: DepthDiff): ContinuityStatus {
  if (diff.finalUpdateId <= lastUpdateId) return 'stale';
  if (diff.firstUpdateId <= lastUpdateId + 1 && diff.finalUpdateId >= lastUpdateId + 1) {
    return 'applied';
  }
  return 'gap';
}
