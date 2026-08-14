import type { DepthDiff } from '../ingest/types.js';

export interface Level {
  price: number;
  qty: number;
}

export interface BookMeta {
  lastUpdateId: number;
}

export interface BookSide {
  bids: Level[];
  asks: Level[];
}

export type DepthApplyResult = 'applied' | 'stale' | 'gap';

export interface BookStore {
  applySnapshot(symbol: string, bids: Level[], asks: Level[], lastUpdateId: number): Promise<void>;
  applyDepthDiff(diff: DepthDiff): Promise<DepthApplyResult>;
  topN(symbol: string, n: number): Promise<BookSide>;
  getMeta(symbol: string): Promise<BookMeta | undefined>;
}
