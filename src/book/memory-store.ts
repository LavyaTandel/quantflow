import type { DepthDiff, DepthLevelChange } from '../ingest/types.js';
import { assessContinuity } from './continuity.js';
import type { BookMeta, BookSide, BookStore, DepthApplyResult, Level } from './store.js';

interface SymbolState {
  readonly bids: Map<number, number>;
  readonly asks: Map<number, number>;
  lastUpdateId: number;
}

function absorbChanges(levels: Map<number, number>, changes: DepthLevelChange[]): void {
  for (const change of changes) {
    if (change.newQty === 0) {
      levels.delete(change.price);
    } else {
      levels.set(change.price, change.newQty);
    }
  }
}

function seededSide(levels: Level[]): Map<number, number> {
  const state = new Map<number, number>();
  for (const level of levels) {
    if (level.qty === 0) continue;
    state.set(level.price, level.qty);
  }
  return state;
}

function sortedTop(levels: Map<number, number>, n: number, descending: boolean): Level[] {
  const ordered = [...levels.entries()].sort(([a], [b]) => (descending ? b - a : a - b));
  return ordered.slice(0, n).map(([price, qty]) => ({ price, qty }));
}

export class MemoryBookStore implements BookStore {
  readonly #symbols = new Map<string, SymbolState>();

  async applySnapshot(
    symbol: string,
    bids: Level[],
    asks: Level[],
    lastUpdateId: number,
  ): Promise<void> {
    this.#symbols.set(symbol, {
      bids: seededSide(bids),
      asks: seededSide(asks),
      lastUpdateId,
    });
  }

  async applyDepthDiff(diff: DepthDiff): Promise<DepthApplyResult> {
    const state = this.#symbols.get(diff.symbol);
    if (!state) return 'gap';
    const status = assessContinuity(state.lastUpdateId, diff);
    if (status !== 'applied') return status;
    absorbChanges(
      state.bids,
      diff.levels.filter((change) => change.side === 'bid'),
    );
    absorbChanges(
      state.asks,
      diff.levels.filter((change) => change.side === 'ask'),
    );
    state.lastUpdateId = diff.finalUpdateId;
    return 'applied';
  }

  async topN(symbol: string, n: number): Promise<BookSide> {
    const state = this.#symbols.get(symbol);
    if (!state || n <= 0) return { bids: [], asks: [] };
    return {
      bids: sortedTop(state.bids, n, true),
      asks: sortedTop(state.asks, n, false),
    };
  }

  async getMeta(symbol: string): Promise<BookMeta | undefined> {
    const state = this.#symbols.get(symbol);
    return state ? { lastUpdateId: state.lastUpdateId } : undefined;
  }
}
