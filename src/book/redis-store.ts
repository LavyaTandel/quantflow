import type { DepthDiff } from '../ingest/types.js';
import { assessContinuity } from './continuity.js';
import type { BookMeta, BookSide, BookStore, DepthApplyResult, Level } from './store.js';

export interface BookRedisMulti {
  zAdd(key: string, members: Array<{ score: number; value: string }>): BookRedisMulti;
  zRem(key: string, members: string[]): BookRedisMulti;
  hSet(key: string, fieldValue: Record<string, string>): BookRedisMulti;
  hDel(key: string, fields: string[]): BookRedisMulti;
  del(keys: string[]): BookRedisMulti;
  exec(): Promise<unknown>;
}

export interface BookRedisClient {
  multi(): BookRedisMulti;
  del(keys: string[]): Promise<number>;
  hGetAll(key: string): Promise<Record<string, string>>;
  zRangeWithScores(
    key: string,
    min: number,
    max: number,
    options?: { REV?: boolean },
  ): Promise<Array<{ value: string; score: number }>>;
}

const bidsKeyOf = (symbol: string) => `book:${symbol}:bids`;
const asksKeyOf = (symbol: string) => `book:${symbol}:asks`;
const qtyKeyOf = (symbol: string) => `book:${symbol}:qty`;
const metaKeyOf = (symbol: string) => `book:${symbol}:meta`;

function sideKeyOf(symbol: string, side: 'bid' | 'ask'): string {
  return side === 'bid' ? bidsKeyOf(symbol) : asksKeyOf(symbol);
}

export interface RedisBookStoreOptions {
  continuityCache?: boolean;
}

export class RedisBookStore implements BookStore {
  readonly #client: BookRedisClient;
  readonly #continuityCache: boolean;
  readonly #lastUpdateIds = new Map<string, number>();

  constructor(client: BookRedisClient, options: RedisBookStoreOptions = {}) {
    this.#client = client;
    this.#continuityCache = options.continuityCache ?? true;
  }

  #cachedLastUpdateId(symbol: string): number | undefined {
    return this.#continuityCache ? this.#lastUpdateIds.get(symbol) : undefined;
  }

  #rememberLastUpdateId(symbol: string, lastUpdateId: number): void {
    if (this.#continuityCache) this.#lastUpdateIds.set(symbol, lastUpdateId);
  }

  async applySnapshot(
    symbol: string,
    bids: Level[],
    asks: Level[],
    lastUpdateId: number,
  ): Promise<void> {
    const bidQty: Record<string, string> = {};
    const askQty: Record<string, string> = {};
    const tx = this.#client.multi();
    tx.del([bidsKeyOf(symbol), asksKeyOf(symbol), qtyKeyOf(symbol)]);
    for (const level of bids) {
      if (level.qty === 0) continue;
      tx.zAdd(bidsKeyOf(symbol), [{ score: level.price, value: String(level.price) }]);
      bidQty[String(level.price)] = String(level.qty);
    }
    for (const level of asks) {
      if (level.qty === 0) continue;
      tx.zAdd(asksKeyOf(symbol), [{ score: level.price, value: String(level.price) }]);
      askQty[String(level.price)] = String(level.qty);
    }
    tx.hSet(qtyKeyOf(symbol), bidQty);
    tx.hSet(qtyKeyOf(symbol), askQty);
    tx.hSet(metaKeyOf(symbol), { lastUpdateId: String(lastUpdateId) });
    await tx.exec();
    this.#rememberLastUpdateId(symbol, lastUpdateId);
  }

  async applyDepthDiff(diff: DepthDiff): Promise<DepthApplyResult> {
    let current = this.#cachedLastUpdateId(diff.symbol);
    if (current === undefined) {
      // SINGLE-WRITER ASSUMPTION: exactly one process owns this symbol's book, so the lazily
      // hydrated lastUpdateId cannot race a concurrent foreign writer. Multi-instance deployments
      // need an owner model (e.g. leader election per symbol) instead of this cache.
      const meta = await this.#client.hGetAll(metaKeyOf(diff.symbol));
      const stored = meta['lastUpdateId'];
      if (stored === undefined) return 'gap';
      current = Number(stored);
      this.#rememberLastUpdateId(diff.symbol, current);
    }
    const status = assessContinuity(current, diff);
    if (status !== 'applied') return status;
    const tx = this.#client.multi();
    for (const level of diff.levels) {
      const sideKey = sideKeyOf(diff.symbol, level.side);
      const price = String(level.price);
      if (level.newQty === 0) {
        tx.zRem(sideKey, [price]);
        tx.hDel(qtyKeyOf(diff.symbol), [price]);
      } else {
        tx.zAdd(sideKey, [{ score: level.price, value: price }]);
        tx.hSet(qtyKeyOf(diff.symbol), { [price]: String(level.newQty) });
      }
    }
    tx.hSet(metaKeyOf(diff.symbol), { lastUpdateId: String(diff.finalUpdateId) });
    await tx.exec();
    this.#rememberLastUpdateId(diff.symbol, diff.finalUpdateId);
    return 'applied';
  }

  async topN(symbol: string, n: number): Promise<BookSide> {
    if (!(n > 0)) return { bids: [], asks: [] };
    const [rawBids, rawAsks, qtys] = await Promise.all([
      this.#client.zRangeWithScores(bidsKeyOf(symbol), 0, n - 1, { REV: true }),
      this.#client.zRangeWithScores(asksKeyOf(symbol), 0, n - 1),
      this.#client.hGetAll(qtyKeyOf(symbol)),
    ]);
    const toLevel = (member: { value: string; score: number }): Level => ({
      price: member.score,
      qty: Number(qtys[member.value] ?? '0'),
    });
    return { bids: rawBids.map(toLevel), asks: rawAsks.map(toLevel) };
  }

  async getMeta(symbol: string): Promise<BookMeta | undefined> {
    const cached = this.#cachedLastUpdateId(symbol);
    if (cached !== undefined) return { lastUpdateId: cached };
    const meta = await this.#client.hGetAll(metaKeyOf(symbol));
    const lastUpdateId = meta['lastUpdateId'];
    if (lastUpdateId === undefined) return undefined;
    const parsed = Number(lastUpdateId);
    this.#rememberLastUpdateId(symbol, parsed);
    return { lastUpdateId: parsed };
  }
}
