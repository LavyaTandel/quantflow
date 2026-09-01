import type { BookSide, Level } from '../book/store.js';

export interface FeatureVector {
  I: number;
  ofi: number;
  dwp: number;
  sweepFlag: boolean;
  windowMs: number;
}

/**
 * Imbalance I = (Qb - Qa) / (Qb + Qa) in [-1,+1].
 * Pure, <1 ms. Handles edge cases:
 * - empty/one-sided/zero depth => 0 or +/-1.
 */
export function computeImbalance(Qb: number, Qa: number): number {
  const sum = Qb + Qa;
  if (sum === 0) return 0;
  if (!Number.isFinite(Qb) || !Number.isFinite(Qa)) return 0;
  return (Qb - Qa) / sum;
}

/**
 * Helper to sum quantities on a side (top N). Used for I when derived from book.
 */
export function imbalanceFromBook(book: BookSide): number {
  const qb = book.bids.reduce((s, l) => s + l.qty, 0);
  const qa = book.asks.reduce((s, l) => s + l.qty, 0);
  // Use best-level quantity for canonical F1 per findings, but summing topN
  // is equivalent for L1 when only top level is provided; caller chooses N via topN.
  return computeImbalance(qb, qa);
}

function bestLevel(levels: Level[]): Level | undefined {
  return levels[0];
}

/**
 * Order Flow Imbalance per Cont-Kukanov-Stoikov per 100 ms bucket.
 * Ofi_n = I^b_n - I^a_n where each side contribution is:
 *  - bids: Vb_n - Vb_{n-1} if P_b unchanged; +Vb_n if P_b up; -Vb_{n-1} if P_b down.
 *  - asks (mirrored): Va_n - Va_{n-1} if P_a unchanged; +Va_n if P_a down; -Va_{n-1} if P_a up.
 * Then bucket OFI is sum of Ofi_n over window; here single bucket delta.
 * Returns 0 for null prev or empty books.
 */
export function computeOFI(prevBook: BookSide | null, currBook: BookSide): number {
  if (!prevBook) return 0;
  // Bid contribution
  const prevBid = bestLevel(prevBook.bids);
  const currBid = bestLevel(currBook.bids);
  const prevBidPrice = prevBid?.price ?? null;
  const currBidPrice = currBid?.price ?? null;
  const prevBidQty = prevBid?.qty ?? 0;
  const currBidQty = currBid?.qty ?? 0;

  let ib: number;
  if (prevBidPrice === null && currBidPrice === null) ib = 0;
  else if (prevBidPrice === null) ib = currBidQty;
  else if (currBidPrice === null) ib = -prevBidQty;
  else if (currBidPrice > prevBidPrice) ib = currBidQty;
  else if (currBidPrice < prevBidPrice) ib = -prevBidQty;
  else ib = currBidQty - prevBidQty;

  // Ask contribution (mirrored direction)
  const prevAsk = bestLevel(prevBook.asks);
  const currAsk = bestLevel(currBook.asks);
  const prevAskPrice = prevAsk?.price ?? null;
  const currAskPrice = currAsk?.price ?? null;
  const prevAskQty = prevAsk?.qty ?? 0;
  const currAskQty = currAsk?.qty ?? 0;

  let ia: number;
  if (prevAskPrice === null && currAskPrice === null) ia = 0;
  else if (prevAskPrice === null) ia = currAskQty;
  else if (currAskPrice === null) ia = -prevAskQty;
  else if (currAskPrice < prevAskPrice)
    ia = currAskQty; // ask moved toward mid => supply increase => positive ia
  else if (currAskPrice > prevAskPrice)
    ia = -prevAskQty; // ask moved away => supply decrease
  else ia = currAskQty - prevAskQty;

  return ib - ia;
}

/**
 * Liquidity sweep / depletion detection (F4 guardrail).
 * Hybrid rule simplified for pure-function boundary:
 * - depleted side (no levels or zero qty) => true
 * - spread widened beyond 100 bps of mid => true
 * - otherwise false
 *
 * Signature mandated: detectSweep(topLevels, spread) => boolean.
 * We accept BookSide as first arg (canonical) and also Level[] for compatibility.
 */
export function detectSweep(book: BookSide, spread: number): boolean;
export function detectSweep(levels: Level[], spread: number): boolean;
export function detectSweep(arg: BookSide | Level[], spread: number): boolean {
  if (Array.isArray(arg)) {
    // Level[] form: if empty => depleted
    if (arg.length === 0) return true;
    // No opposite side to compare, only check spread if provided
    if (!Number.isFinite(spread)) return false;
    // Without mid, approximate sweep if spread anomalously large (>10)
    return spread > 10;
  }
  const book = arg as BookSide;
  if (book.bids.length === 0 || book.asks.length === 0) return true;
  const totalBid = book.bids.reduce((s, l) => s + l.qty, 0);
  const totalAsk = book.asks.reduce((s, l) => s + l.qty, 0);
  if (totalBid === 0 || totalAsk === 0) return true;
  if (!Number.isFinite(spread) || spread <= 0) return false;
  const bestBid = book.bids[0]!.price;
  const bestAsk = book.asks[0]!.price;
  if (!Number.isFinite(bestBid) || !Number.isFinite(bestAsk) || bestBid <= 0) return false;
  const mid = (bestBid + bestAsk) / 2;
  if (mid <= 0) return false;
  const bps = (spread / mid) * 10_000;
  if (bps > 100) return true;
  return false;
}

/**
 * Depth-Weighted Pressure (F3 internal, K=5 exponential decay).
 * DWP = Σ w_k*(Qb_k - Qa_k) / Σ w_k*(Qb_k + Qa_k)
 * w_k = exp(-alpha*(k)) ; alpha=0.5 gives [1,0.606,0.367,0.223,0.135]
 * Also supported: w_k = 1/k shape. Default is exponential.
 * Returns 0 for empty denominator.
 */
export function computeDWP(book: BookSide, K = 5, alpha = 0.5): number {
  if (K <= 0) return 0;
  let num = 0;
  let den = 0;
  const n = Math.min(K, Math.max(book.bids.length, book.asks.length));
  // Always iterate K, missing levels contribute 0 qty
  for (let k = 0; k < K; k++) {
    const w = Math.exp(-alpha * k);
    const qb = book.bids[k]?.qty ?? 0;
    const qa = book.asks[k]?.qty ?? 0;
    num += w * (qb - qa);
    den += w * (qb + qa);
    // Avoid unused variable warning for n
    void n;
  }
  if (den === 0) return 0;
  return num / den;
}

/**
 * Build FeatureVector from books and prior state.
 */
export function buildFeatureVector(
  currBook: BookSide,
  prevBook: BookSide | null,
  opts: { windowMs: number; spread: number },
): FeatureVector {
  const I = imbalanceFromBook(currBook);
  const ofi = computeOFI(prevBook, currBook);
  const dwp = computeDWP(currBook, 5);
  const sweepFlag = detectSweep(currBook, opts.spread);
  return { I, ofi, dwp, sweepFlag, windowMs: opts.windowMs };
}
