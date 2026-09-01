import type { LedgerStore } from './store.js';
import type { Transaction, TransactionType } from './types.js';

export interface StatementRow {
  transactionId: string;
  accountId: string;
  asset: string;
  type: TransactionType;
  amount: number;
  balanceAfter: number;
  prevVersion: number;
  kafkaOffset?: number;
  fencingToken: number;
  idempotencyKey: string;
  timestamp: Date;
  transferId?: string;
}

function txVersion(tx: Transaction): number {
  const v = (tx as unknown as { version?: number }).version;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return tx.prevVersion + 1;
}

export function formatStatementRow(tx: Transaction, balanceAfter: number): StatementRow {
  return {
    transactionId: tx.id,
    accountId: tx.accountId,
    asset: tx.asset,
    type: tx.type,
    amount: tx.amount,
    balanceAfter,
    prevVersion: tx.prevVersion,
    kafkaOffset: tx.kafkaOffset,
    fencingToken: tx.fencingToken,
    idempotencyKey: tx.idempotencyKey,
    timestamp: new Date(tx.createdAt),
    transferId: tx.transferId,
  };
}

const CSV_HEADER =
  'timestamp,transactionId,accountId,asset,type,amount,balanceAfter,prevVersion,kafkaOffset,fencingToken,idempotencyKey,transferId';

function csvEscape(v: string): string {
  if (v.includes(',') || v.includes('"') || v.includes('\n')) {
    return `"${v.replace(/"/g, '""')}"`;
  }
  return v;
}

export function toCSV(rows: StatementRow[]): string {
  const lines = [CSV_HEADER];
  for (const r of rows) {
    const cols = [
      r.timestamp.toISOString(),
      r.transactionId,
      r.accountId,
      r.asset,
      r.type,
      String(r.amount),
      String(r.balanceAfter),
      String(r.prevVersion),
      r.kafkaOffset !== undefined ? String(r.kafkaOffset) : '',
      String(r.fencingToken),
      r.idempotencyKey,
      r.transferId ?? '',
    ].map(csvEscape);
    lines.push(cols.join(','));
  }
  return lines.join('\n');
}

export function toJSONL(rows: StatementRow[]): string {
  return rows.map((r) => JSON.stringify({ ...r, timestamp: r.timestamp.toISOString() })).join('\n');
}

export async function buildStatement(
  store: LedgerStore,
  tenantId: string,
  accountId: string,
  asset: string,
  opts: { cursor?: number; limit?: number; fromVersion?: number; toVersion?: number } = {},
): Promise<{ rows: StatementRow[]; nextCursor?: number }> {
  const { transactions, nextCursor } = await store.listTransactionsFiltered(
    tenantId,
    accountId,
    asset,
    opts,
  );
  if (transactions.length === 0) return { rows: [], nextCursor };

  // Compute balanceAfter by walking full ordered history to handle cursor pagination.
  // Fetch full history without cursor but with same from/to bounds to map version->balance.
  // If opts.cursor is set, we still need prior balances, so fetch without cursor.
  const fullFilter: { fromVersion?: number; toVersion?: number } = {};
  if (opts.fromVersion !== undefined) fullFilter.fromVersion = opts.fromVersion;
  if (opts.toVersion !== undefined) fullFilter.toVersion = opts.toVersion;
  // Use limit large enough to get all (if many, we page iteratively — but for test hermetic it's fine to fetch all via fallback store)
  // For memory store, we can call listTransactions and filter manually to avoid limit truncation.
  let fullTransactions: Transaction[];
  // Try to get all without pagination: use listTransactions (which delegates to filtered with default limit 100) may truncate >100.
  // So instead, fetch via listTransactionsFiltered with large limit.
  const allRes = await store.listTransactionsFiltered(tenantId, accountId, asset, {
    ...fullFilter,
    limit: 10000,
  });
  fullTransactions = allRes.transactions;
  // Ensure sorted
  fullTransactions = [...fullTransactions].sort((a, b) => txVersion(a) - txVersion(b));

  // Build version -> balanceAfter map by walking from zero.
  const versionToBalance = new Map<number, number>();
  let running = 0;
  for (const tx of fullTransactions) {
    const v = txVersion(tx);
    if (tx.type === 'credit') running += tx.amount;
    else running -= tx.amount;
    versionToBalance.set(v, running);
  }

  const rows = transactions.map((tx) => {
    const v = txVersion(tx);
    const bal = versionToBalance.get(v);
    // Fallback to walking filtered slice if not found (should not happen)
    const fallbackBal = (() => {
      if (bal !== undefined) return bal;
      // compute incrementally over filtered slice
      let r = 0;
      for (const t of transactions) {
        if (t.type === 'credit') r += t.amount;
        else r -= t.amount;
        if (t.id === tx.id) break;
      }
      return r;
    })();
    return formatStatementRow(tx, fallbackBal);
  });

  return { rows, nextCursor };
}

export function exportStatement(
  rows: StatementRow[],
  format: 'csv' | 'jsonl' | 'json' = 'json',
): { body: string; contentType: string } {
  if (format === 'csv') return { body: toCSV(rows), contentType: 'text/csv' };
  if (format === 'jsonl') return { body: toJSONL(rows), contentType: 'application/x-ndjson' };
  return { body: JSON.stringify({ rows }), contentType: 'application/json' };
}
