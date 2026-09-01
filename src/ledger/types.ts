/**
 * Ledger domain types — locked per Q1 A / Q2 A (ADR-0004).
 * Single-asset Ledger Account key (tenantId, accountId, asset) + monotonic version.
 */

export interface LedgerAccount {
  tenantId: string;
  accountId: string;
  asset: string;
  balance: number;
  version: number;
  createdAt: Date;
  updatedAt: Date;
  /** last applied fencing token for this account-asset */
  lastFencingToken: number;
}

export type TransactionType = 'debit' | 'credit' | 'transfer';

export interface Transaction {
  id: string;
  tenantId: string;
  accountId: string;
  asset: string;
  type: TransactionType;
  amount: number;
  /** expected version at append time — CAS guard */
  prevVersion: number;
  /** Kafka log offset after produce (optional until materialized) */
  kafkaOffset?: number;
  idempotencyKey: string;
  fencingToken: number;
  /** for transfer: two linked rows share transferId */
  transferId?: string;
  createdAt: Date;
}

export interface AppendInput {
  tenantId: string;
  accountId: string;
  asset: string;
  type: TransactionType;
  amount: number;
  idempotencyKey: string;
  transferId?: string;
  /** optional explicit CAS guard — if omitted service reads current version */
  expectedVersion?: number;
}

export type AppendResult =
  | { account: LedgerAccount; transaction: Transaction }
  | { conflict: { currentVersion: number; currentBalance: number } };
