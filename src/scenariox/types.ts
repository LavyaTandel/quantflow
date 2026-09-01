/**
 * ScenarioX domain types — distinct from Ledger Account cash (ADR-0005).
 * Portfolio/Position are simulation holdings; PnL is derived from market ticks,
 * no automatic ledger Transaction is created. Settlement requires explicit
 * Ledger append.
 */

export interface Portfolio {
  id: string;
  tenantId: string;
  name: string;
  createdAt: Date;
}

export interface Position {
  id: string;
  portfolioId: string;
  tenantId: string;
  symbol: string;
  quantity: number;
  avgEntryPrice: number;
  side: 'long' | 'short';
}

export interface MacroShock {
  description: string;
  embedding?: number[];
}

export interface Scenario {
  id: string;
  name: string;
  shocks: MacroShock[];
}

export interface RiskResult {
  var95: number;
  var99: number;
  sharpe: number;
  horizonDays: 1;
}

export type RiskEngineKind = 'historical' | 'monte-carlo';
