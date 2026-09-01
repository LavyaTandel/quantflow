import { randomUUID } from 'node:crypto';
import type { Portfolio, Position } from './types.js';

/**
 * ScenarioStore — per-tenant Portfolio/Position CRUD.
 *
 * Invariants:
 * - Compound key (tenantId, portfolioId) — every read filtered by tenantId.
 * - Portfolio/Position are distinct from Ledger Account cash; no ledger Transaction side-effect.
 * - Positions store tenantId redundantly to enforce isolation without extra lookup.
 */

export interface ScenarioStore {
  createPortfolio(tenantId: string, name: string): Promise<Portfolio>;
  addPosition(
    portfolioId: string,
    pos: Omit<Position, 'id' | 'portfolioId' | 'tenantId'> & { tenantId: string },
  ): Promise<Position>;
  listPortfolios(tenantId: string): Promise<Portfolio[]>;
  getPortfolio(tenantId: string, portfolioId: string): Promise<Portfolio | undefined>;
  listPositions(tenantId: string, portfolioId: string): Promise<Position[]>;
  getPosition(
    tenantId: string,
    portfolioId: string,
    positionId: string,
  ): Promise<Position | undefined>;
  updatePosition(
    tenantId: string,
    portfolioId: string,
    positionId: string,
    patch: Partial<Pick<Position, 'quantity' | 'avgEntryPrice' | 'side' | 'symbol'>>,
  ): Promise<Position | undefined>;
  removePosition(tenantId: string, portfolioId: string, positionId: string): Promise<boolean>;
}

export class MemoryScenarioStore implements ScenarioStore {
  private readonly portfolios = new Map<string, Portfolio>();
  // key: portfolioId -> positions
  private readonly positionsByPortfolio = new Map<string, Map<string, Position>>();

  private portfolioKey(tenantId: string, portfolioId: string): string {
    return `${tenantId}:${portfolioId}`;
  }

  async createPortfolio(tenantId: string, name: string): Promise<Portfolio> {
    const id = randomUUID();
    const portfolio: Portfolio = {
      id,
      tenantId,
      name,
      createdAt: new Date(),
    };
    this.portfolios.set(this.portfolioKey(tenantId, id), { ...portfolio });
    this.positionsByPortfolio.set(id, new Map<string, Position>());
    return { ...portfolio };
  }

  async addPosition(
    portfolioId: string,
    pos: Omit<Position, 'id' | 'portfolioId' | 'tenantId'> & { tenantId: string },
  ): Promise<Position> {
    // Find portfolio by id across tenants — need tenantId from pos to validate ownership
    const { tenantId, symbol, quantity, avgEntryPrice, side } = pos;
    const portfolio = await this.getPortfolio(tenantId, portfolioId);
    if (!portfolio) throw new Error(`portfolio not found: ${portfolioId} for tenant ${tenantId}`);
    const id = randomUUID();
    const position: Position = {
      id,
      portfolioId,
      tenantId,
      symbol,
      quantity,
      avgEntryPrice,
      side,
    };
    const map = this.positionsByPortfolio.get(portfolioId);
    if (!map) throw new Error(`portfolio positions map missing: ${portfolioId}`);
    map.set(id, { ...position });
    return { ...position };
  }

  async listPortfolios(tenantId: string): Promise<Portfolio[]> {
    const out: Portfolio[] = [];
    for (const p of this.portfolios.values()) {
      if (p.tenantId === tenantId) out.push({ ...p, createdAt: new Date(p.createdAt) });
    }
    return out;
  }

  async getPortfolio(tenantId: string, portfolioId: string): Promise<Portfolio | undefined> {
    const found = this.portfolios.get(this.portfolioKey(tenantId, portfolioId));
    if (!found) return undefined;
    return { ...found, createdAt: new Date(found.createdAt) };
  }

  async listPositions(tenantId: string, portfolioId: string): Promise<Position[]> {
    const portfolio = await this.getPortfolio(tenantId, portfolioId);
    if (!portfolio) return [];
    const map = this.positionsByPortfolio.get(portfolioId);
    if (!map) return [];
    return [...map.values()].filter((p) => p.tenantId === tenantId).map((p) => ({ ...p }));
  }

  async getPosition(
    tenantId: string,
    portfolioId: string,
    positionId: string,
  ): Promise<Position | undefined> {
    const portfolio = await this.getPortfolio(tenantId, portfolioId);
    if (!portfolio) return undefined;
    const map = this.positionsByPortfolio.get(portfolioId);
    const pos = map?.get(positionId);
    if (!pos) return undefined;
    if (pos.tenantId !== tenantId) return undefined;
    return { ...pos };
  }

  async updatePosition(
    tenantId: string,
    portfolioId: string,
    positionId: string,
    patch: Partial<Pick<Position, 'quantity' | 'avgEntryPrice' | 'side' | 'symbol'>>,
  ): Promise<Position | undefined> {
    const existing = await this.getPosition(tenantId, portfolioId, positionId);
    if (!existing) return undefined;
    const map = this.positionsByPortfolio.get(portfolioId);
    if (!map) return undefined;
    const updated: Position = {
      ...existing,
      ...patch,
      id: existing.id,
      portfolioId: existing.portfolioId,
      tenantId: existing.tenantId,
    };
    map.set(positionId, { ...updated });
    return { ...updated };
  }

  async removePosition(
    tenantId: string,
    portfolioId: string,
    positionId: string,
  ): Promise<boolean> {
    const existing = await this.getPosition(tenantId, portfolioId, positionId);
    if (!existing) return false;
    const map = this.positionsByPortfolio.get(portfolioId);
    if (!map) return false;
    return map.delete(positionId);
  }
}
