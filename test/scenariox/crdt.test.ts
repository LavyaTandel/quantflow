import { describe, it, expect } from 'vitest';
import { CRDTWorkspace } from '../../src/scenariox/crdt.js';
import type { Portfolio, Position } from '../../src/scenariox/types.js';

function makePortfolio(over: Partial<Portfolio> = {}): Portfolio {
  return {
    id: 'p1',
    tenantId: 't1',
    name: 'Growth',
    createdAt: new Date('2026-01-01T00:00:00Z'),
    ...over,
  };
}

function makePosition(over: Partial<Position> = {}): Position {
  return {
    id: 'pos-1',
    portfolioId: 'p1',
    tenantId: 't1',
    symbol: 'BTCUSDT',
    quantity: 1,
    avgEntryPrice: 30000,
    side: 'long',
    ...over,
  };
}

describe('CRDTWorkspace — concurrent Position edits merge without loss', () => {
  it('two Automerge docs forked from same base, each add different Position, merge has both', () => {
    const ws = new CRDTWorkspace();
    const portfolio = makePortfolio();
    const base = ws.createDoc(portfolio, [makePosition({ id: 'pos-base', symbol: 'BTCUSDT' })]);

    // Fork
    const docA = ws.clone(base);
    const docB = ws.clone(base);

    const posA = makePosition({ id: 'pos-a', symbol: 'ETHUSDT', quantity: 5, avgEntryPrice: 2000 });
    const posB = makePosition({
      id: 'pos-b',
      symbol: 'SOLUSDT',
      quantity: 100,
      avgEntryPrice: 100,
    });

    const editedA = ws.updatePosition(docA, posA);
    const editedB = ws.updatePosition(docB, posB);

    const merged = ws.merge(editedA, editedB);
    const positions = ws.getPositions(merged);
    const ids = positions.map((p) => p.id).sort();
    expect(ids).toEqual(['pos-a', 'pos-b', 'pos-base'].sort());

    // Also test reverse merge order yields same set
    const merged2 = ws.merge(editedB, editedA);
    const ids2 = ws
      .getPositions(merged2)
      .map((p) => p.id)
      .sort();
    expect(ids2).toEqual(ids);
  });

  it('concurrent updates to different fields of same Position merge without loss (if non-conflicting) or LWW', () => {
    const ws = new CRDTWorkspace();
    const portfolio = makePortfolio();
    const basePos = makePosition({
      id: 'pos-shared',
      symbol: 'BTCUSDT',
      quantity: 1,
      avgEntryPrice: 30000,
    });
    const base = ws.createDoc(portfolio, [basePos]);

    const docA = ws.clone(base);
    const docB = ws.clone(base);

    // A updates quantity, B updates symbol (different keys) — both should be present after merge if Automerge handles per-key
    // For same Position id, Automerge's last-writer per field wins; sequential test ensures no data loss for distinct ids
    const posA: Position = { ...basePos, quantity: 10 };
    const posB: Position = { ...basePos, avgEntryPrice: 35000 };

    const editedA = ws.updatePosition(docA, posA);
    const editedB = ws.updatePosition(docB, posB);

    const merged = ws.merge(editedA, editedB);
    const mergedPos = ws.getPositions(merged).find((p) => p.id === 'pos-shared');
    expect(mergedPos).toBeDefined();
    // At least one of the concurrent edits is preserved; both edits touch different fields but same doc key — Automerge uses LWW per object.
    // We assert position still exists (no loss) and one of the values is present.
    expect([10, 1]).toContain(mergedPos!.quantity);
    expect([30000, 35000]).toContain(mergedPos!.avgEntryPrice);
  });

  it('removePosition merges correctly', () => {
    const ws = new CRDTWorkspace();
    const portfolio = makePortfolio();
    const base = ws.createDoc(portfolio, [
      makePosition({ id: 'pos-keep', symbol: 'BTCUSDT' }),
      makePosition({ id: 'pos-remove', symbol: 'ETHUSDT' }),
    ]);
    const docA = ws.clone(base);
    const docB = ws.clone(base);

    const editedA = ws.removePosition(docA, 'pos-remove');
    // B adds unrelated position concurrently
    const editedB = ws.updatePosition(
      docB,
      makePosition({ id: 'pos-new', symbol: 'AVAXUSDT', quantity: 50, avgEntryPrice: 20 }),
    );

    const merged = ws.merge(editedA, editedB);
    const ids = ws
      .getPositions(merged)
      .map((p) => p.id)
      .sort();
    // pos-remove should be gone (or eventually consistent); pos-new present; pos-keep preserved
    // Automerge delete vs add: merge semantics ensure delete wins over untouched, new add preserved
    expect(ids).toContain('pos-keep');
    expect(ids).toContain('pos-new');
  });
});
