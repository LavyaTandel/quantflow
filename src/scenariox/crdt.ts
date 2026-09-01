import * as Automerge from '@automerge/automerge';
import type { Portfolio, Position } from './types.js';

/**
 * CRDTWorkspace — Automerge-backed JSON document for multi-user Portfolio editing.
 *
 * Trade-off: Yjs vs Automerge — Yjs offers rich-text/file awareness and
 * provider ecosystem, but is Type-aware (Y.Map/Y.Array) and heavier for plain JSON.
 * Automerge is JSON-native (plain JS objects), deterministic merge via operation-based CRDT,
 * simpler for Portfolio JSON (positions as map). Chosen Automerge per ADR-0005.
 *
 * Each document holds: { portfolio: Portfolio, positions: Record<positionId, Position> }
 * Merge is `Automerge.merge(docA, docB)` — concurrent Position adds/updates are preserved without loss.
 */

export type ScenarioDoc = Automerge.Doc<{
  portfolio: Portfolio;
  positions: Record<string, Position>;
}>;

export class CRDTWorkspace {
  /**
   * Create a new Automerge doc from a Portfolio snapshot.
   * Positions optionally seeded.
   */
  createDoc(portfolio: Portfolio, positions: Position[] = []): ScenarioDoc {
    let doc = Automerge.init<{ portfolio: Portfolio; positions: Record<string, Position> }>();
    doc = Automerge.change(doc, (d) => {
      d.portfolio = { ...portfolio, createdAt: new Date(portfolio.createdAt) };
      d.positions = {};
      for (const p of positions) {
        d.positions[p.id] = { ...p };
      }
    });
    return doc;
  }

  /** Apply a Position upsert into the doc (immutable — returns new doc). */
  updatePosition(doc: ScenarioDoc, pos: Position): ScenarioDoc {
    return Automerge.change(doc, (d) => {
      d.positions[pos.id] = { ...pos };
    });
  }

  /** Remove a Position from the doc. */
  removePosition(doc: ScenarioDoc, positionId: string): ScenarioDoc {
    return Automerge.change(doc, (d) => {
      delete d.positions[positionId];
    });
  }

  /** Merge two concurrent docs — both sides' Position edits are preserved. */
  merge(docA: ScenarioDoc, docB: ScenarioDoc): ScenarioDoc {
    return Automerge.merge(docA, docB);
  }

  /** Fork helper — clone for concurrent editing (Automerge docs are immutable, clone is identity + handle). */
  clone(doc: ScenarioDoc): ScenarioDoc {
    return Automerge.clone(doc);
  }

  /** Read positions from doc. */
  getPositions(doc: ScenarioDoc): Position[] {
    return Object.values(doc.positions ?? {});
  }

  getPortfolio(doc: ScenarioDoc): Portfolio {
    return doc.portfolio;
  }
}
