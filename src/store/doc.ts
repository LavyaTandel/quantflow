import type { EventKind, NormalizedEvent } from '../ingest/types.js';

export interface TickDocBase {
  ts: Date;
  symbol: string;
  kind: EventKind;
}

export interface TradeTickDoc extends TickDocBase {
  kind: 'trade';
  price: number;
  qty: number;
  tradeId: string;
}

export interface BboTickDoc extends TickDocBase {
  kind: 'bbo';
  bidPrice: number;
  bidQty: number;
  askPrice: number;
  askQty: number;
}

export interface DepthTickDoc extends TickDocBase {
  kind: 'depth';
  nLevels: number;
}

export type TickDoc = TradeTickDoc | BboTickDoc | DepthTickDoc;

export function toDoc(event: NormalizedEvent): TickDoc {
  switch (event.kind) {
    case 'trade':
      return {
        ts: new Date(event.ts),
        symbol: event.symbol,
        kind: 'trade',
        price: event.price,
        qty: event.qty,
        tradeId: event.tradeId,
      };
    case 'bbo':
      return {
        ts: new Date(event.ts),
        symbol: event.symbol,
        kind: 'bbo',
        bidPrice: event.bidPrice,
        bidQty: event.bidQty,
        askPrice: event.askPrice,
        askQty: event.askQty,
      };
    case 'depth':
      return {
        ts: new Date(event.ts),
        symbol: event.symbol,
        kind: 'depth',
        nLevels: event.levels.length,
      };
  }
}
