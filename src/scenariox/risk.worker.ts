import { parentPort } from 'node:worker_threads';
import { historicalVaR } from './risk.js';
import type { Portfolio, Position, RiskEngineKind } from './types.js';

interface WorkerPayload {
  portfolio: Portfolio;
  positions: Position[];
  historicalReturnsEntries: Array<[string, number[]]>;
  engine: RiskEngineKind;
}

if (parentPort) {
  parentPort.on('message', (msg: WorkerPayload) => {
    try {
      const historicalReturns = new Map<string, number[]>(msg.historicalReturnsEntries);
      // Revive dates that were structured-cloned (portfolio.createdAt may be string after serialize)
      const portfolio = {
        ...msg.portfolio,
        createdAt: new Date(msg.portfolio.createdAt),
      } as Portfolio;
      const result = historicalVaR(portfolio, msg.positions, historicalReturns, msg.engine);
      parentPort!.postMessage(result);
    } catch (err) {
      parentPort!.postMessage({
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });
}
