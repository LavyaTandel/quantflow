export * from './types.js';
export * from './normalize.js';
export * from './pipeline.js';
export * from './replay.js';
export * from './binance-socket.js';

import {
  BinanceMarketSocket,
  type BinanceSocketConfig,
  type SocketStats,
} from './binance-socket.js';
import { EventPipeline, type PipelineCounters } from './pipeline.js';
import { normalizeFrame } from './normalize.js';

export interface IngestionConfig extends BinanceSocketConfig {
  pipelineCapacity?: number;
}

export interface IngestionStats extends SocketStats {
  droppedUnknownFrames: number;
  pipeline: PipelineCounters;
}

export interface Ingestion {
  readonly socket: BinanceMarketSocket;
  readonly pipeline: EventPipeline;
  start(): void;
  stop(): void;
  stats(): IngestionStats;
}

export function createIngestion(config: IngestionConfig = {}): Ingestion {
  const { pipelineCapacity, ...socketConfig } = config;
  const socket = new BinanceMarketSocket(socketConfig);
  const pipeline = new EventPipeline({ capacity: pipelineCapacity });
  let droppedUnknownFrames = 0;

  socket.on('frame', (frame) => {
    const outcome = normalizeFrame(frame, { nowMs: Date.now() });
    if (outcome.droppedUnknown) droppedUnknownFrames += 1;
    pipeline.pushMany(outcome.events);
  });

  return {
    socket,
    pipeline,
    start() {
      socket.start();
    },
    stop() {
      socket.stop();
      pipeline.close();
    },
    stats() {
      return { ...socket.getStats(), droppedUnknownFrames, pipeline: pipeline.counters() };
    },
  };
}
