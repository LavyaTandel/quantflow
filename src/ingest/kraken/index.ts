export * from './crc32.js';
export * from './normalize.js';
export * from './kraken-socket.js';

import type { PipelineCounters } from '../pipeline.js';
import { EventPipeline } from '../pipeline.js';
import type { SocketStats } from '../binance-socket.js';
import { KrakenMarketSocket, type KrakenSocketConfig } from './kraken-socket.js';
import { KrakenNormalizer } from './normalize.js';

export interface KrakenIngestionConfig extends KrakenSocketConfig {
  pipelineCapacity?: number;
}

export interface KrakenIngestionStats extends SocketStats {
  pingsSent: number;
  droppedUnknownFrames: number;
  checksumFailures: number;
  pipeline: PipelineCounters;
}

export interface KrakenIngestion {
  readonly socket: KrakenMarketSocket;
  readonly pipeline: EventPipeline;
  readonly normalizer: KrakenNormalizer;
  start(): void;
  stop(): void;
  stats(): KrakenIngestionStats;
}

export function createKrakenIngestion(config: KrakenIngestionConfig = {}): KrakenIngestion {
  const { pipelineCapacity, ...socketConfig } = config;
  const socket = new KrakenMarketSocket(socketConfig);
  const normalizer = new KrakenNormalizer({ depth: socketConfig.depth ?? 100 });
  const pipeline = new EventPipeline({ capacity: pipelineCapacity });
  let droppedUnknownFrames = 0;

  socket.on('frame', (frame) => {
    const outcome = normalizer.normalize(frame, { nowMs: Date.now() });
    if (outcome.droppedUnknown) droppedUnknownFrames += 1;
    pipeline.pushMany(outcome.events);
  });

  return {
    socket,
    pipeline,
    normalizer,
    start() {
      socket.start();
    },
    stop() {
      socket.stop();
      pipeline.close();
    },
    stats() {
      return {
        ...socket.getStats(),
        droppedUnknownFrames,
        checksumFailures: normalizer.counters().checksumFailures,
        pipeline: pipeline.counters(),
      };
    },
  };
}
