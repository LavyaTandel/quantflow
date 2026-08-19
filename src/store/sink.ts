import { MongoClient } from 'mongodb';
import type { MongoClientOptions } from 'mongodb';
import type { TickDoc } from './doc.js';

export interface Sink {
  insertMany(docs: readonly TickDoc[]): Promise<void>;
}

// Pool assumptions (see findings/06 P1): the Node driver's wait queue is unbounded
// and waitQueueTimeoutMS defaults to 0 = wait forever, so the writer must bound both
// sides. This client sets an explicit 10s wait-queue timeout; FlushService caps
// in-flight flushes at maxInFlight (default 2), each insertMany holding at most one
// pooled connection, far below maxPoolSize=100 — a single writer can never exhaust
// the pool on its own.
export const DEFAULT_WAIT_QUEUE_TIMEOUT_MS = 10_000;
export const ASSUMED_MAX_POOL_SIZE = 100;

export function createMongoClient(url: string, overrides: MongoClientOptions = {}): MongoClient {
  return new MongoClient(url, {
    maxPoolSize: ASSUMED_MAX_POOL_SIZE,
    waitQueueTimeoutMS: DEFAULT_WAIT_QUEUE_TIMEOUT_MS,
    ...overrides,
  });
}

export function mongoSink(client: MongoClient, dbName: string, collectionName: string): Sink {
  const collection = client.db(dbName).collection(collectionName);
  return {
    async insertMany(docs: readonly TickDoc[]): Promise<void> {
      if (docs.length === 0) return;
      await collection.insertMany(docs as TickDoc[], {
        ordered: false,
        writeConcern: { w: 1 },
      });
    },
  };
}
