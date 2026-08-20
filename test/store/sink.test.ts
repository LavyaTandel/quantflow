import { describe, expect, it } from 'vitest';
import { createMongoClient, mongoSink } from '../../src/store/sink.js';

describe('mongo client/sink wiring (hermetic: never connects)', () => {
  it('createMongoClient sets explicit pool backpressure options', () => {
    const client = createMongoClient('mongodb://localhost:27017');
    expect(client.options.waitQueueTimeoutMS).toBe(10_000);
    expect(client.options.maxPoolSize).toBe(100);
  });

  it('caller overrides win over the defaults', () => {
    const client = createMongoClient('mongodb://localhost:27017', {
      waitQueueTimeoutMS: 1_000,
      maxPoolSize: 4,
    });
    expect(client.options.waitQueueTimeoutMS).toBe(1_000);
    expect(client.options.maxPoolSize).toBe(4);
  });

  it('construction does not open sockets and mongoSink short-circuits empty batches', async () => {
    const client = createMongoClient('mongodb://localhost:27017');
    const sink = mongoSink(client, 'quantflow', 'ticks');
    expect(typeof sink.insertMany).toBe('function');
    await expect(sink.insertMany([])).resolves.toBeUndefined();
    expect(client.options.waitQueueTimeoutMS).toBe(10_000);
  });
});
