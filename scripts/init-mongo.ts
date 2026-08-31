// One-time setup for the benchmark suite: recreates the quantflow database
// with a fresh time-series collection so every measured run starts clean.
//
//   npx tsx scripts/init-mongo.ts
import {
  BENCH_DB,
  MONGO_URL,
  TICKS_COLLECTION,
  collectHardwareLine,
  hardwareTable,
} from './bench/lib.js';
import { createMongoClient } from '../src/store/sink.js';

async function main(): Promise<void> {
  const client = createMongoClient(MONGO_URL);
  await client.connect();

  const adminCheck = await client.db('admin').command({ ping: 1 });
  console.log(`[init-mongo] ping ok: ${JSON.stringify(adminCheck)}`);

  await client.db(BENCH_DB).dropDatabase();
  const db = client.db(BENCH_DB);
  await db.createCollection(TICKS_COLLECTION, {
    timeseries: {
      timeField: 'ts',
      metaField: 'symbol',
      granularity: 'seconds',
    },
  });
  const collections = await db.listCollections().toArray();
  console.log(`[init-mongo] created ${collections.map((c) => `${BENCH_DB}.${c.name}`).join(', ')}`);

  const hardware = await collectHardwareLine({ mongo: client });
  console.log('[init-mongo] environment:');
  for (const line of hardwareTable(hardware).split('\n')) console.log(`  ${line}`);

  await client.close();
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
