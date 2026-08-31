import { setTimeout as delay } from 'node:timers/promises';
import { createClient as createRedisClient } from 'redis';
import type { RedisClientType } from 'redis';
import {
  DEFAULT_SYMBOLS,
  createIngestion,
  replayEvents,
  type Ingestion,
  type NormalizedEvent,
  type DepthDiff,
  type EventKind,
} from './ingest/index.js';
import {
  RedisBookStore,
  bufferDiffsWhileBootstrapping,
  fetchRestSnapshot,
  type BookStore,
  type BootstrapEvent,
} from './book/index.js';
import { FlushService, TickBuffer, createMongoClient, mongoSink } from './store/index.js';
import { FanoutHub } from './fanout/index.js';

const KNOWN_STAGES = ['ingest', 'book', 'store', 'fanout'] as const;
type Stage = (typeof KNOWN_STAGES)[number];

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017';
const MONGO_DB = process.env.MONGO_DB ?? 'quantflow';
const MONGO_COLLECTION = process.env.MONGO_COLLECTION ?? 'ticks';
const PORT = Number(process.env.PORT ?? 8080);
const SNAPSHOT_BASE_URL = process.env.SNAPSHOT_BASE_URL;

function log(scope: string, message: string): void {
  console.log(`[${scope}] ${message}`);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function parseStages(raw: string | undefined): Stage[] {
  const items = (raw ?? 'ingest,book,store,fanout')
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  const chosen: Stage[] = [];
  for (const item of items) {
    if (!(KNOWN_STAGES as readonly string[]).includes(item)) {
      log('app', `ignoring unknown stage "${item}"`);
      continue;
    }
    if (!chosen.includes(item as Stage)) chosen.push(item as Stage);
  }
  return chosen;
}

// Per-symbol diff fan-in. One puller per symbol at a time (the runner below);
// bootstrap never abandons an outstanding pull, so waiters resolve exactly once.
type PullResult = { value: DepthDiff; done: false } | { value: undefined; done: true };

class SymbolDiffBus {
  readonly #queues = new Map<string, DepthDiff[]>();
  readonly #waiters = new Map<string, Array<(result: PullResult) => void>>();
  readonly #overflowResync = new Set<string>();
  readonly #capacity: number;
  #closed = false;

  constructor(capacity = 10_000) {
    this.#capacity = capacity;
  }

  isClosed(): boolean {
    return this.#closed;
  }

  push(diff: DepthDiff): void {
    if (this.#closed) return;
    const waiting = this.#waiters.get(diff.symbol);
    const waiter = waiting?.shift();
    if (waiter) {
      waiter({ value: diff, done: false });
      return;
    }
    let queue = this.#queues.get(diff.symbol);
    if (!queue) {
      queue = [];
      this.#queues.set(diff.symbol, queue);
    }
    if (queue.length >= this.#capacity) {
      queue.length = 0;
      this.#overflowResync.add(diff.symbol);
    }
    queue.push(diff);
  }

  next(symbol: string): Promise<PullResult> {
    const queue = this.#queues.get(symbol);
    if (queue && queue.length > 0) {
      return Promise.resolve({ value: queue.shift() as DepthDiff, done: false });
    }
    if (this.#closed) return Promise.resolve({ value: undefined, done: true });
    return new Promise((resolve) => {
      const list = this.#waiters.get(symbol) ?? [];
      list.push(resolve);
      this.#waiters.set(symbol, list);
    });
  }

  consumeOverflowResync(symbol: string): boolean {
    return this.#overflowResync.delete(symbol);
  }

  iterable(symbol: string): AsyncIterable<DepthDiff> {
    const next = (): Promise<PullResult> => this.next(symbol);
    return {
      [Symbol.asyncIterator]() {
        return {
          next: async (): Promise<IteratorResult<DepthDiff>> => await next(),
        };
      },
    };
  }

  close(): void {
    this.#closed = true;
    for (const [symbol, list] of this.#waiters) {
      for (const waiter of list.splice(0)) waiter({ value: undefined, done: true });
      this.#waiters.delete(symbol);
    }
  }
}

interface RuntimeCounters {
  eventsReceived: number;
  byKind: Record<EventKind, number>;
  bookApplied: number;
  bookStale: number;
  bookGaps: number;
  bookResyncs: number;
}

interface BookRunnerDeps {
  bus: SymbolDiffBus;
  store: BookStore;
  snapshotBaseUrl?: string;
  counters: RuntimeCounters;
}

async function runSymbolBook(symbol: string, deps: BookRunnerDeps): Promise<void> {
  const { bus, store, counters } = deps;
  const onBootstrapEvent = (event: BootstrapEvent): void => {
    if (event.kind === 'continuity-gap') {
      log(
        'book',
        `${symbol} gap during bootstrap expected=${event.expectedFrom} got=${event.gotFirst}-${event.gotFinal}`,
      );
    }
  };
  const bootstrap = () =>
    bufferDiffsWhileBootstrapping({
      symbol,
      diffs: bus.iterable(symbol),
      store,
      fetchSnapshot: () => fetchRestSnapshot(symbol, { baseUrl: SNAPSHOT_BASE_URL }),
      onEvent: onBootstrapEvent,
    });

  while (!bus.isClosed()) {
    const result = await bootstrap();
    if (result.status !== 'synced') {
      log('book', `${symbol} bootstrap unsynced (${result.reason}); retrying in 15s`);
      await delay(15_000);
      continue;
    }
    log(
      'book',
      `${symbol} synced lastUpdateId=${result.lastUpdateId} applied=${result.applied} buffered=${result.bufferedWhileFetching} elapsed=${result.elapsedMs}ms`,
    );

    for (;;) {
      if (bus.consumeOverflowResync(symbol)) break;
      const pull = await bus.next(symbol);
      if (pull.done) return;
      try {
        const outcome = await store.applyDepthDiff(pull.value);
        if (outcome === 'applied') {
          counters.bookApplied += 1;
          continue;
        }
        if (outcome === 'stale') {
          counters.bookStale += 1;
          continue;
        }
        break;
      } catch (err) {
        log('book', `${symbol} apply failed (${messageOf(err)}); retrying in 1s`);
        await delay(1_000);
      }
    }
    if (bus.isClosed()) return;
    counters.bookGaps += 1;
    counters.bookResyncs += 1;
    log('book', `${symbol} continuity gap; resynchronizing`);
  }
}

async function* noEvents(): AsyncGenerator<NormalizedEvent> {}

async function main(): Promise<void> {
  const stages = parseStages(process.env.STAGES);
  // Offline demo mode: a replay file is an ingestion source in its own right,
  // so it implies the ingest stage even when STAGES omits it.
  const replayMode = Boolean(process.env.REPLAY_FILE);
  if (replayMode && !stages.includes('ingest')) {
    stages.push('ingest');
    log('app', `REPLAY_FILE implies ingest stage; effective=${stages.join(',')}`);
  }
  const has = (stage: Stage): boolean => stages.includes(stage);
  const counters: RuntimeCounters = {
    eventsReceived: 0,
    byKind: { trade: 0, bbo: 0, depth: 0 },
    bookApplied: 0,
    bookStale: 0,
    bookGaps: 0,
    bookResyncs: 0,
  };

  log('app', `stages=${stages.join(',')}`);

  let hub: FanoutHub | undefined;
  if (has('fanout')) {
    hub = new FanoutHub({ stages, publicDir: 'public' });
    await hub.listen(PORT);
    log(
      'app',
      `http on http://localhost:${PORT} (ws at /ws, GET / serves public/index.html when present)`,
    );
  }

  let mongo: ReturnType<typeof createMongoClient> | undefined;
  let flusher: FlushService | undefined;
  let buffer: TickBuffer<NormalizedEvent> | undefined;
  if (has('store')) {
    mongo = createMongoClient(MONGO_URL);
    await mongo.connect();
    buffer = new TickBuffer<NormalizedEvent>();
    flusher = new FlushService({ sink: mongoSink(mongo, MONGO_DB, MONGO_COLLECTION), buffer });
    flusher.start();
    log('app', `mongo ${MONGO_URL} db=${MONGO_DB} collection=${MONGO_COLLECTION}`);
  }

  let redis: RedisClientType | undefined;
  let bus: SymbolDiffBus | undefined;
  const runners: Array<Promise<void>> = [];
  if (has('book')) {
    redis = createRedisClient({ url: REDIS_URL });
    await redis.connect();
    bus = new SymbolDiffBus();
    const store = new RedisBookStore(redis);
    for (const symbol of DEFAULT_SYMBOLS) {
      runners.push(
        runSymbolBook(symbol, { bus, store, snapshotBaseUrl: SNAPSHOT_BASE_URL, counters }),
      );
    }
    log('app', `redis ${REDIS_URL}; bootstrapping ${DEFAULT_SYMBOLS.length} books`);
  }

  let ingestion: Ingestion | undefined;
  let source: AsyncGenerator<NormalizedEvent>;
  if (replayMode) {
    // Paced so downstream stages (and any connected viewer) observe a stream
    // rather than an instant dump; REPLAY_DELAY_MS=0 restores full speed.
    const replayDelayMs = Math.max(0, Number(process.env.REPLAY_DELAY_MS ?? 600));
    log('app', `replay mode: ${process.env.REPLAY_FILE} (${replayDelayMs}ms between events)`);
    async function* pacedReplay(): AsyncGenerator<NormalizedEvent> {
      for await (const event of replayEvents(process.env.REPLAY_FILE as string)) {
        if (replayDelayMs > 0) await delay(replayDelayMs);
        yield event;
      }
    }
    source = pacedReplay();
  } else if (has('ingest')) {
    ingestion = createIngestion();
    ingestion.socket.on('open', () => log('ingest', 'connected'));
    ingestion.socket.on('reconnectScheduled', ({ attempt, delayMs, reason }) =>
      log('ingest', `reconnect #${attempt} in ${delayMs}ms (${reason})`),
    );
    ingestion.socket.on('error', (err) => log('ingest', `socket error: ${err.message}`));
    source = ingestion.pipeline.drain();
  } else {
    source = noEvents();
  }

  let requestStop: (() => void) | undefined;
  const stopped = new Promise<'stop'>((resolve) => {
    requestStop = () => resolve('stop');
  });

  const statsTimer = setInterval(() => {
    if (flusher) {
      const s = flusher.stats();
      log(
        'store',
        `flushes=${s.flushesSucceeded}/${s.flushesStarted} docs=${s.docsWritten} buffer=${s.bufferDepth} droppedOldest=${s.droppedOldest} refusedDepth=${s.refusedDepth} p50=${s.latencyMs.p50}ms p99=${s.latencyMs.p99}ms`,
      );
    }
    if (hub) {
      const s = hub.stats();
      log(
        'fanout',
        `clients=${s.clientsConnected} frames=${s.framesBroadcast} sendErrors=${s.sendErrors} slowDrops=${s.slowClientsDropped}`,
      );
    }
  }, 30_000);
  statsTimer.unref();

  let shutdownStarted = false;
  let shutdownDone: Promise<void> = Promise.resolve();
  const shutdown = async (reason: string): Promise<void> => {
    if (shutdownStarted) return;
    shutdownStarted = true;
    log('app', `shutting down (${reason})`);
    const force = setTimeout(() => {
      console.error('[app] shutdown timed out; forcing exit');
      process.exit(1);
    }, 9000);
    force.unref();
    requestStop?.();
    ingestion?.stop();
    bus?.close();
    await Promise.race([Promise.allSettled(runners), delay(3_000)]);
    if (flusher) {
      try {
        await flusher.close();
      } catch {
        // unwritten docs already logged by the flusher
      }
    }
    await hub?.close();
    if (redis) {
      try {
        await redis.quit();
      } catch {
        redis.disconnect();
      }
    }
    await mongo?.close();
    clearInterval(statsTimer);

    const hubStats = hub?.stats();
    const flushStats = flusher?.stats();
    log(
      'app',
      `summary events=${counters.eventsReceived} trade=${counters.byKind.trade} bbo=${counters.byKind.bbo} depth=${counters.byKind.depth} | book applied=${counters.bookApplied} stale=${counters.bookStale} gaps=${counters.bookGaps} resyncs=${counters.bookResyncs} | store docsWritten=${flushStats?.docsWritten ?? 0} droppedOldest=${flushStats?.droppedOldest ?? 0} | fanout clients=${hubStats?.connectionsAccepted ?? 0} frames=${hubStats?.framesBroadcast ?? 0} slowDrops=${hubStats?.slowClientsDropped ?? 0}`,
    );
    clearTimeout(force);
    process.exit(0);
  };

  process.once('SIGINT', () => {
    shutdownDone = shutdown('SIGINT');
  });
  process.once('SIGTERM', () => {
    shutdownDone = shutdown('SIGTERM');
  });
  process.stdin.resume();
  process.stdin.once('end', () => {
    shutdownDone = shutdown('stdin-end');
  });

  if (ingestion) ingestion.start();

  const iterator = source[Symbol.asyncIterator]();
  let stoppedBySignal = false;
  pump: for (;;) {
    const pending = iterator.next();
    const outcome = await Promise.race([
      pending.then(
        (r) => ({ kind: 'next' as const, r }),
        () => ({ kind: 'failed' as const }),
      ),
      stopped.then(() => ({ kind: 'stop' as const })),
    ]);
    if (outcome.kind === 'stop') {
      stoppedBySignal = true;
      pending
        .catch(() => {})
        .finally(() => {
          void iterator.return(undefined).catch(() => {});
        });
      break pump;
    }
    if (outcome.kind === 'failed') {
      log('app', 'event source failed; stopping pump');
      break pump;
    }
    if (outcome.r.done) break pump;
    const event = outcome.r.value;
    counters.eventsReceived += 1;
    counters.byKind[event.kind] += 1;
    hub?.broadcast(event);
    buffer?.push(event);
    if (bus && event.kind === 'depth') bus.push(event);
  }

  if (!stoppedBySignal) {
    log('app', 'event source ended; idling (Ctrl-C or stdin EOF to stop)');
  } else {
    await shutdownDone;
  }
}

main().catch((err: unknown) => {
  console.error(`[app] fatal: ${messageOf(err)}`);
  process.exitCode = 1;
});
