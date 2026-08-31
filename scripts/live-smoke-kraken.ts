const RUN_MS = 60_000;
const PRINT_INTERVAL_MS = 5_000;

function fail(reason: string): never {
  console.error(`live-smoke-kraken: ${reason}`);
  process.exit(1);
}

if (process.env.LIVE_KRAKEN_SMOKE !== '1') {
  fail(
    'refusing to hit live sockets; set LIVE_KRAKEN_SMOKE=1 and run via a TS loader, e.g. `npx tsx scripts/live-smoke-kraken.ts`',
  );
}

const { createKrakenIngestion } = await import('../src/ingest/kraken/index.js');

const ingestion = createKrakenIngestion();
let running = true;

const perKind = { trade: 0, bbo: 0, depth: 0 };
const consumed = (async () => {
  for await (const event of ingestion.pipeline.drain()) {
    perKind[event.kind] += 1;
  }
})();

ingestion.socket.on('open', () => console.log('live-smoke-kraken: connected'));
ingestion.socket.on('reconnectScheduled', ({ attempt, delayMs, reason }) =>
  console.log(`live-smoke-kraken: reconnect #${attempt} in ${delayMs}ms (${reason})`),
);
ingestion.socket.on('error', (err) =>
  console.error('live-smoke-kraken: socket error', err.message),
);

const printer = setInterval(() => {
  const stats = ingestion.stats();
  console.log(
    `live-smoke-kraken: frames=${stats.framesReceived} droppedUnknownFrames=${stats.droppedUnknownFrames} checksumFailures=${stats.checksumFailures} pingsSent=${stats.pingsSent} pipeline=${JSON.stringify(stats.pipeline)}`,
  );
}, PRINT_INTERVAL_MS);

setTimeout(() => {
  running = false;
  clearInterval(printer);
  ingestion.stop();
}, RUN_MS).unref();

process.on('SIGINT', () => {
  running = false;
  clearInterval(printer);
  ingestion.stop();
});

ingestion.start();
while (running) {
  await new Promise((resolve) => setTimeout(resolve, PRINT_INTERVAL_MS));
}
await consumed;
const final = ingestion.stats();
console.log(
  `live-smoke-kraken: done in ${RUN_MS}ms frames=${final.framesReceived} parseErrors=${final.parseErrors} droppedUnknownFrames=${final.droppedUnknownFrames} checksumFailures=${final.checksumFailures} reconnects=${final.reconnects}`,
);
console.log(
  `live-smoke-kraken: per-kind counters trade=${perKind.trade} bbo=${perKind.bbo} depth=${perKind.depth}`,
);
