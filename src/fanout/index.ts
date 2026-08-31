import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { WebSocketServer, type WebSocket } from 'ws';
import type { NormalizedEvent } from '../ingest/types.js';

export interface FanoutHubOptions {
  stages: readonly string[];
  publicDir?: string;
  maxSendErrorsPerClient?: number;
}

export interface FanoutStats {
  clientsConnected: number;
  connectionsAccepted: number;
  framesBroadcast: number;
  sendErrors: number;
  slowClientsDropped: number;
}

const DEFAULT_MAX_SEND_ERRORS = 100;

interface ClientState {
  ws: WebSocket;
  sendErrors: number;
}

export function encodeEvent(event: NormalizedEvent): string {
  switch (event.kind) {
    case 'trade':
      return JSON.stringify({
        t: 'trade',
        s: event.symbol,
        p: event.price,
        q: event.qty,
        ts: event.ts,
      });
    case 'bbo':
      return JSON.stringify({
        t: 'bbo',
        s: event.symbol,
        bp: event.bidPrice,
        bq: event.bidQty,
        ap: event.askPrice,
        aq: event.askQty,
        ts: event.ts,
      });
    case 'depth':
      return JSON.stringify({ t: 'depth', s: event.symbol, n: event.levels.length, ts: event.ts });
  }
}

export class FanoutHub {
  readonly #stages: readonly string[];
  readonly #publicDir: string;
  readonly #maxSendErrors: number;
  readonly #server: Server;
  readonly #wss: WebSocketServer;
  readonly #clients = new Set<ClientState>();

  #connectionsAccepted = 0;
  #framesBroadcast = 0;
  #sendErrors = 0;
  #slowClientsDropped = 0;

  constructor(options: FanoutHubOptions) {
    this.#stages = [...options.stages];
    this.#publicDir = options.publicDir ?? 'public';
    this.#maxSendErrors = options.maxSendErrorsPerClient ?? DEFAULT_MAX_SEND_ERRORS;
    this.#server = createServer((req, res) => {
      void this.#handleHttp(req, res);
    });
    this.#wss = new WebSocketServer({ noServer: true });
    this.#server.on('upgrade', (req, socket, head) => {
      const pathname = new URL(req.url ?? '/', 'http://localhost').pathname;
      if (pathname !== '/ws') {
        socket.destroy();
        return;
      }
      this.#wss.handleUpgrade(req, socket, head, (ws) => this.#onConnection(ws));
    });
  }

  listen(port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const onError = (err: Error): void => reject(err);
      this.#server.once('error', onError);
      this.#server.listen(port, () => {
        this.#server.removeListener('error', onError);
        resolve();
      });
    });
  }

  broadcast(event: NormalizedEvent): void {
    if (this.#clients.size === 0) return;
    const frame = encodeEvent(event);
    this.#framesBroadcast += 1;
    for (const state of [...this.#clients]) this.#send(state, frame);
  }

  stats(): FanoutStats {
    return {
      clientsConnected: this.#clients.size,
      connectionsAccepted: this.#connectionsAccepted,
      framesBroadcast: this.#framesBroadcast,
      sendErrors: this.#sendErrors,
      slowClientsDropped: this.#slowClientsDropped,
    };
  }

  async close(): Promise<void> {
    for (const state of [...this.#clients]) {
      try {
        state.ws.close(1001, 'server-shutdown');
      } catch {
        // already closing
      }
    }
    const deadline = Date.now() + 1000;
    while (this.#clients.size > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    for (const state of [...this.#clients]) {
      try {
        state.ws.terminate();
      } catch {
        // already closed
      }
    }
    await new Promise<void>((resolve) => this.#wss.close(() => resolve()));
    await new Promise<void>((resolve) => this.#server.close(() => resolve()));
  }

  #onConnection(ws: WebSocket): void {
    const state: ClientState = { ws, sendErrors: 0 };
    this.#clients.add(state);
    this.#connectionsAccepted += 1;
    ws.on('error', () => {
      // counted via send callbacks / close
    });
    ws.on('close', () => this.#clients.delete(state));
    this.#send(state, JSON.stringify({ t: 'hello', stages: this.#stages }));
  }

  #send(state: ClientState, frame: string): void {
    try {
      state.ws.send(frame, (err) => {
        if (err) this.#recordSendError(state);
      });
    } catch {
      this.#recordSendError(state);
    }
  }

  #recordSendError(state: ClientState): void {
    this.#sendErrors += 1;
    state.sendErrors += 1;
    if (state.sendErrors >= this.#maxSendErrors) {
      this.#clients.delete(state);
      this.#slowClientsDropped += 1;
      try {
        state.ws.terminate();
      } catch {
        // already closed
      }
    }
  }

  async #handleHttp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const pathname = new URL(req.url ?? '/', 'http://localhost').pathname;
    if (req.method === 'GET' && pathname === '/') {
      try {
        const html = await readFile(join(this.#publicDir, 'index.html'), 'utf8');
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(html);
        return;
      } catch {
        res.writeHead(404, { 'content-type': 'text/plain' });
        res.end('not found');
        return;
      }
    }
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  }
}
