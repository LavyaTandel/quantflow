import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import os from 'node:os';
import { format as prettierFormat } from 'prettier';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
export const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017';
export const BENCH_DB = 'quantflow';
export const TICKS_COLLECTION = 'ticks';

const DOCS_DIR = join(import.meta.dirname, '..', '..', 'docs', 'benchmarks');
export const RUN_DATE = '2026-08-26';

// Nearest-rank percentile on a pre-sorted array (ascending).
// idx = ceil(p/100 * n) - 1, clamped to [0, n-1]. Same definition as
// src/store/flusher.ts so numbers are comparable across the repo.
export function percentile(sortedAsc: readonly number[], p: number): number {
  const n = sortedAsc.length;
  if (n === 0) return 0;
  const idx = Math.min(n - 1, Math.max(0, Math.ceil((p / 100) * n) - 1));
  return sortedAsc[idx] as number;
}

export interface Percentiles {
  samples: number;
  p50: number;
  p99: number;
  max: number;
}

export class LatencyRecorder {
  private readonly samples: number[] = [];

  record(ms: number): void {
    this.samples.push(ms);
  }

  summarize(): Percentiles {
    const sorted = [...this.samples].sort((a, b) => a - b);
    return {
      samples: sorted.length,
      p50: percentile(sorted, 50),
      p99: percentile(sorted, 99),
      max: sorted.length > 0 ? (sorted[sorted.length - 1] as number) : 0,
    };
  }

  exportSamples(): readonly number[] {
    return this.samples;
  }

  get count(): number {
    return this.samples.length;
  }
}

// Deterministic PRNG (mulberry32) so runs are reproducible from a seed.
export function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a += 0x6d2b79f5;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function dockerVersion(): string | undefined {
  try {
    return execFileSync('docker', ['version', '--format', '{{.Server.Version}}'], {
      encoding: 'utf8',
      timeout: 5000,
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    return undefined;
  }
}

export async function redisVersion(
  client: import('redis').RedisClientType,
): Promise<string | undefined> {
  try {
    const info = await client.info('server');
    const match = /redis_version:([^\r\n]+)/.exec(info);
    return match?.[1]?.trim();
  } catch {
    return undefined;
  }
}

export async function mongoVersion(
  client: import('mongodb').MongoClient,
): Promise<string | undefined> {
  try {
    const buildInfo = await client.db(BENCH_DB).command({ buildInfo: 1 });
    return typeof buildInfo.version === 'string' ? buildInfo.version : undefined;
  } catch {
    return undefined;
  }
}

export interface HardwareLine {
  cpuModel: string;
  logicalCores: number;
  ramGb: string;
  os: string;
  nodeVersion: string;
  redisVersion?: string;
  mongoVersion?: string;
  dockerEngineVersion?: string;
}

export async function collectHardwareLine(
  extra: { redis?: import('redis').RedisClientType; mongo?: import('mongodb').MongoClient } = {},
): Promise<HardwareLine> {
  return {
    cpuModel: os.cpus()[0]?.model ?? 'unknown',
    logicalCores: os.cpus().length,
    ramGb: (os.totalmem() / 1024 ** 3).toFixed(1),
    os: `${os.type()} ${os.release()} (${os.platform()})`,
    nodeVersion: process.version,
    redisVersion: extra.redis ? await redisVersion(extra.redis) : undefined,
    mongoVersion: extra.mongo ? await mongoVersion(extra.mongo) : undefined,
    dockerEngineVersion: dockerVersion(),
  };
}

export function hardwareTable(h: HardwareLine): string {
  const rows: Array<[string, string]> = [
    ['CPU', `${h.cpuModel} x${h.logicalCores} logical cores`],
    ['RAM', `${h.ramGb} GB total`],
    ['OS', h.os],
    ['Node.js', h.nodeVersion],
  ];
  if (h.dockerEngineVersion) rows.push(['Docker engine', h.dockerEngineVersion]);
  if (h.redisVersion) rows.push(['Redis server', h.redisVersion]);
  if (h.mongoVersion) rows.push(['MongoDB server', h.mongoVersion]);
  const body = rows.map(([k, v]) => `| ${k} | ${v} |`).join('\n');
  return `| Component | Value |\n| --- | --- |\n${body}`;
}

export const PERCENTILE_DEFINITION =
  'Percentile definition: nearest-rank on the ascending-sorted sample — index = ceil(p/100 × n) − 1, ' +
  'clamped to [0, n−1]; p50/p99/max computed over all recorded samples of the phase unless stated otherwise.';

// Results files live under prettier --check's scope, so format before writing
// to keep reruns gate-green without a separate formatting pass.
export async function writeResultsFile(row: string, markdown: string): Promise<string> {
  const formatted = await prettierFormat(markdown, {
    parser: 'markdown',
    printWidth: 100,
    semi: true,
    singleQuote: true,
    trailingComma: 'all',
  });
  mkdirSync(DOCS_DIR, { recursive: true });
  const path = join(DOCS_DIR, `${RUN_DATE}-${row}.md`);
  writeFileSync(path, formatted, 'utf8');
  return path;
}

export function fmtMs(ms: number): string {
  return ms >= 100
    ? `${ms.toFixed(1)} ms`
    : ms >= 10
      ? `${ms.toFixed(2)} ms`
      : `${ms.toFixed(3)} ms`;
}

export function fmtInt(n: number): string {
  return n.toLocaleString('en-US');
}

export function verdict(passed: boolean): 'PASS' | 'MISS' {
  return passed ? 'PASS' : 'MISS';
}

// Timer-paced emission with drift correction: on every tick it emits however
// many events should have been produced by now (rate × elapsed) minus what has
// already been emitted, so jitter in earlier ticks is absorbed, not accumulated.
export class PacedEmitter {
  readonly ratePerSec: number;
  #timer: NodeJS.Timeout | null = null;
  #startedAt = 0;
  #emitted = 0;

  constructor(
    ratePerSec: number,
    private readonly emitBatch: (count: number) => void,
    private readonly tickMs = 5,
  ) {
    if (!Number.isFinite(ratePerSec) || ratePerSec <= 0) {
      throw new RangeError(`ratePerSec must be positive, got ${ratePerSec}`);
    }
    this.ratePerSec = ratePerSec;
  }

  start(): void {
    if (this.#timer !== null) return;
    this.#startedAt = performance.now();
    this.#emitted = 0;
    this.#timer = setInterval(() => {
      const due = Math.floor(((performance.now() - this.#startedAt) / 1000) * this.ratePerSec);
      const batch = due - this.#emitted;
      if (batch > 0) {
        this.emitBatch(batch);
        this.#emitted += batch;
      }
    }, this.tickMs);
  }

  stop(): void {
    if (this.#timer === null) return;
    clearInterval(this.#timer);
    this.#timer = null;
  }

  get count(): number {
    return this.#emitted;
  }

  get elapsedSec(): number {
    return this.#startedAt === 0 ? 0 : (performance.now() - this.#startedAt) / 1000;
  }

  get running(): boolean {
    return this.#timer !== null;
  }
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export const yieldToLoop = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
