import type { Scenario } from './types.js';

/**
 * MacroShockEmbedder — maps natural-language shock descriptions to nearest Scenario
 * via cosine similarity over dense vectors.
 *
 * Production: `AllMiniLML6V2Embedder` wraps local `all-MiniLM-L6-v2` (80 MB) via
 * `transformers.js` or `onnxruntime-node`. Not instantiated in hermetic tests.
 * Stripe entitles shock count per month — enforced in billing layer, documented here
 * but not enforced in core (core is hermetic).
 */

export interface MacroShockEmbedder {
  embed(text: string): Promise<number[]>;
  nearestScenario(shockText: string, scenarios: Scenario[]): Promise<Scenario | undefined>;
}

function cosine(a: number[], b: number[]): number {
  let dot = 0;
  let na = 0;
  let nb = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    dot += a[i]! * b[i]!;
    na += a[i]! * a[i]!;
    nb += b[i]! * b[i]!;
  }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

/**
 * Deterministic hash: string -> fixed-dim vector (384 dims to match all-MiniLM-L6-v2).
 * Not semantic, but deterministic for hermetic tests. Also captures word-overlap signal
 * to make nearestScenario meaningful: vector incorporates bag-of-words hash.
 */
function hashToVector(text: string, dim = 384): number[] {
  const vec = new Array<number>(dim).fill(0);
  const words = text.toLowerCase().split(/\W+/).filter(Boolean);
  // Word-level signal: each word hashes to a few positions
  for (const w of words) {
    let h = 0;
    for (let i = 0; i < w.length; i++) h = (h * 31 + w.charCodeAt(i)) >>> 0;
    const idx = h % dim;
    const idx2 = (h >>> 7) % dim;
    vec[idx]! += 1;
    vec[idx2]! += 0.5;
  }
  // Char-level spread for overall determinism
  let h2 = 5381;
  for (let i = 0; i < text.length; i++) h2 = ((h2 << 5) + h2 + text.charCodeAt(i)) >>> 0;
  for (let i = 0; i < dim; i++) {
    // Mix in small deterministic noise per position
    const jitter = (h2 >> (i % 16)) & 1 ? 0.01 : -0.01;
    vec[i]! += jitter;
  }
  // L2 normalize
  const norm = Math.sqrt(vec.reduce((s, v) => s + v * v, 0)) || 1;
  return vec.map((v) => v / norm);
}

/**
 * FakeEmbedder — hermetic, deterministic, 5-10 ms delay, cosine lookup.
 * `nearestScenario` scores scenarios by max cosine between shockText embedding
 * and any of the scenario's MacroShock embeddings (or scenario name/shock descriptions
 * hashed on the fly). Returns nearest by shared word overlap.
 */
export class FakeEmbedder implements MacroShockEmbedder {
  private readonly dim: number;
  private readonly delayMs: number;

  constructor(opts: { dim?: number; delayMs?: number } = {}) {
    this.dim = opts.dim ?? 384;
    this.delayMs = opts.delayMs ?? 7;
  }

  async embed(text: string): Promise<number[]> {
    // Simulate model latency 5-10 ms
    const jitter = Math.floor(Math.random() * 5); // 0-4
    const delay = Math.max(5, Math.min(10, this.delayMs + (jitter % 3)));
    await new Promise((r) => setTimeout(r, delay));
    return hashToVector(text, this.dim);
  }

  async nearestScenario(shockText: string, scenarios: Scenario[]): Promise<Scenario | undefined> {
    if (scenarios.length === 0) return undefined;
    const q = await this.embed(shockText);
    let best: Scenario | undefined;
    let bestScore = -Infinity;
    for (const sc of scenarios) {
      // Score scenario by best cosine among its shocks (+ scenario name fallback)
      let score = -Infinity;
      const candidates: string[] = [sc.name, ...sc.shocks.map((s) => s.description)];
      if (candidates.length === 0) candidates.push(sc.name);
      for (const c of candidates) {
        // Prefer embedding if present, else hash
        let vec: number[];
        const shockWithEmbedding = sc.shocks.find((s) => s.description === c && s.embedding);
        if (shockWithEmbedding?.embedding) vec = shockWithEmbedding.embedding;
        else vec = hashToVector(c, this.dim);
        const s = cosine(q, vec);
        if (s > score) score = s;
      }
      // Word-overlap boost to make tests deterministic and intuitive:
      // if shockText shares words with scenario, boost slightly (already via vector but amplify)
      const qWords = new Set(shockText.toLowerCase().split(/\W+/).filter(Boolean));
      const scWords = new Set(candidates.join(' ').toLowerCase().split(/\W+/).filter(Boolean));
      let overlap = 0;
      for (const w of qWords) if (scWords.has(w)) overlap++;
      const boosted = score + overlap * 0.001;
      if (boosted > bestScore) {
        bestScore = boosted;
        best = sc;
      }
    }
    return best;
  }
}

/**
 * AllMiniLML6V2Embedder — stub documenting local model hook.
 *
 * Production wiring:
 * ```
 * import { pipeline } from '@xenova/transformers'; // or transformers.js
 * // or import * as ort from 'onnxruntime-node'
 * const extractor = await pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
 * const output = await extractor(text, { pooling: 'mean', normalize: true });
 * ```
 * Model size ~80 MB, runs locally, no network. Latency budget 200 ms P99 for single lookup.
 * Stripe entitles shock count per month (e.g., tiers: starter 100, pro 1000, enterprise unlimited)
 * — check `billing/tiers.ts` and enforce at API gateway, not here.
 *
 * This stub keeps hermetic tests free of native deps; `FakeEmbedder` is used in tests.
 */
export class AllMiniLML6V2Embedder implements MacroShockEmbedder {
  async embed(_text: string): Promise<number[]> {
    throw new Error(
      'AllMiniLML6V2Embedder requires local model (all-MiniLM-L6-v2, 80 MB) via transformers.js or onnxruntime-node — not available in hermetic tests. Use FakeEmbedder.',
    );
  }

  async nearestScenario(_shockText: string, _scenarios: Scenario[]): Promise<Scenario | undefined> {
    throw new Error(
      'AllMiniLML6V2Embedder requires local model — not available in hermetic tests. Use FakeEmbedder.',
    );
  }
}
