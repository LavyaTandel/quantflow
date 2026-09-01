import type { FeatureVector } from './features.js';

export interface ClassifyResult {
  lean: 'buy' | 'sell' | 'neutral';
  confidence: number;
  score: number;
}

export interface Model {
  classify(features: FeatureVector): Promise<ClassifyResult>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * FakeModel: hermetic, deterministic, configurable latency 5-15 ms.
 * lean = sign(I) if |I|>0.3 else sign(ofi) else neutral
 * confidence = |I| clamped [0,1]
 * score = I (normalized to [-1,1])
 */
export class FakeModel implements Model {
  readonly #latencyMs: number;
  readonly #minMs: number;
  readonly #maxMs: number;
  readonly #jitter: boolean;

  constructor(opts: { latencyMs?: number; minLatencyMs?: number; maxLatencyMs?: number } = {}) {
    if (opts.latencyMs !== undefined) {
      this.#latencyMs = opts.latencyMs;
      this.#minMs = opts.latencyMs;
      this.#maxMs = opts.latencyMs;
      this.#jitter = false;
    } else {
      this.#minMs = opts.minLatencyMs ?? 5;
      this.#maxMs = opts.maxLatencyMs ?? 15;
      this.#latencyMs = Math.floor((this.#minMs + this.#maxMs) / 2);
      this.#jitter = true;
    }
  }

  async classify(features: FeatureVector): Promise<ClassifyResult> {
    const latency = this.#jitter
      ? this.#minMs + Math.random() * (this.#maxMs - this.#minMs)
      : this.#latencyMs;
    if (latency > 0) await sleep(latency);
    const I = features.I;
    const ofi = features.ofi;
    let lean: 'buy' | 'sell' | 'neutral';
    if (Math.abs(I) > 0.3) lean = I > 0 ? 'buy' : 'sell';
    else if (ofi !== 0) lean = ofi > 0 ? 'buy' : 'sell';
    else lean = 'neutral';
    const confidence = Math.min(1, Math.max(0, Math.abs(I)));
    const score = I;
    return { lean, confidence, score };
  }
}

/**
 * LlamaCppModel stub documenting the real hook.
 *
 * Prod wiring (not executed in tests):
 * ```
 * llama-server --model Llama-3.2-1B-Instruct-Q4_K_M.gguf \
 *   --n-gpu-layers 0 --threads 4 --port 8080
 * ```
 * POST http://127.0.0.1:8080/completion
 *   { prompt: "OFI=1240 D=800 I=0.31 DWP=0.22 sweep=0 -> lean=?",
 *     n_predict: 1, temperature: 0, cache_prompt: true }
 * Response: { content: "buy" } -> mapped to {lean, confidence, score}
 *
 * Quant: Q4_K_M ~4.5 bpw, ~60-80 ms E2E (prefill 64 tokens ~8 ms, decode 1 tok ~50 ms)
 * No live model in hermetic tests; FakeModel is the default for tests.
 */
export class LlamaCppModel implements Model {
  readonly endpoint: string;
  constructor(endpoint = 'http://127.0.0.1:8080') {
    this.endpoint = endpoint;
  }

  async classify(_features: FeatureVector): Promise<ClassifyResult> {
    // Intentionally not connected in test/hermetic mode.
    // Real implementation would:
    // 1. format prompt: `OFI=${ofi} I=${I} DWP=${dwp} sweep=${+sweep} -> lean=?`
    // 2. POST to `${endpoint}/completion` with { n_predict: 1, temperature: 0 }
    // 3. parse single token "buy"|"sell"|"neutral"
    throw new Error(
      `LlamaCppModel not connected — start llama-server --n-gpu-layers 0 --model <Q4_K_M.gguf> at ${this.endpoint}; use FakeModel in tests`,
    );
  }
}
