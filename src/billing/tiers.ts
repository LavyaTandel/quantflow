export type Tier = 'starter' | 'pro' | 'enterprise';

export interface RateLimits {
  api: number;
  wsConnections: number;
  alertsPerSymbolPerSec: number;
}

export interface TierDefinition {
  id: Tier;
  displayName: string;
  priceMonthly: number | null;
  stripePriceId: string | null;
  features: string[];
  rateLimits: RateLimits;
}

/**
 * Seat vs usage metering:
 * - Starter/Pro: usage-based — metered via `rateLimits.api` (60/min, 300/min) + `alertsPerSymbolPerSec`.
 *   Every request/WS/alert is counted; no silent drops — webhook/rate-limit rejections are counted and surface 429.
 * - Enterprise: seat-based — `rateLimits.api = Infinity`; metering is per-seat contract (custom Stripe price = null),
 *   unlimited API/WS within seat count. No usage bucket.
 *
 * Gateway enforces `derivedOnly:true` and counts every rejection.
 */
export const TIERS: Record<Tier, TierDefinition> = {
  starter: {
    id: 'starter',
    displayName: 'Starter',
    priceMonthly: 49,
    stripePriceId: 'price_starter_test',
    features: ['F1'],
    rateLimits: {
      api: 60,
      wsConnections: 5,
      alertsPerSymbolPerSec: 0,
    },
  },
  pro: {
    id: 'pro',
    displayName: 'Pro',
    priceMonthly: 199,
    stripePriceId: 'price_pro_test',
    features: ['F1', 'F2'],
    rateLimits: {
      api: 300,
      wsConnections: 20,
      alertsPerSymbolPerSec: 1,
    },
  },
  enterprise: {
    id: 'enterprise',
    displayName: 'Enterprise',
    priceMonthly: null,
    stripePriceId: null,
    features: ['F1', 'F2', 'F4', 'F3'],
    rateLimits: {
      api: Infinity,
      wsConnections: 100,
      alertsPerSymbolPerSec: 1,
    },
  },
};

export function getTier(id: string): TierDefinition | undefined {
  return TIERS[id as Tier];
}

export function isFeatureAllowed(tier: string | Tier | TierDefinition, feature: string): boolean {
  const def = typeof tier === 'string' ? getTier(tier) : tier;
  if (!def) return false;
  return def.features.includes(feature);
}
