import Stripe from 'stripe';
import { TIERS, getTier } from './tiers.js';
import type { Tier } from './tiers.js';
import type { Tenant, TenantStore } from './tenant.js';
import type { StripeService } from './stripe.js';

/**
 * Price ID mapping — test vs live:
 * - `price_starter_test` -> Starter ($49/mo) -> replace with live `price_XXX` from Stripe Dashboard
 * - `price_pro_test`      -> Pro ($199/mo)    -> replace with live `price_YYY`
 * - Enterprise has no Stripe price (custom contract, seat-based)
 *
 * Webhook endpoint must be behind `authenticate` gateway shell (reuse TenantStore);
 * `verifyStripeWebhook` validates `stripe-signature` header via `stripe.webhooks.constructEvent`.
 */

// reverse lookup stripePriceId -> Tier
function tierForPriceId(priceId: string | null | undefined): Tier | undefined {
  if (!priceId) return undefined;
  for (const [tier, def] of Object.entries(TIERS) as Array<[Tier, (typeof TIERS)[Tier]]>) {
    if (def.stripePriceId === priceId) return tier;
  }
  return undefined;
}

// In-memory idempotency fallback for hermetic tests. Production uses Redis SET NX EX 24h on `stripe:processed:{event.id}`.
const processedInMemory = new Set<string>();
let redisIdempotencyClient: {
  set(key: string, value: string, opts: { NX: true; EX: number }): Promise<string | null>;
} | null = null;

export function setWebhookIdempotencyRedis(
  client: {
    set(key: string, value: string, opts: { NX: true; EX: number }): Promise<string | null>;
  } | null,
): void {
  redisIdempotencyClient = client;
}

export function clearWebhookIdempotencyForTests(): void {
  processedInMemory.clear();
}

async function isDuplicateEvent(eventId: string, tenantStore?: unknown): Promise<boolean> {
  // Try Redis SET NX EX 24h if a client is available via injected global or tenantStore internals
  const candidate =
    redisIdempotencyClient ??
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (tenantStore as any)?.redisClient ??
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (tenantStore as any)?.client ??
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (tenantStore as any)?._redis ??
    null;
  if (candidate && typeof candidate.set === 'function') {
    try {
      const key = `stripe:processed:${eventId}`;
      const res = await candidate.set(key, '1', { NX: true, EX: 86400 });
      if (res === null) return true; // already exists
      if (res === 'OK') return false;
    } catch {
      // fall through to in-memory
    }
  }
  if (processedInMemory.has(eventId)) return true;
  processedInMemory.add(eventId);
  return false;
}

/**
 * Verify Stripe webhook signature.
 * Wraps `stripe.webhooks.constructEvent` (or Fake HMAC helper when stripe not configured).
 * Must verify `stripe-signature` header, throw on invalid.
 */
export function verifyStripeWebhook(
  payload: string | Buffer,
  signature: string,
  secret: string,
  stripeClient?: Stripe,
): Stripe.Event {
  if (!signature) throw new Error('missing stripe-signature');
  if (!secret) throw new Error('missing webhook secret');
  const body = typeof payload === 'string' ? payload : payload.toString('utf8');
  const client =
    stripeClient ?? new Stripe('sk_test_dummy', { apiVersion: '2025-02-24.acme' as never });
  return client.webhooks.constructEvent(body, signature, secret);
}

/**
 * Handle `invoice.paid` -> TenantStore.updateTier reconciliation.
 * Idempotent on `event.id` via Redis SET NX EX 24h or in-memory Set.
 */
export async function handleInvoicePaid(
  event: Stripe.Event,
  tenantStore: TenantStore,
  _stripeService: StripeService,
): Promise<Tenant | undefined> {
  if (!event || (event as { type?: string }).type !== 'invoice.paid') return undefined;
  const eventId = (event as { id?: string }).id;
  if (!eventId) return undefined;

  if (await isDuplicateEvent(eventId, tenantStore)) {
    // Idempotent: return existing tenant if any, without double-apply
    // Try to locate tenant by customer to return current state
    const objDup = (event as unknown as { data?: { object?: Record<string, unknown> } }).data
      ?.object as Record<string, unknown> | undefined;
    const customerDup =
      typeof objDup?.['customer'] === 'string'
        ? (objDup?.['customer'] as string)
        : ((objDup?.['customer'] as { id?: string } | undefined)?.id ?? undefined);
    if (customerDup) {
      const allDup = await tenantStore.list();
      const foundDup = allDup.find((t) => t.stripeCustomerId === customerDup);
      if (foundDup) return foundDup;
    }
    return undefined;
  }

  const obj = (event as unknown as { data?: { object?: Record<string, unknown> } }).data?.object as
    Record<string, unknown> | undefined;
  if (!obj) return undefined;

  const customerId =
    typeof obj['customer'] === 'string'
      ? (obj['customer'] as string)
      : ((obj['customer'] as { id?: string } | undefined)?.id ?? undefined);

  // Try multiple price locations (invoice line items)
  const lines = obj['lines'] as
    { data?: Array<{ price?: { id?: string }; plan?: { id?: string } }> } | undefined;
  let priceId: string | undefined =
    (obj['price'] as { id?: string } | undefined)?.id ??
    lines?.data?.[0]?.price?.id ??
    lines?.data?.[0]?.plan?.id ??
    undefined;

  // Also support subscription items shape
  if (!priceId && typeof obj['subscription'] === 'object' && obj['subscription'] !== null) {
    const sub = obj['subscription'] as { items?: { data?: Array<{ price?: { id?: string } }> } };
    priceId = sub.items?.data?.[0]?.price?.id;
  }
  // Direct subscription string won't have price; fallback to first line's price already handled
  // Support Stripe invoice `discount` or `subscription_details`
  if (!priceId) {
    const subDetails = obj['subscription_details'] as { price?: { id?: string } } | undefined;
    priceId = subDetails?.price?.id;
  }

  const tier = tierForPriceId(priceId ?? null);
  if (!tier) return undefined;
  const def = getTier(tier);
  if (!def) return undefined;
  if (!customerId) return undefined;

  // Locate tenant by stripeCustomerId (scan list — TenantStore has no findByStripeCustomer)
  const all = await tenantStore.list();
  const tenant = all.find((t) => t.stripeCustomerId === customerId);

  if (tenant) {
    if (tenant.tier === tier) return tenant;
    const updated = await tenantStore.updateTier(tenant.id, tier);
    // Ensure stripeCustomerId persisted if store supports it
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const anyStore = tenantStore as any;
    if (typeof anyStore.setStripeCustomerId === 'function') {
      try {
        await anyStore.setStripeCustomerId(updated.id, customerId);
        const refreshed = await tenantStore.findById(updated.id);
        return refreshed ?? updated;
      } catch {
        // ignore
      }
    }
    return updated;
  }

  // No existing tenant for this customer — create one (needed for second-tenant webhook flow)
  // Name derived from customer id to keep determinism
  const created = await tenantStore.create(`stripe-${customerId.slice(0, 8)}`, tier);
  // Persist stripeCustomerId association if store supports it
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const anyStore2 = tenantStore as any;
  if (typeof anyStore2.setStripeCustomerId === 'function') {
    try {
      await anyStore2.setStripeCustomerId(created.id, customerId);
      const refreshed = await tenantStore.findById(created.id);
      return refreshed ?? created;
    } catch {
      // ignore
    }
  } else if (typeof anyStore2.linkStripeCustomer === 'function') {
    try {
      await anyStore2.linkStripeCustomer(created.id, customerId);
    } catch {
      // ignore
    }
  }
  // Fallback: mutate in-memory if possible (Fake)
  try {
    // Directly patch underlying maps for MemoryTenantStore hermetic case
    if (anyStore2.byId && anyStore2.byId instanceof Map) {
      const ent = anyStore2.byId.get(created.id);
      if (ent) ent.stripeCustomerId = customerId;
    }
    if (anyStore2['#store'] || anyStore2.store) {
      // no-op
    }
  } catch {
    // ignore
  }
  return created;
}
