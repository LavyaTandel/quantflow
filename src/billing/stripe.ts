import { randomBytes } from 'node:crypto';
import Stripe from 'stripe';
import { getTier } from './tiers.js';
import type { Tier } from './tiers.js';

export interface StripeService {
  createCustomer(tenantName: string): Promise<{ customerId: string }>;
  createSubscription(
    customerId: string,
    tier: Tier,
  ): Promise<{ subscriptionId: string; status: 'active' | 'incomplete' }>;
  handleWebhook(event: unknown): Promise<void>;
  getCustomer(customerId: string): Promise<{ customerId: string; name: string } | undefined>;
}

export class FakeStripeService implements StripeService {
  private readonly customers = new Map<string, { customerId: string; name: string }>();
  private readonly subscriptions = new Map<
    string,
    { subscriptionId: string; customerId: string; tier: Tier; status: 'active' | 'incomplete' }
  >();
  private readonly events: unknown[] = [];

  async createCustomer(tenantName: string): Promise<{ customerId: string }> {
    const customerId = `cus_test_${randomBytes(8).toString('hex')}`;
    this.customers.set(customerId, { customerId, name: tenantName });
    return { customerId };
  }

  async createSubscription(
    customerId: string,
    tier: Tier,
  ): Promise<{ subscriptionId: string; status: 'active' | 'incomplete' }> {
    if (!this.customers.has(customerId)) {
      throw new Error(`customer not found: ${customerId}`);
    }
    const def = getTier(tier);
    if (!def) throw new Error(`unknown tier: ${tier}`);
    if (def.stripePriceId === null || def.priceMonthly === null) {
      throw new Error(`enterprise tier requires custom contract - no stripe price`);
    }
    const subscriptionId = `sub_test_${randomBytes(8).toString('hex')}`;
    this.subscriptions.set(subscriptionId, {
      subscriptionId,
      customerId,
      tier,
      status: 'active',
    });
    return { subscriptionId, status: 'active' };
  }

  async handleWebhook(event: unknown): Promise<void> {
    this.events.push(event);
  }

  async getCustomer(customerId: string): Promise<{ customerId: string; name: string } | undefined> {
    const found = this.customers.get(customerId);
    if (!found) return undefined;
    return { ...found };
  }

  getEvents(): unknown[] {
    return [...this.events];
  }
}

export class StripeLiveService implements StripeService {
  private readonly stripe: Stripe;

  constructor(apiKey: string, opts?: Stripe.StripeConfig) {
    if (!apiKey) throw new Error('STRIPE_SECRET_KEY required for live service');
    this.stripe = new Stripe(apiKey, { apiVersion: '2025-02-24.acme' as never, ...opts });
  }

  async createCustomer(tenantName: string): Promise<{ customerId: string }> {
    const customer = await this.stripe.customers.create({ name: tenantName });
    return { customerId: customer.id };
  }

  async createSubscription(
    customerId: string,
    tier: Tier,
  ): Promise<{ subscriptionId: string; status: 'active' | 'incomplete' }> {
    const def = getTier(tier);
    if (!def) throw new Error(`unknown tier: ${tier}`);
    if (!def.stripePriceId)
      throw new Error(`enterprise tier requires custom contract - no stripe price`);
    // Map test Price IDs to live via env; comment documents mapping:
    // price_starter_test -> process.env.STRIPE_PRICE_STARTER_LIVE
    // price_pro_test      -> process.env.STRIPE_PRICE_PRO_LIVE
    const livePrice =
      def.stripePriceId === 'price_starter_test'
        ? (process.env['STRIPE_PRICE_STARTER_LIVE'] ?? def.stripePriceId)
        : def.stripePriceId === 'price_pro_test'
          ? (process.env['STRIPE_PRICE_PRO_LIVE'] ?? def.stripePriceId)
          : def.stripePriceId;
    const sub = await this.stripe.subscriptions.create({
      customer: customerId,
      items: [{ price: livePrice }],
    });
    const status = sub.status === 'active' ? 'active' : 'incomplete';
    return { subscriptionId: sub.id, status };
  }

  async handleWebhook(event: unknown): Promise<void> {
    // Live service would route via verifyStripeWebhook + handleInvoicePaid; keep no-op for compatibility
    void event;
  }

  async getCustomer(customerId: string): Promise<{ customerId: string; name: string } | undefined> {
    try {
      const c = await this.stripe.customers.retrieve(customerId);
      if (c.deleted) return undefined;
      return { customerId: c.id, name: (c as Stripe.Customer).name ?? '' };
    } catch {
      return undefined;
    }
  }

  getStripeClient(): Stripe {
    return this.stripe;
  }
}

export function createTestStripeService(): StripeService {
  return new FakeStripeService();
}

export function createStripeService(): StripeService {
  const key = process.env['STRIPE_SECRET_KEY'];
  if (key) return new StripeLiveService(key);
  return new FakeStripeService();
}
