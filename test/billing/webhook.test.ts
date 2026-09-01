import { describe, it, expect, beforeEach } from 'vitest';
import Stripe from 'stripe';
import { MemoryTenantStore } from '../../src/billing/tenant.js';
import { FakeStripeService } from '../../src/billing/stripe.js';
import {
  verifyStripeWebhook,
  handleInvoicePaid,
  clearWebhookIdempotencyForTests,
} from '../../src/billing/webhook.js';

const WEBHOOK_SECRET = 'whsec_test_1234567890abcdef1234567890abcdef';

function createStripe(): Stripe {
  return new Stripe('sk_test_dummy', { apiVersion: '2025-02-24.acme' as never });
}

function generateSignature(payload: string, secret: string, stripe: Stripe): string {
  // Use stripe's helper to generate a valid test header
  return stripe.webhooks.generateTestHeaderString({ payload, secret });
}

function makeInvoicePaidEvent(opts: {
  eventId: string;
  customerId: string;
  priceId: string;
}): Stripe.Event {
  return {
    id: opts.eventId,
    type: 'invoice.paid',
    data: {
      object: {
        customer: opts.customerId,
        lines: { data: [{ price: { id: opts.priceId } }] },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
      } as any,
    },
    // minimal required fields for Stripe.Event
    api_version: '2025-02-24.acme',
    created: Math.floor(Date.now() / 1000),
    livemode: false,
    pending_webhooks: 1,
    request: { id: 'req_test', idempotency_key: null },
    object: 'event',
  } as unknown as Stripe.Event;
}

describe('verifyStripeWebhook', () => {
  it('valid signature passes', () => {
    const stripe = createStripe();
    const payload = JSON.stringify({ type: 'invoice.paid', id: 'evt_1' });
    const sig = generateSignature(payload, WEBHOOK_SECRET, stripe);
    const event = verifyStripeWebhook(payload, sig, WEBHOOK_SECRET, stripe);
    expect(event.type).toBe('invoice.paid');
  });

  it('invalid signature throws', () => {
    const stripe = createStripe();
    const payload = JSON.stringify({ type: 'invoice.paid', id: 'evt_1' });
    const sig = generateSignature(payload, WEBHOOK_SECRET, stripe);
    const badSig = sig.replace(
      /v1=[^,]+/,
      'v1=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef',
    );
    expect(() => verifyStripeWebhook(payload, badSig, WEBHOOK_SECRET, stripe)).toThrow();
  });

  it('missing signature throws', () => {
    const stripe = createStripe();
    const payload = JSON.stringify({ type: 'invoice.paid' });
    expect(() => verifyStripeWebhook(payload, '', WEBHOOK_SECRET, stripe)).toThrow(
      /missing stripe-signature/,
    );
  });
});

describe('handleInvoicePaid', () => {
  beforeEach(() => {
    clearWebhookIdempotencyForTests();
  });

  it('upgrades tier via MemoryTenantStore on valid invoice.paid', async () => {
    const store = new MemoryTenantStore();
    const tenant = await store.create('Acme', 'starter');
    await store.setStripeCustomerId(tenant.id, 'cus_acme_123');
    const event = makeInvoicePaidEvent({
      eventId: 'evt_upgrade_1',
      customerId: 'cus_acme_123',
      priceId: 'price_pro_test',
    });
    // also need to simulate verified event via webhook helper
    const stripe = createStripe();
    const payload = JSON.stringify(event);
    const sig = generateSignature(payload, WEBHOOK_SECRET, stripe);
    const verified = verifyStripeWebhook(payload, sig, WEBHOOK_SECRET, stripe);
    // verified event should still have our customer/price mapping
    // overwrite verified data with our test data (constructEvent preserves payload structure)
    // For hermetic, directly use event with correct shape
    const fakeService = new FakeStripeService();
    // Use original event (with typed fields) for reconciliation
    const result = await handleInvoicePaid(event, store, fakeService);
    expect(result).toBeDefined();
    expect(result!.tier).toBe('pro');
    expect(result!.entitlements.features).toEqual(['F1', 'F2']);
    const refetched = await store.findById(tenant.id);
    expect(refetched!.tier).toBe('pro');

    // verify via verifyStripeWebhook path also works
    expect(verified.type).toBe('invoice.paid');
    void verified;
  });

  it('price mapping starter vs pro', async () => {
    const store = new MemoryTenantStore();
    const tStarter = await store.create('StarterCo', 'starter');
    await store.setStripeCustomerId(tStarter.id, 'cus_starter_1');
    const tPro = await store.create('ProCo', 'starter');
    await store.setStripeCustomerId(tPro.id, 'cus_pro_1');

    const svc = new FakeStripeService();

    const evtStarter = makeInvoicePaidEvent({
      eventId: 'evt_price_starter',
      customerId: 'cus_starter_1',
      priceId: 'price_starter_test',
    });
    const r1 = await handleInvoicePaid(evtStarter, store, svc);
    expect(r1!.tier).toBe('starter');

    const evtPro = makeInvoicePaidEvent({
      eventId: 'evt_price_pro',
      customerId: 'cus_pro_1',
      priceId: 'price_pro_test',
    });
    const r2 = await handleInvoicePaid(evtPro, store, svc);
    expect(r2!.tier).toBe('pro');
  });

  it('duplicate event.id second call is idempotent (no second update)', async () => {
    const store = new MemoryTenantStore();
    const tenant = await store.create('Acme', 'starter');
    await store.setStripeCustomerId(tenant.id, 'cus_dup_123');
    const svc = new FakeStripeService();
    const event = makeInvoicePaidEvent({
      eventId: 'evt_dup_123',
      customerId: 'cus_dup_123',
      priceId: 'price_pro_test',
    });

    const first = await handleInvoicePaid(event, store, svc);
    expect(first!.tier).toBe('pro');

    // downgrade manually to detect if second call reapplies
    await store.updateTier(tenant.id, 'starter');
    const afterDowngrade = await store.findById(tenant.id);
    expect(afterDowngrade!.tier).toBe('starter');

    const second = await handleInvoicePaid(event, store, svc);
    // second should be idempotent — not re-upgrade
    const final = await store.findById(tenant.id);
    expect(final!.tier).toBe('starter');
    // second returns existing tenant without mutation
    expect(second).toBeDefined();
  });

  it('creates tenant if stripe customer not found', async () => {
    const store = new MemoryTenantStore();
    const svc = new FakeStripeService();
    const event = makeInvoicePaidEvent({
      eventId: 'evt_new_customer',
      customerId: 'cus_new_999',
      priceId: 'price_starter_test',
    });
    const result = await handleInvoicePaid(event, store, svc);
    expect(result).toBeDefined();
    expect(result!.tier).toBe('starter');
    expect(result!.stripeCustomerId).toBe('cus_new_999');
    const all = await store.list();
    expect(all).toHaveLength(1);
  });

  it('second tenant isolation via webhook', async () => {
    const store = new MemoryTenantStore();
    const a = await store.create('TenantA', 'starter');
    await store.setStripeCustomerId(a.id, 'cus_A_123');
    const b = await store.create('TenantB', 'starter');
    await store.setStripeCustomerId(b.id, 'cus_B_456');

    const svc = new FakeStripeService();
    const evtB = makeInvoicePaidEvent({
      eventId: 'evt_isolate_B',
      customerId: 'cus_B_456',
      priceId: 'price_pro_test',
    });
    await handleInvoicePaid(evtB, store, svc);

    const afterA = await store.findById(a.id);
    const afterB = await store.findById(b.id);
    expect(afterA!.tier).toBe('starter');
    expect(afterB!.tier).toBe('pro');
  });

  it('non-invoice.paid event returns undefined', async () => {
    const store = new MemoryTenantStore();
    const svc = new FakeStripeService();
    const evt = {
      id: 'evt_other',
      type: 'customer.subscription.updated',
      data: { object: {} },
    } as unknown as Stripe.Event;
    const result = await handleInvoicePaid(evt, store, svc);
    expect(result).toBeUndefined();
  });
});
