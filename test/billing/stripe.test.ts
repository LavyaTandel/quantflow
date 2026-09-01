import { describe, expect, it } from 'vitest';
import { FakeStripeService, createTestStripeService } from '../../src/billing/stripe.js';

describe('FakeStripeService', () => {
  it('createCustomer -> createSubscription flow per tier starter/pro active', async () => {
    const stripe = new FakeStripeService();
    const { customerId: c1 } = await stripe.createCustomer('Acme');
    const sub1 = await stripe.createSubscription(c1, 'starter');
    expect(sub1.status).toBe('active');
    expect(sub1.subscriptionId).toMatch(/^sub_test_/);

    const { customerId: c2 } = await stripe.createCustomer('Beta');
    const sub2 = await stripe.createSubscription(c2, 'pro');
    expect(sub2.status).toBe('active');
  });

  it('enterprise tier throws or requires custom contract', async () => {
    const stripe = new FakeStripeService();
    const { customerId } = await stripe.createCustomer('Enterprise Co');
    await expect(stripe.createSubscription(customerId, 'enterprise')).rejects.toThrow(
      /custom contract/,
    );
  });

  it('second tenant isolation', async () => {
    const stripe = createTestStripeService() as FakeStripeService;
    const { customerId: a } = await stripe.createCustomer('A');
    const { customerId: b } = await stripe.createCustomer('B');
    expect(a).not.toBe(b);

    const subA = await stripe.createSubscription(a, 'starter');
    const subB = await stripe.createSubscription(b, 'pro');
    expect(subA.subscriptionId).not.toBe(subB.subscriptionId);

    expect(await stripe.getCustomer(a)).toMatchObject({ customerId: a, name: 'A' });
    expect(await stripe.getCustomer(b)).toMatchObject({ customerId: b, name: 'B' });
    expect(await stripe.getCustomer('cus_test_missing')).toBeUndefined();
  });

  it('handleWebhook is no-op and does not throw', async () => {
    const stripe = new FakeStripeService();
    await expect(stripe.handleWebhook({ type: 'invoice.paid', data: {} })).resolves.toBeUndefined();
    await expect(
      stripe.handleWebhook({ type: 'customer.subscription.updated' }),
    ).resolves.toBeUndefined();
    expect(stripe.getEvents()).toHaveLength(2);
  });

  it('createSubscription validates customer existence and tier', async () => {
    const stripe = new FakeStripeService();
    await expect(stripe.createSubscription('cus_test_missing', 'starter')).rejects.toThrow(
      /customer not found/,
    );
    const { customerId } = await stripe.createCustomer('X');
    await expect(
      stripe.createSubscription(customerId, 'unknown' as unknown as 'starter'),
    ).rejects.toThrow(/unknown tier/);
  });
});
