# Billing

## Tiers

- **Starter** ($49/mo, `price_starter_test` → live `STRIPE_PRICE_STARTER_LIVE`): usage-based, `rateLimits.api=60/min`, `alertsPerSymbolPerSec=0`, wsConnections 5. Metered per request.
- **Pro** ($199/mo, `price_pro_test` → live `STRIPE_PRICE_PRO_LIVE`): usage-based, `rateLimits.api=300/min`, `alertsPerSymbolPerSec=1`, wsConnections 20.
- **Enterprise** (custom contract, `stripePriceId=null`): seat-based, `rateLimits.api=Infinity`, wsConnections 100, unlimited. No Stripe price — seat license.

No silent drops: every webhook `invoice.paid` and rate-limit rejection is counted. Gateway `rateLimiter` returns 429 with `Retry-After` and `X-RateLimit-Remaining`; `derivedOnly:true` ensures no raw NormalizedEvent leaves pipeline.

## Stripe webhook

- Verify `stripe-signature` via `verifyStripeWebhook(payload, signature, secret, stripeClient?)` which wraps `stripe.webhooks.constructEvent`.
- Reconcile `invoice.paid` → `TenantStore.updateTier` via `handleInvoicePaid(event, tenantStore, stripeService)`.
- Price mapping reverse lookups `TIERS` (`price_starter_test`/`price_pro_test` → tier).
- Idempotency: `stripe:processed:{event.id}` SET NX EX 86400 (24h) in Redis, fallback in-memory Set for hermetic tests. Second delivery is no-op.
- Behind `authenticate` gateway shell (reuse TenantStore).

## Persistence

- `RedisTenantStore`: `tenant:{id}` HASH, `tenant:apikey:{apiKey}` → id, `tenant:index` SET. Uses `createClient` from `redis`.
- `RedisRateLimiter`: `ratelimit:{tenantId}:{windowStart}` INCR + EXPIRE 60, windowStart=floor(now/60s)*60s.
- `Memory*` stores kept for hermetic default tests.
