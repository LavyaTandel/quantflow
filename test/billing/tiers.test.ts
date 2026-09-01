import { describe, expect, it } from 'vitest';
import { TIERS, getTier, isFeatureAllowed } from '../../src/billing/tiers.js';

describe('tiers', () => {
  it('defines three tiers with correct pricing and stripe ids', () => {
    expect(TIERS.starter.priceMonthly).toBe(49);
    expect(TIERS.starter.displayName).toBe('Starter');
    expect(TIERS.starter.stripePriceId).toBe('price_starter_test');

    expect(TIERS.pro.priceMonthly).toBe(199);
    expect(TIERS.pro.displayName).toBe('Pro');
    expect(TIERS.pro.stripePriceId).toBe('price_pro_test');

    expect(TIERS.enterprise.priceMonthly).toBeNull();
    expect(TIERS.enterprise.displayName).toBe('Enterprise');
    expect(TIERS.enterprise.stripePriceId).toBeNull();
  });

  it('getTier returns definitions and undefined for unknown', () => {
    expect(getTier('starter')).toEqual(TIERS.starter);
    expect(getTier('pro')).toEqual(TIERS.pro);
    expect(getTier('enterprise')).toEqual(TIERS.enterprise);
    expect(getTier('unknown')).toBeUndefined();
  });

  it('isFeatureAllowed per tier', () => {
    expect(isFeatureAllowed('starter', 'F1')).toBe(true);
    expect(isFeatureAllowed('starter', 'F2')).toBe(false);
    expect(isFeatureAllowed('starter', 'F3')).toBe(false);
    expect(isFeatureAllowed('starter', 'F4')).toBe(false);

    expect(isFeatureAllowed('pro', 'F1')).toBe(true);
    expect(isFeatureAllowed('pro', 'F2')).toBe(true);
    expect(isFeatureAllowed('pro', 'F3')).toBe(false);
    expect(isFeatureAllowed('pro', 'F4')).toBe(false);

    expect(isFeatureAllowed('enterprise', 'F1')).toBe(true);
    expect(isFeatureAllowed('enterprise', 'F2')).toBe(true);
    expect(isFeatureAllowed('enterprise', 'F3')).toBe(true);
    expect(isFeatureAllowed('enterprise', 'F4')).toBe(true);

    expect(isFeatureAllowed(TIERS.pro, 'F1')).toBe(true);
    expect(isFeatureAllowed(TIERS.pro, 'F4')).toBe(false);
    expect(isFeatureAllowed('unknown', 'F1')).toBe(false);
  });

  it('exposes correct rate limits per tier', () => {
    expect(TIERS.starter.rateLimits).toEqual({
      api: 60,
      wsConnections: 5,
      alertsPerSymbolPerSec: 0,
    });
    expect(TIERS.pro.rateLimits).toEqual({
      api: 300,
      wsConnections: 20,
      alertsPerSymbolPerSec: 1,
    });
    expect(TIERS.enterprise.rateLimits.wsConnections).toBe(100);
    expect(TIERS.enterprise.rateLimits.alertsPerSymbolPerSec).toBe(1);
    expect(TIERS.enterprise.rateLimits.api).toBe(Infinity);
  });
});
