import { describe, expect, it } from 'vitest';
import { extractApiKey, authenticate, AuthError } from '../../src/gateway/auth.js';
import type { TenantStore } from '../../src/gateway/auth.js';

function req(headers: Record<string, string | string[] | undefined>) {
  return { headers } as { headers: Record<string, string | string[] | undefined> };
}

describe('extractApiKey', () => {
  it('extracts from x-api-key header', () => {
    expect(extractApiKey(req({ 'x-api-key': 'key-123' }))).toBe('key-123');
  });

  it('extracts from Authorization Bearer', () => {
    expect(extractApiKey(req({ authorization: 'Bearer token-xyz' }))).toBe('token-xyz');
  });

  it('prefers x-api-key over Authorization', () => {
    expect(extractApiKey(req({ 'x-api-key': 'a', authorization: 'Bearer b' }))).toBe('a');
  });

  it('handles array header values', () => {
    expect(extractApiKey(req({ 'x-api-key': ['arr-key'] }))).toBe('arr-key');
  });

  it('returns undefined when missing', () => {
    expect(extractApiKey(req({}))).toBeUndefined();
  });

  it('is case-insensitive for bearer prefix', () => {
    expect(extractApiKey(req({ authorization: 'bearer lower' }))).toBe('lower');
  });
});

describe('authenticate', () => {
  it('returns tenant on success', async () => {
    const store: TenantStore = {
      findByApiKey: async (k) =>
        k === 'k1' ? { id: 't1', tier: 'starter', apiKey: 'k1' } : undefined,
    };
    await expect(authenticate('k1', store)).resolves.toEqual({
      id: 't1',
      tier: 'starter',
      apiKey: 'k1',
    });
  });

  it('returns undefined for unknown key', async () => {
    const store: TenantStore = { findByApiKey: async () => undefined };
    await expect(authenticate('bad', store)).resolves.toBeUndefined();
  });

  it('returns undefined for empty key', async () => {
    const store: TenantStore = {
      findByApiKey: async () => {
        throw new Error('should not be called');
      },
    };
    await expect(authenticate('', store)).resolves.toBeUndefined();
  });

  it('AuthError has 401 status', () => {
    const err = new AuthError('nope');
    expect(err.statusCode).toBe(401);
    expect(err.name).toBe('AuthError');
  });
});
