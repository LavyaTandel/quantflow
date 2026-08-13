import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  BackoffScheduler,
  ConnectBudget,
  ControlRateLimiter,
} from '../../src/ingest/binance-socket.js';

describe('BackoffScheduler', () => {
  it('grows exponentially and caps at maxDelayMs', () => {
    const scheduler = new BackoffScheduler({
      baseDelayMs: 100,
      maxDelayMs: 1_000,
      rng: () => 1,
      maxAttemptsPerWindow: 1_000_000,
      windowMs: 3_600_000,
    });
    const delays = [0, 1, 2, 3, 4, 5].map((n) => scheduler.nextDelayMs(n * 1000));
    expect(delays).toEqual([100, 200, 400, 800, 1000, 1000]);
    expect(scheduler.attemptCount).toBe(6);
  });

  it('full jitter keeps delays within [0, exponential ceiling]', () => {
    const scheduler = new BackoffScheduler({
      baseDelayMs: 200,
      maxDelayMs: 5_000,
      rng: () => 0,
      maxAttemptsPerWindow: 1_000_000,
      windowMs: 3_600_000,
    });
    for (let n = 0; n < 20; n++) expect(scheduler.nextDelayMs(n)).toBe(0);
    const mid = new BackoffScheduler({
      baseDelayMs: 200,
      maxDelayMs: 5_000,
      rng: () => 0.5,
      maxAttemptsPerWindow: 1_000_000,
      windowMs: 3_600_000,
    });
    expect(mid.nextDelayMs(0)).toBe(100);
  });

  it('resets the exponent after a successful connection', () => {
    const scheduler = new BackoffScheduler({ baseDelayMs: 100, maxDelayMs: 10_000, rng: () => 1 });
    scheduler.nextDelayMs(0);
    scheduler.nextDelayMs(0);
    scheduler.reset();
    expect(scheduler.attemptCount).toBe(0);
    expect(scheduler.nextDelayMs(0)).toBe(100);
  });

  it('honors the attempts-per-window budget with continuous refill', () => {
    const scheduler = new BackoffScheduler({
      baseDelayMs: 10,
      maxDelayMs: 10,
      rng: () => 0,
      maxAttemptsPerWindow: 300,
      windowMs: 300_000,
    });
    let now = 0;
    for (let i = 0; i < 300; i++) {
      expect(scheduler.nextDelayMs(now)).toBe(0);
    }
    expect(scheduler.nextDelayMs(now)).toBe(1000);
    now += 500;
    expect(scheduler.nextDelayMs(now)).toBe(500);
    now += 60_000;
    expect(scheduler.nextDelayMs(now)).toBe(0);
  });

  describe('with fake timers', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('never sleeps: default-clock scheduling advances only via setSystemTime', () => {
      vi.setSystemTime(0);
      const scheduler = new BackoffScheduler({ baseDelayMs: 50, maxDelayMs: 50, rng: () => 0 });
      for (let i = 0; i < 300; i++) scheduler.nextDelayMs();
      vi.setSystemTime(500);
      const forced = scheduler.nextDelayMs();
      expect(forced).toBeGreaterThanOrEqual(1);
      expect(forced).toBeLessThanOrEqual(1000);
      vi.setSystemTime(60_000);
      expect(scheduler.nextDelayMs()).toBe(0);
    });
  });
});

describe('ConnectBudget', () => {
  it('starts full, drains, and refills at window rate', () => {
    const budget = new ConnectBudget({ maxAttemptsPerWindow: 300, windowMs: 300_000 });
    let now = 0;
    for (let i = 0; i < 300; i++) budget.acquireWaitMs(now);
    expect(budget.acquireWaitMs(now)).toBe(1000);
    now += 2500;
    expect(budget.acquireWaitMs(now)).toBe(0);
    expect(budget.acquireWaitMs(now)).toBe(0);
    expect(budget.acquireWaitMs(now)).toBe(500);
  });
});

describe('ControlRateLimiter', () => {
  it('coalesces duplicates and sends at most one message per interval', () => {
    const limiter = new ControlRateLimiter(250, 1);
    expect(limiter.enqueue({ method: 'SUBSCRIBE', params: ['btcusdt@aggTrade'] })).toBeDefined();
    expect(limiter.enqueue({ method: 'SUBSCRIBE', params: ['btcusdt@aggTrade'] })).toBeUndefined();
    limiter.enqueue({ method: 'SUBSCRIBE', params: ['ethusdt@aggTrade'] });
    expect(limiter.pending).toBe(2);
    expect(limiter.takeDue(0, -Infinity)).toHaveLength(1);
    expect(limiter.takeDue(100, 0)).toHaveLength(0);
    expect(limiter.takeDue(250, 0)).toHaveLength(1);
    expect(limiter.pending).toBe(0);
  });

  it('keeps sustained traffic under five messages per second', () => {
    const limiter = new ControlRateLimiter(250, 1);
    for (let i = 0; i < 10; i++) {
      expect(limiter.enqueue({ method: 'SUBSCRIBE', params: [`s${i}@x`] })).toBeDefined();
    }
    let lastSent = -Infinity;
    let sent = 0;
    for (let now = 0; now <= 1000; now += 50) {
      const messages = limiter.takeDue(now, lastSent);
      if (messages.length > 0) lastSent = now;
      sent += messages.length;
    }
    expect(sent).toBeLessThanOrEqual(5);
  });
});
