/**
 * Throughput Tracker Tests
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { ThroughputTracker } from '../src/application/throughputTracker';

describe('ThroughputTracker', () => {
  let tracker: ThroughputTracker;

  beforeEach(() => {
    tracker = new ThroughputTracker();
  });

  test('should start with zero rates', () => {
    const rates = tracker.getRates();
    expect(rates.pushPerSec).toBe(0);
    expect(rates.pullPerSec).toBe(0);
    expect(rates.completePerSec).toBe(0);
    expect(rates.failPerSec).toBe(0);
  });

  test('should track push rate', async () => {
    for (let i = 0; i < 100; i++) {
      tracker.pushRate.increment();
    }

    // Wait a bit for rate calculation
    await Bun.sleep(150);

    const rates = tracker.getRates();
    expect(rates.pushPerSec).toBeGreaterThan(0);
  });

  test('should track pull rate', async () => {
    for (let i = 0; i < 50; i++) {
      tracker.pullRate.increment();
    }

    await Bun.sleep(150);

    const rates = tracker.getRates();
    expect(rates.pullPerSec).toBeGreaterThan(0);
  });

  test('should track batch increments', async () => {
    tracker.pushRate.increment(100);

    await Bun.sleep(150);

    const rates = tracker.getRates();
    expect(rates.pushPerSec).toBeGreaterThan(0);
  });

  test('should track complete and fail rates', async () => {
    for (let i = 0; i < 30; i++) {
      tracker.completeRate.increment();
    }
    for (let i = 0; i < 10; i++) {
      tracker.failRate.increment();
    }

    await Bun.sleep(150);

    const rates = tracker.getRates();
    expect(rates.completePerSec).toBeGreaterThan(0);
    expect(rates.failPerSec).toBeGreaterThan(0);
  });

  test('should handle rapid successive calls', () => {
    // Calling getRate() rapidly should not cause division by zero
    const rates1 = tracker.getRates();
    const rates2 = tracker.getRates();

    expect(rates1.pushPerSec).toBe(0);
    expect(rates2.pushPerSec).toBe(0);
  });

  test('rates should decrease over time without new events', async () => {
    tracker.pushRate.increment(1000);

    await Bun.sleep(200);
    const rate1 = tracker.getRates().pushPerSec;

    // No new increments
    await Bun.sleep(200);
    const rate2 = tracker.getRates().pushPerSec;

    // Rate should decrease (EMA decays without new events)
    expect(rate2).toBeLessThan(rate1);
  });
});
