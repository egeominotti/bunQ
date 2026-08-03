/**
 * Executable proof for /guide/queue/limits/
 * ("Queue Rate Limiting and Global Concurrency").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`queue guide · limits [${mode}]`, () => {
    test('no rate limit and no concurrency cap are configured by default', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('limits');

      expect(await queue.getGlobalRateLimit()).toBeNull();
      expect(await queue.getGlobalConcurrency()).toBeNull();
      expect(await queue.getRateLimitTtl()).toBe(-2);
      expect(await queue.isMaxed()).toBe(false);
    });

    test('setGlobalConcurrency and its async twin read back', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('limits');

      queue.setGlobalConcurrency(10);
      await waitUntil(async () => (await queue.getGlobalConcurrency()) === 10, 'concurrency 10');

      await queue.setGlobalConcurrencyAsync(4);
      expect(await queue.getGlobalConcurrency()).toBe(4);

      queue.removeGlobalConcurrency();
      await waitUntil(
        async () => (await queue.getGlobalConcurrency()) === null,
        'the concurrency cap to be cleared'
      );

      await queue.setGlobalConcurrencyAsync(2);
      await queue.removeGlobalConcurrencyAsync();
      expect(await queue.getGlobalConcurrency()).toBeNull();
    });

    test('setGlobalRateLimit stores max and window, defaulting to one second', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('limits');

      queue.setGlobalRateLimit(100);
      await waitUntil(
        async () => (await queue.getGlobalRateLimit())?.max === 100,
        'the rate limit to be applied'
      );
      expect(await queue.getGlobalRateLimit()).toEqual({ max: 100, duration: 1000 });

      queue.setGlobalRateLimit(100, 60_000);
      await waitUntil(
        async () => (await queue.getGlobalRateLimit())?.duration === 60_000,
        'the custom window to be applied'
      );

      queue.removeGlobalRateLimit();
      await waitUntil(
        async () => (await queue.getGlobalRateLimit()) === null,
        'the rate limit to be cleared'
      );

      await queue.setGlobalRateLimitAsync(100, 60_000);
      expect(await queue.getGlobalRateLimit()).toEqual({ max: 100, duration: 60_000 });
      await queue.removeGlobalRateLimitAsync();
      expect(await queue.getGlobalRateLimit()).toBeNull();
    });

    test('a global concurrency cap holds across every worker on the queue', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('concurrency');
      await queue.pauseAsync();
      await queue.setGlobalConcurrencyAsync(1);

      let inFlight = 0;
      let peak = 0;
      let done = 0;
      for (let i = 0; i < 2; i++) {
        harness.worker(
          queue.name,
          async () => {
            inFlight++;
            peak = Math.max(peak, inFlight);
            await Bun.sleep(60);
            inFlight--;
            done++;
            return true;
          },
          { concurrency: 4 }
        );
      }
      for (let i = 0; i < 6; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => done === 6, 'every job to be processed', 20_000);
      expect(peak).toBe(1);
    }, 40_000);

    test('isMaxed reports an exhausted concurrency cap', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('maxed');
      await queue.setGlobalConcurrencyAsync(1);
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async () => {
        await held;
        return true;
      });
      await queue.add('holder', {}, { durable: true });

      await waitUntil(() => queue.isMaxed(), 'the queue to report maxed');
      release();
      await waitUntil(async () => !(await queue.isMaxed()), 'the slot to be released');
    }, 20_000);

    test('a queue rate limit throttles how many jobs start per window', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('throttled');
      await queue.pauseAsync();
      await queue.setGlobalRateLimitAsync(2, 60_000);

      let started = 0;
      harness.worker(
        queue.name,
        async () => {
          started++;
          return true;
        },
        { concurrency: 5 }
      );
      for (let i = 0; i < 8; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => started >= 2, 'the first window to be consumed', 10_000);
      await Bun.sleep(500);
      expect(started).toBe(2);
      expect(await queue.getRateLimitTtl()).toBeGreaterThan(0);
    }, 30_000);

    test('rateLimit(ms) applies a temporary throttle that the server expires', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('temporary');

      await queue.rateLimit(700);

      expect(await queue.getGlobalRateLimit()).toMatchObject({ max: 1 });
      expect(await queue.getRateLimitTtl()).toBeGreaterThan(0);

      await waitUntil(
        async () => (await queue.getGlobalRateLimit()) === null,
        'the server to expire the temporary throttle',
        10_000
      );
      expect(await queue.getRateLimitTtl()).toBe(-2);
    }, 20_000);

    test('rateLimit rejects a non-positive or non-finite duration', async () => {
      harness = await startHarness('limits', mode);
      const queue = harness.queue('temporary');

      await expect(queue.rateLimit(0)).rejects.toThrow(/positive finite number/);
      await expect(queue.rateLimit(-1)).rejects.toThrow(/positive finite number/);
      await expect(queue.rateLimit(Number.POSITIVE_INFINITY)).rejects.toThrow(
        /positive finite number/
      );
      await expect(queue.rateLimit(Number.NaN)).rejects.toThrow(/positive finite number/);
    });
  });
}
