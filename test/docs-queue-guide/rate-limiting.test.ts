/**
 * Executable proof for /guide/rate-limiting/
 * ("Rate Limiting & Concurrency for Bun Job Queues").
 *
 * The queue-level knobs are proven in limits.test.ts; this file covers the
 * page's own additional claims: the token bucket refill, the per-worker
 * concurrency option and the per-worker limiter.
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
  describe(`queue guide · rate limiting in depth [${mode}]`, () => {
    test('the two knobs are independent and both start unset', async () => {
      harness = await startHarness('rate-limiting', mode);
      const queue = harness.queue('knobs');

      expect(await queue.getGlobalRateLimit()).toBeNull();
      expect(await queue.getGlobalConcurrency()).toBeNull();

      await queue.setGlobalRateLimitAsync(100);
      expect(await queue.getGlobalConcurrency()).toBeNull();

      await queue.setGlobalConcurrencyAsync(5);
      expect(await queue.getGlobalRateLimit()).toEqual({ max: 100, duration: 1000 });

      await queue.removeGlobalRateLimitAsync();
      expect(await queue.getGlobalConcurrency()).toBe(5);
    });

    test('the queue rate limit is a bucket that refills inside the window', async () => {
      harness = await startHarness('rate-limiting', mode);
      const queue = harness.queue('bucket');
      await queue.pauseAsync();
      // 4 tokens spread over 2s: a continuous refill releases one every ~500ms,
      // a fixed window would release nothing until 2s have elapsed.
      await queue.setGlobalRateLimitAsync(4, 2_000);

      const starts: number[] = [];
      harness.worker(
        queue.name,
        async () => {
          starts.push(Date.now());
          return true;
        },
        { concurrency: 8 }
      );
      for (let i = 0; i < 6; i++) await queue.add(`job-${i}`, { i }, { durable: true });

      const resumedAt = Date.now();
      await queue.resumeAsync();
      await waitUntil(() => starts.length >= 4, 'the initial burst', 10_000);
      expect(Date.now() - resumedAt).toBeLessThan(1_500);

      await waitUntil(() => starts.length >= 5, 'the refilled token', 10_000);
      const fifth = starts[4] - resumedAt;
      expect(fifth).toBeGreaterThan(100);
      expect(fifth).toBeLessThan(2_000);
    }, 30_000);

    test('worker concurrency caps how many jobs that worker runs in parallel', async () => {
      harness = await startHarness('rate-limiting', mode);
      const queue = harness.queue('worker-concurrency');
      await queue.pauseAsync();

      let inFlight = 0;
      let peak = 0;
      let done = 0;
      harness.worker(
        queue.name,
        async () => {
          inFlight++;
          peak = Math.max(peak, inFlight);
          await Bun.sleep(80);
          inFlight--;
          done++;
          return true;
        },
        { concurrency: 3 }
      );
      for (let i = 0; i < 12; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => done === 12, 'every job to finish', 20_000);
      expect(peak).toBeLessThanOrEqual(3);
      expect(peak).toBeGreaterThan(1);
    }, 40_000);

    test('the worker limiter caps a sequential worker', async () => {
      harness = await startHarness('rate-limiting', mode);
      const queue = harness.queue('worker-limiter');
      await queue.pauseAsync();

      let started = 0;
      harness.worker(
        queue.name,
        async () => {
          started++;
          return true;
        },
        { concurrency: 1, limiter: { max: 2, duration: 60_000 } }
      );
      for (let i = 0; i < 6; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => started >= 2, 'the limiter budget to be consumed', 10_000);
      await Bun.sleep(600);
      expect(started).toBe(2);
      expect(await queue.getJobCountsAsync()).toMatchObject({
        waiting: 4,
        active: 0,
        completed: 2,
      });
    }, 30_000);

    test('the worker limiter caps a concurrent worker', async () => {
      harness = await startHarness('rate-limiting', mode);
      const queue = harness.queue('worker-limiter-concurrent');
      await queue.pauseAsync();

      let started = 0;
      harness.worker(
        queue.name,
        async () => {
          started++;
          await Bun.sleep(500);
          return true;
        },
        { concurrency: 5, limiter: { max: 2, duration: 60_000 } }
      );
      for (let i = 0; i < 6; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => started >= 2, 'the limiter budget to be consumed', 10_000);
      // Still inside the first processor's 500ms window: only the limiter can
      // hold the remaining slots back.
      await Bun.sleep(200);
      expect(started).toBe(2);
      await Bun.sleep(400);
      expect(await queue.getJobCountsAsync()).toMatchObject({
        waiting: 4,
        active: 0,
        completed: 2,
      });
    }, 30_000);
  });
}
