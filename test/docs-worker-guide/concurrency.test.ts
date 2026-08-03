/**
 * Executable proof for /guide/worker/concurrency/
 * ("Worker Concurrency and Batch Pulling").
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
  describe(`worker guide · concurrency and batching [${mode}]`, () => {
    test('concurrency defaults to one job at a time', async () => {
      harness = await startHarness('worker-concurrency', mode);
      const queue = harness.queue('serial');
      await queue.pauseAsync();

      let inFlight = 0;
      let peak = 0;
      let done = 0;
      const worker = harness.worker(queue.name, async () => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        await Bun.sleep(40);
        inFlight--;
        done++;
        return true;
      });
      expect(worker.concurrency).toBe(1);

      for (let i = 0; i < 5; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => done === 5, 'every job to finish', 20_000);
      expect(peak).toBe(1);
    }, 30_000);

    test('concurrency: 5 runs up to five jobs at once', async () => {
      harness = await startHarness('worker-concurrency', mode);
      const queue = harness.queue('parallel');
      await queue.pauseAsync();

      let inFlight = 0;
      let peak = 0;
      let done = 0;
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
        { concurrency: 5 }
      );

      for (let i = 0; i < 20; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();

      await waitUntil(() => done === 20, 'every job to finish', 30_000);
      expect(peak).toBeLessThanOrEqual(5);
      expect(peak).toBeGreaterThan(1);
    }, 40_000);

    test('concurrency can be changed at runtime and clamps to a minimum of one', async () => {
      harness = await startHarness('worker-concurrency', mode);
      const queue = harness.queue('runtime');
      const worker = harness.worker(queue.name, async () => true, { concurrency: 5 });

      worker.concurrency = 10;
      expect(worker.concurrency).toBe(10);

      worker.concurrency = 2;
      expect(worker.concurrency).toBe(2);

      worker.concurrency = 0;
      expect(worker.concurrency).toBe(1);

      worker.concurrency = -5;
      expect(worker.concurrency).toBe(1);
    });

    test('raising concurrency at runtime widens real parallelism', async () => {
      harness = await startHarness('worker-concurrency', mode);
      const queue = harness.queue('scale-up');
      await queue.pauseAsync();

      let inFlight = 0;
      let peak = 0;
      let done = 0;
      const worker = harness.worker(
        queue.name,
        async () => {
          inFlight++;
          peak = Math.max(peak, inFlight);
          await Bun.sleep(60);
          inFlight--;
          done++;
          return true;
        },
        { concurrency: 1 }
      );

      for (let i = 0; i < 12; i++) await queue.add(`job-${i}`, { i }, { durable: true });
      await queue.resumeAsync();
      await waitUntil(() => done >= 1, 'the serial worker to start', 15_000);
      expect(peak).toBe(1);

      worker.concurrency = 4;
      await waitUntil(() => done === 12, 'every job to finish', 30_000);
      expect(peak).toBeGreaterThan(1);
      expect(peak).toBeLessThanOrEqual(4);
    }, 40_000);

    test('batchSize pulls many jobs per request', async () => {
      harness = await startHarness('worker-concurrency', mode);
      const queue = harness.queue('batched');
      await queue.pauseAsync();

      let done = 0;
      harness.worker(
        queue.name,
        async () => {
          done++;
          return true;
        },
        { batchSize: 100, concurrency: 4 }
      );
      await queue.addBulk(
        Array.from({ length: 60 }, (_, index) => ({ name: `job-${index}`, data: { index } }))
      );
      await queue.resumeAsync();

      await waitUntil(() => done === 60, 'the whole batch to be processed', 30_000);
      // TCP acknowledgements are batched, so the broker counter settles after.
      await waitUntil(
        async () => (await queue.getCompletedCount()) === 60,
        'the broker to record every completion',
        20_000
      );
    }, 40_000);

    test('a long-polling worker is woken immediately by a bulk push', async () => {
      harness = await startHarness('worker-concurrency', mode);
      const queue = harness.queue('long-poll');

      const started: number[] = [];
      harness.worker(
        queue.name,
        async () => {
          started.push(Date.now());
          return true;
        },
        { pollTimeout: 5_000, batchSize: 10 }
      );
      // Let the worker settle into its long poll before the bulk push arrives.
      await Bun.sleep(300);

      const pushedAt = Date.now();
      await queue.addBulk([
        { name: 'a', data: { i: 1 } },
        { name: 'b', data: { i: 2 } },
      ]);

      await waitUntil(() => started.length === 2, 'both bulk jobs to start', 10_000);
      expect(started[0] - pushedAt).toBeLessThan(4_000);
    }, 30_000);
  });
}
