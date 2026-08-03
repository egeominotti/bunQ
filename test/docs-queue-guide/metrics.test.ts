/**
 * Executable proof for /guide/queue/metrics/
 * ("Workers, Stats and Metrics from the Queue").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`queue guide · workers, stats and metrics [${mode}]`, () => {
    test('getWorkers and getWorkersCount list the workers on this queue', async () => {
      harness = await startHarness('metrics', mode);
      const queue = harness.queue('observed');
      const other = harness.queue('unobserved');

      expect(await queue.getWorkers()).toEqual([]);
      expect(await queue.getWorkersCount()).toBe(0);

      const worker = harness.worker(queue.name, async () => true, { concurrency: 3 });
      await worker.waitUntilReady();

      await waitUntil(async () => (await queue.getWorkersCount()) === 1, 'the worker to register');
      const workers = await queue.getWorkers();
      expect(workers).toHaveLength(1);
      expect(workers[0].queues).toContain(queue.name);
      expect(workers[0].concurrency).toBe(3);

      // A worker on another queue is not reported here.
      expect(await other.getWorkersCount()).toBe(0);
    }, 20_000);

    test('getMetrics reports completed and failed counters', async () => {
      harness = await startHarness('metrics', mode);
      const queue = harness.queue('metrics');
      harness.worker(queue.name, async (job) => {
        if ((job.data as { fail?: boolean }).fail) throw new Error('boom');
        return true;
      });

      const done = await queue.add('ok', { fail: false }, { durable: true });
      const bad = await queue.add('bad', { fail: true }, { attempts: 1, durable: true });
      await waitForState(queue, done.id, 'completed', 10_000);
      await waitForState(queue, bad.id, 'failed', 10_000);

      const completed = await queue.getMetrics('completed', 0, 100);
      const failed = await queue.getMetrics('failed', 0, 100);

      expect(completed.meta.count).toBeGreaterThanOrEqual(1);
      expect(failed.meta.count).toBeGreaterThanOrEqual(1);
      expect(completed.data).toBeArray();
      expect(failed.data).toBeArray();
    }, 30_000);

    // The public window uses inclusive newest-first bucket indexes.
    test('getMetrics returns the requested window as a data series', async () => {
      harness = await startHarness('metrics', mode);
      const queue = harness.queue('metrics-window');
      harness.worker(queue.name, async () => true);
      const job = await queue.add('ok', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect((await queue.getMetrics('completed', 0, 100)).data.length).toBeGreaterThan(0);
    }, 30_000);

    // Counters and buckets must remain isolated by queue.
    test('getMetrics counts only this queue', async () => {
      harness = await startHarness('metrics', mode);
      const queue = harness.queue('metrics-scoped');
      const other = harness.queue('metrics-other');
      harness.worker(queue.name, async () => true);
      harness.worker(other.name, async () => true);

      const mine = await queue.add('mine', {}, { durable: true });
      const theirs = await other.add('theirs', {}, { durable: true });
      await waitForState(queue, mine.id, 'completed', 10_000);
      await waitForState(other, theirs.id, 'completed', 10_000);

      expect((await queue.getMetrics('completed', 0, 100)).meta.count).toBe(1);
    }, 30_000);

    test('trimEvents removes old entries from this queue event journal', async () => {
      harness = await startHarness('metrics', mode);
      const queue = harness.queue('events');
      harness.worker(queue.name, async () => true);

      const jobs = await queue.addBulk(
        Array.from({ length: 3 }, (_, index) => ({
          name: `event-${index}`,
          data: { index },
          opts: { durable: true },
        }))
      );
      for (const job of jobs) await waitForState(queue, job.id, 'completed', 10_000);

      expect(await queue.trimEvents(2)).toBeGreaterThan(0);
      expect(await queue.trimEvents(2)).toBe(0);
    }, 30_000);
  });
}
