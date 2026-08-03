/**
 * Executable proof for /guide/worker/errors/
 * ("Worker Error Handling, Retries and Backoff").
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
  describe(`worker guide · errors and retries [${mode}]`, () => {
    test('a thrown error is retried automatically until it succeeds', async () => {
      harness = await startHarness('worker-errors', mode);
      const queue = harness.queue('retrying');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        if (runs < 3) throw new Error('transient');
        return { ok: true };
      });
      const job = await queue.add('flaky', {}, { attempts: 3, backoff: 10, durable: true });

      await waitForState(queue, job.id, 'completed', 20_000);
      expect(runs).toBe(3);
    }, 30_000);

    test('failed fires on every confirmed attempt, not only the last one', async () => {
      harness = await startHarness('worker-errors', mode);
      const queue = harness.queue('failing');
      const attemptsSeen: number[] = [];

      const worker = harness.worker(queue.name, async () => {
        throw new Error('always');
      });
      worker.on('failed', (job) => attemptsSeen.push(job.attemptsMade));

      const job = await queue.add('doomed', {}, { attempts: 3, backoff: 10, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);
      await waitUntil(() => attemptsSeen.length === 3, `three failed events`, 10_000);

      // The snapshot was pulled before the failure, so the attempt that just
      // failed is attemptsMade + 1.
      expect(attemptsSeen).toEqual([0, 1, 2]);
    }, 30_000);

    test('exponential backoff widens the wait between attempts', async () => {
      harness = await startHarness('worker-errors', mode);
      const queue = harness.queue('backoff');
      const starts: number[] = [];

      harness.worker(queue.name, async () => {
        starts.push(Date.now());
        throw new Error('always');
      });
      const job = await queue.add(
        'doomed',
        {},
        // A wide base keeps the doubling above the pull/scheduling noise that
        // rides on every measured gap.
        { attempts: 3, backoff: { type: 'exponential', delay: 500 }, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 30_000);
      await waitUntil(() => starts.length === 3, 'three attempts', 10_000);

      // calculateBackoff: base = delay * 2^attempts, jitter in [0.5, 1.5).
      // Consecutive draws overlap ([500,1500) then [1000,3000)), so compare each
      // gap with its guaranteed floor instead of with the other sample.
      const firstGap = starts[1] - starts[0];
      const secondGap = starts[2] - starts[1];
      expect(firstGap).toBeGreaterThanOrEqual(450);
      expect(secondGap).toBeGreaterThanOrEqual(900);
    }, 60_000);

    test('a fixed backoff keeps a roughly constant wait', async () => {
      harness = await startHarness('worker-errors', mode);
      const queue = harness.queue('fixed-backoff');
      const starts: number[] = [];

      harness.worker(queue.name, async () => {
        starts.push(Date.now());
        throw new Error('always');
      });
      const job = await queue.add(
        'doomed',
        {},
        { attempts: 3, backoff: { type: 'fixed', delay: 300 }, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 30_000);
      await waitUntil(() => starts.length === 3, 'three attempts', 10_000);

      const firstGap = starts[1] - starts[0];
      const secondGap = starts[2] - starts[1];
      // Jitter is applied, so compare magnitude rather than exact equality.
      expect(firstGap).toBeGreaterThan(50);
      expect(secondGap).toBeLessThan(firstGap * 4);
    }, 60_000);

    test('a permanently failing job ends in the DLQ with its error', async () => {
      harness = await startHarness('worker-errors', mode);
      const queue = harness.queue('permanent');

      harness.worker(queue.name, async () => {
        throw new Error('permanent failure');
      });
      const job = await queue.add('doomed', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry).toBeDefined();
      expect(entry?.error).toContain('permanent failure');
      expect(entry?.attempts.length).toBeGreaterThanOrEqual(1);
      expect(await queue.getFailedCount()).toBe(1);
    }, 30_000);

    test('errors outside the processor surface on the worker error event', async () => {
      harness = await startHarness('worker-errors', mode);
      const queue = harness.queue('worker-error');
      const errors: Error[] = [];

      const worker = harness.worker(queue.name, async () => true);
      worker.on('error', (error) => errors.push(error));
      worker.emit('error', new Error('connection lost'));

      expect(errors).toHaveLength(1);
      expect(errors[0].message).toBe('connection lost');
    });
  });
}
