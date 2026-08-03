/**
 * Executable proof for /guide/cpu-intensive-workers/
 * ("CPU-Intensive Workers: Preserve Heartbeats and Locks").
 *
 * The page's core claim is a timing one: what matters is not total job duration
 * but the longest interval during which the worker cannot renew its lease.
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

/** Busy-loop that never yields for `ms`, blocking timers and I/O. */
function blockFor(ms: number): void {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    // intentionally busy
  }
}

/** Same amount of work, yielding every 500 iterations as the guide shows. */
async function blockWithYields(ms: number): Promise<void> {
  const until = Date.now() + ms;
  let operations = 0;
  while (Date.now() < until) {
    if (++operations % 500 === 0) await Bun.sleep(0);
  }
}

for (const mode of MODES) {
  describe(`worker guide · CPU-intensive workers [${mode}]`, () => {
    test('a non-yielding handler starves heartbeats and gets recovered', async () => {
      harness = await startHarness('cpu-intensive', mode);
      const queue = harness.queue('blocking');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 200,
        maxStalls: 5,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => {
          blockFor(3_000); // longer than stallInterval, no yield point
          return true;
        },
        { heartbeatInterval: 50, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('heavy', {}, { attempts: 10, durable: true });
      await waitUntil(
        async () => {
          const state = await queue.getJobState(job.id);
          return state === 'waiting' || state === 'failed';
        },
        'the blocked job to be recovered by the broker',
        30_000
      );
    }, 60_000);

    test('the same work with cooperative yields keeps the lease alive', async () => {
      harness = await startHarness('cpu-intensive', mode);
      const queue = harness.queue('yielding');
      const stalled: string[] = [];

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 400,
        maxStalls: 5,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => {
          await blockWithYields(3_000);
          return true;
        },
        { heartbeatInterval: 50, useLocks: true }
      );
      worker.on('error', () => undefined);
      worker.on('stalled', (jobId: string) => stalled.push(jobId));

      const job = await queue.add('heavy', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 30_000);

      expect(stalled).toEqual([]);
    }, 60_000);

    test('sizing lockDuration and stallInterval above the blocking window avoids recovery', async () => {
      harness = await startHarness('cpu-intensive', mode);
      const queue = harness.queue('sized');
      const stalled: string[] = [];

      // Safety margin: both windows are far above the worst blocking segment.
      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 120_000,
        maxStalls: 3,
        gracePeriod: 5_000,
      });
      const worker = harness.worker(
        queue.name,
        async () => {
          blockFor(1_500);
          return true;
        },
        { heartbeatInterval: 10_000, lockDuration: 120_000, useLocks: true }
      );
      worker.on('error', () => undefined);
      worker.on('stalled', (jobId: string) => stalled.push(jobId));

      const job = await queue.add('heavy', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 30_000);

      expect(stalled).toEqual([]);
    }, 60_000);

    test('heartbeatInterval: 0 disables lease renewal, as the caution warns', async () => {
      harness = await startHarness('cpu-intensive', mode);
      const queue = harness.queue('no-renewal');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 100,
        maxStalls: 5,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('idle', {}, { attempts: 10, durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await waitUntil(
        async () => (await queue.getJobState(job.id)) !== 'active',
        'the un-renewed lease to be recovered',
        30_000
      );
    }, 60_000);

    test('delivery is at least once: a recovered job is delivered again', async () => {
      harness = await startHarness('cpu-intensive', mode);
      const queue = harness.queue('at-least-once');
      const deliveries: string[] = [];

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 10,
        gracePeriod: 0,
      });
      // Two workers with lease renewal disabled: the stale handler keeps
      // running while the broker hands the job to the other worker.
      for (let i = 0; i < 2; i++) {
        const worker = harness.worker(
          queue.name,
          async (job) => {
            deliveries.push(job.id);
            return await new Promise<never>(() => undefined);
          },
          { heartbeatInterval: 0, useLocks: true, concurrency: 1 }
        );
        worker.on('error', () => undefined);
      }

      const job = await queue.add('heavy', {}, { attempts: 10, durable: true });
      await waitUntil(
        () => deliveries.filter((id) => id === job.id).length >= 2,
        'the job to be delivered a second time',
        40_000
      );
      expect(deliveries.filter((id) => id === job.id).length).toBeGreaterThanOrEqual(2);
    }, 60_000);
  });
}
