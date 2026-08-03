/**
 * Executable proof for /guide/worker/stalls/
 * ("Heartbeats, Stall Detection and Lock Ownership").
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
  describe(`worker guide · heartbeats and locks [${mode}]`, () => {
    test('automatic heartbeats keep a long job alive well past stallInterval', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('heartbeating');
      const stalled: string[] = [];

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 400,
        maxStalls: 2,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => {
          await Bun.sleep(3_000);
          return true;
        },
        { heartbeatInterval: 50, useLocks: true }
      );
      worker.on('error', () => undefined);
      worker.on('stalled', (jobId: string) => stalled.push(jobId));

      const job = await queue.add('long', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 30_000);

      expect(stalled).toEqual([]);
    }, 60_000);

    test('a worker without heartbeats is detected as stalled', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('silent');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 50,
        maxStalls: 5,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('silent', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await waitUntil(
        async () => (await queue.getJobState(job.id)) !== 'active',
        'the broker to recover the silent job',
        30_000
      );
    }, 60_000);

    test('useLocks: true hands the processor a lock token', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('locked');
      let token: string | undefined;

      harness.worker(
        queue.name,
        async (job) => {
          token = job.token;
          return true;
        },
        { useLocks: true }
      );
      const job = await queue.add('task', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect(token).toBeString();
      expect(token).not.toBe('');
    }, 20_000);

    test('useLocks: false processes without a token', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('unlocked');
      let token: string | undefined = 'unset';

      harness.worker(
        queue.name,
        async (job) => {
          token = job.token;
          return true;
        },
        { useLocks: false }
      );
      const job = await queue.add('task', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect(token).toBeUndefined();
    }, 20_000);

    test('delivery is exclusive while the lease is valid', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('exclusive');
      const deliveries: string[] = [];
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      for (let i = 0; i < 3; i++) {
        const worker = harness.worker(
          queue.name,
          async (job) => {
            deliveries.push(job.id);
            await held;
            return true;
          },
          { useLocks: true, lockDuration: 60_000, heartbeatInterval: 100 }
        );
        worker.on('error', () => undefined);
      }

      const job = await queue.add('single', {}, { durable: true });
      await waitForState(queue, job.id, 'active', 10_000);
      await Bun.sleep(500);

      expect(deliveries).toEqual([job.id]);
      release();
      await waitForState(queue, job.id, 'completed', 10_000);
    }, 30_000);

    test('a redelivery to the same worker starts a new processing generation', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('redelivery');
      let deliveries = 0;

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 10,
        gracePeriod: 0,
      });
      // Spare concurrency lets a recovered generation run while the stale
      // handler is still blocked. Its newer lease makes the stale outcome safe.
      const worker = harness.worker(
        queue.name,
        async () => {
          deliveries++;
          return await new Promise<never>(() => undefined);
        },
        { heartbeatInterval: 0, useLocks: true, concurrency: 4 }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('stuck', {}, { attempts: 20, durable: true });
      await waitForState(queue, job.id, 'active', 10_000);

      await waitUntil(() => deliveries >= 2, 'the recovered processing generation', 25_000);
      expect(deliveries).toBeGreaterThanOrEqual(2);
    }, 60_000);

    test('extendJobLocks renews the lease of an owned job', async () => {
      harness = await startHarness('worker-stalls', mode);
      const queue = harness.queue('extending');
      let resolveOwned: (value: { id: string; token: string }) => void = () => undefined;
      const owned = new Promise<{ id: string; token: string }>((resolve) => {
        resolveOwned = resolve;
      });
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      const worker = harness.worker(
        queue.name,
        async (job) => {
          resolveOwned({ id: job.id, token: job.token ?? '' });
          await held;
          return true;
        },
        { useLocks: true, lockDuration: 30_000, skipLockRenewal: true, heartbeatInterval: 0 }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('long', {}, { durable: true });
      const info = await owned;

      expect(await worker.extendJobLocks([info.id], [info.token], 60_000)).toBe(1);
      // A token that does not own the job cannot extend it.
      expect(await worker.extendJobLocks([info.id], ['not-my-token'], 60_000)).toBe(0);

      release();
      await waitForState(queue, job.id, 'completed', 10_000);
    }, 30_000);
  });
}
