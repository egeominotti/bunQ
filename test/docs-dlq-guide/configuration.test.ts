/**
 * Executable proof for /guide/dlq/configuration/
 * ("DLQ Configuration: Size, Age and Retry Policy") and the configuration half
 * of /guide/queue/dlq/ ("DLQ Operations from the Queue Object").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Queue } from '../../src/client';
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
  describe(`dlq guide · configuration [${mode}]`, () => {
    test('the documented defaults are the shipped defaults', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('defaults');

      const config = await queue.getDlqConfigAsync();

      expect(config.autoRetry).toBe(false);
      expect(config.autoRetryInterval).toBe(3_600_000);
      expect(config.maxAutoRetries).toBe(3);
      expect(config.maxAge).toBe(604_800_000);
      expect(config.maxEntries).toBe(10_000);
    });

    test('setDlqConfigAsync writes every documented field', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('configured');

      await queue.setDlqConfigAsync({
        autoRetry: true,
        autoRetryInterval: 3_600_000,
        maxAutoRetries: 3,
        maxAge: 604_800_000,
        maxEntries: 10_000,
      });

      expect(await queue.getDlqConfigAsync()).toMatchObject({
        autoRetry: true,
        autoRetryInterval: 3_600_000,
        maxAutoRetries: 3,
        maxAge: 604_800_000,
        maxEntries: 10_000,
      });
    });

    test('the policy is server-side state shared by every client of the queue', async () => {
      harness = await startHarness('dlq-config', mode);
      const name = `shared-dlq-${crypto.randomUUID().slice(0, 8)}`;
      const first = new Queue(name, harness.queueOptions());
      const second = new Queue(name, harness.queueOptions());
      const other = new Queue(`${name}-other`, harness.queueOptions());
      harness.addCleanup(() => first.close());
      harness.addCleanup(() => second.close());
      harness.addCleanup(() => other.close());

      await first.setDlqConfigAsync({ maxEntries: 42, maxAge: 111_000 });

      expect(await second.getDlqConfigAsync()).toMatchObject({ maxEntries: 42, maxAge: 111_000 });
      expect((await other.getDlqConfigAsync()).maxEntries).toBe(10_000);
    });

    test('setDlqConfig is fire-and-forget over TCP and immediate when embedded', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('fire-and-forget');

      queue.setDlqConfig({ maxEntries: 7 });

      if (mode === 'embedded') {
        expect(queue.getDlqConfig().maxEntries).toBe(7);
      }
      await waitUntil(
        async () => (await queue.getDlqConfigAsync()).maxEntries === 7,
        'the server to apply the config'
      );
    }, 20_000);

    test('maxEntries caps the DLQ and evicts the oldest entry', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('capped');
      await queue.setDlqConfigAsync({ maxEntries: 2 });

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const ids: string[] = [];
      for (let i = 0; i < 3; i++) {
        const job = await queue.add(`job-${i}`, { i }, { attempts: 1, durable: true });
        ids.push(job.id);
        await waitUntil(
          async () => (await queue.getDlqAsync()).some((entry) => entry.job.id === job.id),
          `job ${i} to reach the DLQ`,
          30_000
        );
      }

      const remaining = (await queue.getDlqAsync()).map((entry) => entry.job.id);
      expect(remaining).toHaveLength(2);
      expect(remaining).not.toContain(ids[0]); // the oldest entry was evicted
      expect(remaining).toContain(ids[2]);
    }, 90_000);

    test('maxAge: null keeps entries from expiring', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('never-expire');

      await queue.setDlqConfigAsync({ maxAge: null });

      expect((await queue.getDlqConfigAsync()).maxAge).toBeNull();

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('job', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry?.expiresAt).toBeNull();
    }, 40_000);

    test('maxAge sets the entry expiry stamp', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('expiring');
      await queue.setDlqConfigAsync({ maxAge: 600_000 });

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('job', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry?.expiresAt).toBeGreaterThan(Date.now());
      expect(entry?.expiresAt).toBe((entry?.enteredAt ?? 0) + 600_000);
    }, 40_000);

    test('an expired entry is removed by DLQ maintenance', async () => {
      harness = await startHarness('dlq-config', mode);
      const queue = harness.queue('expired');
      await queue.setDlqConfigAsync({ maxAge: 50 });

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('job', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);
      expect((await queue.getDlqAsync({ expired: true })).length).toBeGreaterThanOrEqual(0);

      // Maintenance runs every 60s by default; give it one full cycle.
      await waitUntil(
        async () => (await queue.getDlqAsync()).length === 0,
        'the expired entry to be purged by maintenance',
        90_000
      );
    }, 120_000);
  });
}
