/**
 * Executable proof for /guide/dlq/operations/
 * ("DLQ Operations: Filter, Retry, Remove, Purge").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import type { Queue } from '../../src/client';
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

/** Fail `count` jobs on a fresh queue and wait until they are all in the DLQ. */
async function seedDlq(
  active: CoreE2eHarness,
  label: string,
  count: number
): Promise<Queue<unknown>> {
  const queue = active.queue(label);
  active.worker(queue.name, async () => {
    throw new Error('boom');
  });
  for (let i = 0; i < count; i++) {
    await queue.add(`job-${i}`, { i }, { attempts: 1, durable: true });
  }
  await waitUntil(
    async () => (await queue.getDlqAsync()).length === count,
    `${count} DLQ entries`,
    30_000
  );
  return queue as Queue<unknown>;
}

for (const mode of MODES) {
  describe(`dlq guide · operations [${mode}]`, () => {
    test('reason, time, retriable and pagination filters apply in this mode', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'filters', 5);
      const now = Date.now();

      expect(await queue.getDlqAsync({ reason: 'max_attempts_exceeded' })).toHaveLength(5);
      expect(await queue.getDlqAsync({ reason: 'stalled' })).toHaveLength(0);
      expect(await queue.getDlqAsync({ olderThan: now + 60_000 })).toHaveLength(5);
      expect(await queue.getDlqAsync({ olderThan: now - 60_000 })).toHaveLength(0);
      expect(await queue.getDlqAsync({ newerThan: now - 60_000 })).toHaveLength(5);
      expect(await queue.getDlqAsync({ newerThan: now + 60_000 })).toHaveLength(0);
      // autoRetry is off by default, so nothing has a due nextRetryAt.
      expect(await queue.getDlqAsync({ retriable: true })).toHaveLength(0);

      const page = await queue.getDlqAsync({ limit: 2, offset: 1 });
      expect(page).toHaveLength(2);
      const all = await queue.getDlqAsync();
      expect(page.map((entry) => entry.job.id)).toEqual(
        all.slice(1, 3).map((entry) => entry.job.id)
      );
    }, 60_000);

    test('retriable: true means the auto-retry time is already due', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = harness.queue('retriable');
      await queue.setDlqConfigAsync({
        autoRetry: true,
        autoRetryInterval: 3_600_000,
        maxAutoRetries: 3,
      });
      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      await queue.add('job', {}, { attempts: 1, durable: true });
      await waitUntil(
        async () => (await queue.getDlqAsync()).length === 1,
        'one DLQ entry',
        30_000
      );

      const entry = (await queue.getDlqAsync())[0];
      expect(entry.nextRetryAt).toBeGreaterThan(Date.now());
      // Retry budget remains, but the time is not due yet.
      expect(await queue.getDlqAsync({ retriable: true })).toHaveLength(0);
    }, 60_000);

    test('retryDlq() re-queues everything', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'retry-all', 3);

      expect(await queue.retryDlqAsync()).toBe(3);
      await waitUntil(async () => (await queue.getDlqAsync()).length === 0, 'an empty DLQ');
    }, 60_000);

    test('retryDlq(id) re-queues exactly one entry', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'retry-one', 3);
      const target = (await queue.getDlqAsync())[0].job.id;

      expect(await queue.retryDlqAsync(target)).toBe(1);

      await waitUntil(
        async () => !(await queue.getDlqAsync()).some((entry) => entry.job.id === target),
        'only the targeted entry to leave the DLQ'
      );
      expect((await queue.getDlqAsync()).length).toBeLessThanOrEqual(2);
    }, 60_000);

    test('retryDlqByFilterAsync retries the matching subset', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'retry-filter', 3);

      expect(await queue.retryDlqByFilterAsync({ reason: 'stalled' })).toBe(0);
      expect((await queue.getDlqAsync()).length).toBe(3);

      expect(await queue.retryDlqByFilterAsync({ reason: 'max_attempts_exceeded' })).toBe(3);
      await waitUntil(async () => (await queue.getDlqAsync()).length === 0, 'an empty DLQ');
    }, 60_000);

    test('the synchronous retryDlqByFilter applies the filter in both runtimes', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'retry-filter-sync', 2);

      const removed = queue.retryDlqByFilter({ reason: 'max_attempts_exceeded' });

      if (mode === 'embedded') {
        expect(removed).toBe(2);
        expect(queue.getDlq()).toEqual([]);
      } else {
        // Synchronous TCP mutations cannot return the authoritative count, but
        // they still have to enqueue the same fire-and-forget operation as the
        // other DLQ mutation methods.
        expect(removed).toBe(0);
        await waitUntil(
          async () => (await queue.getDlqAsync()).length === 0,
          'the filtered TCP retry to empty the DLQ'
        );
      }
    }, 60_000);

    test('getDlqStatsAsync reports total, byReason and pendingRetry', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'stats', 4);

      const stats = await queue.getDlqStatsAsync();

      expect(stats.total).toBe(4);
      expect(stats.byReason.max_attempts_exceeded).toBe(4);
      expect(stats.pendingRetry).toBe(0);
      expect(stats.oldestEntry).toBeNumber();
      expect(stats.newestEntry).toBeNumber();
    }, 60_000);

    test('removeDlqJob permanently deletes one entry and is idempotent', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'remove-one', 2);
      const target = (await queue.getDlqAsync())[0].job.id;

      expect(await queue.removeDlqJob(target)).toBe(true);
      expect(await queue.removeDlqJobAsync(target)).toBe(false);
      expect(await queue.getJobState(target)).toBe('unknown');
      expect(await queue.getDlqAsync()).toHaveLength(1);
    }, 60_000);

    test('purgeDlqAsync deletes every entry and returns the count', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'purge', 3);

      expect(await queue.purgeDlqAsync()).toBe(3);

      expect(await queue.getDlqAsync()).toEqual([]);
      expect((await queue.getDlqStatsAsync()).total).toBe(0);
    }, 60_000);

    test('the synchronous purgeDlq works as documented for this mode', async () => {
      harness = await startHarness('dlq-operations', mode);
      const queue = await seedDlq(harness, 'purge-sync', 2);

      const purged = queue.purgeDlq();

      if (mode === 'embedded') {
        expect(purged).toBe(2);
        expect(queue.getDlq()).toEqual([]);
      } else {
        expect(purged).toBe(0);
        await waitUntil(
          async () => (await queue.getDlqAsync()).length === 0,
          'the fire-and-forget purge to land'
        );
      }
    }, 60_000);
  });
}
