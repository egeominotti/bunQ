/**
 * Executable proof for /guide/queue/dlq/
 * ("DLQ Operations from the Queue Object").
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
  describe(`dlq guide · from the queue api [${mode}]`, () => {
    test('the documented setDlqConfig snippet is accepted end to end', async () => {
      harness = await startHarness('dlq-queue-api', mode);
      const queue = harness.queue('emails');

      queue.setDlqConfig({
        autoRetry: true,
        autoRetryInterval: 3_600_000,
        maxAutoRetries: 3,
        maxAge: 604_800_000,
        maxEntries: 10_000,
      });

      await waitUntil(
        async () => (await queue.getDlqConfigAsync()).autoRetry === true,
        'the server to apply the config'
      );
      expect(await queue.getDlqConfigAsync()).toMatchObject({
        autoRetry: true,
        autoRetryInterval: 3_600_000,
        maxAutoRetries: 3,
        maxAge: 604_800_000,
        maxEntries: 10_000,
      });
    }, 20_000);

    test('getDlqConfigAsync reads the authoritative server config', async () => {
      harness = await startHarness('dlq-queue-api', mode);
      const queue = harness.queue('emails');

      await queue.setDlqConfigAsync({ maxEntries: 55 });

      expect((await queue.getDlqConfigAsync()).maxEntries).toBe(55);
      if (mode === 'embedded') expect(queue.getDlqConfig().maxEntries).toBe(55);
    }, 20_000);

    test('reason filters work on both the sync and async read paths', async () => {
      harness = await startHarness('dlq-queue-api', mode);
      const queue = harness.queue('emails');
      const stalledQueue = harness.queue('emails-stalled');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      await queue.add('send', {}, { attempts: 1, durable: true });
      await waitUntil(async () => (await queue.getDlqAsync()).length === 1, 'one entry', 20_000);

      expect(await queue.getDlqAsync({ reason: 'max_attempts_exceeded' })).toHaveLength(1);
      expect(await queue.getDlqAsync({ reason: 'stalled' })).toHaveLength(0);
      expect(await stalledQueue.getDlqAsync()).toHaveLength(0);

      if (mode === 'embedded') {
        expect(queue.getDlq({ reason: 'max_attempts_exceeded' })).toHaveLength(1);
        expect(queue.getDlq({ reason: 'stalled' })).toHaveLength(0);
      }
    }, 40_000);

    test('full entry metadata survives the round trip in this mode', async () => {
      harness = await startHarness('dlq-queue-api', mode);
      const queue = harness.queue<{ key: string }>('emails');

      harness.worker(queue.name, async () => {
        throw new Error('detailed failure');
      });
      const job = await queue.add('send', { key: 'value' }, { attempts: 2, backoff: 10 });
      await waitForState(queue, job.id, 'failed', 30_000);

      const entry = (await queue.getDlqAsync())[0];
      expect(entry.job.id).toBe(job.id);
      expect(entry.job.name).toBe('send');
      expect(entry.job.data).toMatchObject({ key: 'value' });
      expect(entry.error).toContain('detailed failure');
      expect(entry.attempts).toHaveLength(2);
      expect(entry.enteredAt).toBeNumber();
      expect(entry.expiresAt).toBeNumber();
      // Every live Job method survives the transport.
      expect(await entry.job.getState()).toBe('failed');
    }, 60_000);

    test('the synchronous mutation forms are fire-and-forget over TCP', async () => {
      harness = await startHarness('dlq-queue-api', mode);
      const queue = harness.queue('emails');
      let runs = 0;
      let shouldFail = true;

      harness.worker(queue.name, async () => {
        runs++;
        if (shouldFail) throw new Error('boom');
        return true;
      });
      const job = await queue.add('send', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      shouldFail = false;
      const returned = queue.retryDlq(job.id);
      expect(returned).toBe(mode === 'embedded' ? 1 : 0);

      await waitUntil(() => runs === 2, 'the retried job to run again', 20_000);
    }, 40_000);

    test('retryDlqByFilterAsync returns the applied count in this mode', async () => {
      harness = await startHarness('dlq-queue-api', mode);
      const queue = harness.queue('emails');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      for (let i = 0; i < 2; i++) await queue.add(`send-${i}`, {}, { attempts: 1, durable: true });
      await waitUntil(async () => (await queue.getDlqAsync()).length === 2, 'two entries', 30_000);

      expect(await queue.retryDlqByFilterAsync({ reason: 'stalled' })).toBe(0);
      expect(await queue.retryDlqByFilterAsync({ reason: 'max_attempts_exceeded' })).toBe(2);
    }, 60_000);
  });
}
