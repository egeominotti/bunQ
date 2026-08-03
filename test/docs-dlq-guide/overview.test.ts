/**
 * Executable proof for /guide/dlq/
 * ("Dead Letter Queue: What Happens to Failed Jobs").
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
  describe(`dlq guide · overview [${mode}]`, () => {
    test('an exhausted job is retained with its error and attempt history', async () => {
      harness = await startHarness('dlq-overview', mode);
      const queue = harness.queue<{ key: string }>('emails');
      let attempt = 0;

      harness.worker(queue.name, async () => {
        attempt++;
        throw new Error(`failure ${attempt}`);
      });
      const job = await queue.add('send', { key: 'value' }, { attempts: 3, backoff: 10 });
      await waitForState(queue, job.id, 'failed', 30_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry).toBeDefined();
      expect(entry?.job.id).toBe(job.id);
      expect(entry?.reason).toBe('max_attempts_exceeded');
      expect(entry?.error).toContain('failure 3');
      // The history keeps the retryable failures that preceded the terminal one.
      expect(entry?.attempts).toHaveLength(3);
      expect(entry?.attempts.map((record) => record.attempt)).toEqual([1, 2, 3]);
      expect(entry?.attempts[0].error).toContain('failure 1');
      expect(entry?.enteredAt).toBeGreaterThan(0);
    }, 60_000);

    test('removeOnFail keeps the job out of the DLQ entirely', async () => {
      harness = await startHarness('dlq-overview', mode);
      const queue = harness.queue('emails');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const kept = await queue.add('kept', {}, { attempts: 1, durable: true });
      const dropped = await queue.add('dropped', {}, { attempts: 1, removeOnFail: true });

      await waitForState(queue, kept.id, 'failed', 20_000);
      await Bun.sleep(400);

      const ids = (await queue.getDlqAsync()).map((entry) => entry.job.id);
      expect(ids).toContain(kept.id);
      expect(ids).not.toContain(dropped.id);
    }, 40_000);

    test('the documented retention defaults are the shipped defaults', async () => {
      harness = await startHarness('dlq-overview', mode);
      const queue = harness.queue('emails');

      const config = await queue.getDlqConfigAsync();

      expect(config.maxAge).toBe(604_800_000); // 7 days
      expect(config.maxEntries).toBe(10_000);
    });

    test('retryDlq puts everything back in the queue', async () => {
      harness = await startHarness('dlq-overview', mode);
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
      expect(runs).toBe(1);

      shouldFail = false;
      queue.retryDlq();

      await waitUntil(() => runs === 2, 'the retried job to run again', 20_000);
      await waitForState(queue, job.id, 'completed', 20_000);
      await waitUntil(async () => (await queue.getDlqAsync()).length === 0, 'an empty DLQ');
    }, 60_000);

    test('obliterate removes DLQ entries too', async () => {
      harness = await startHarness('dlq-overview', mode);
      const queue = harness.queue('emails');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);
      expect((await queue.getDlqAsync()).length).toBe(1);

      await queue.obliterateAsync();

      await waitUntil(
        async () => (await queue.getDlqAsync()).length === 0,
        'the DLQ to be wiped by obliterate'
      );
    }, 40_000);

    test('the sync query API is an embedded snapshot', async () => {
      harness = await startHarness('dlq-overview', mode);
      const queue = harness.queue('emails');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);
      await waitUntil(async () => (await queue.getDlqAsync()).length === 1, 'the DLQ entry');

      if (mode === 'embedded') {
        expect(queue.getDlq().map((entry) => entry.job.id)).toEqual([job.id]);
        expect(queue.getDlqStats().total).toBe(1);
      } else {
        expect(queue.getDlq()).toEqual([]);
        expect(queue.getDlqStats().total).toBe(0);
        // The async twins are authoritative in both runtimes.
        expect((await queue.getDlqAsync()).map((entry) => entry.job.id)).toEqual([job.id]);
        expect((await queue.getDlqStatsAsync()).total).toBe(1);
      }
    }, 40_000);

    test('getDlqJobsAsync is the compact jobs-only view', async () => {
      harness = await startHarness('dlq-overview', mode);
      const queue = harness.queue('emails');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', { key: 'value' }, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const jobs = await queue.getDlqJobsAsync(10);
      expect(jobs.map((entry) => entry.id)).toEqual([job.id]);
    }, 40_000);
  });
}
