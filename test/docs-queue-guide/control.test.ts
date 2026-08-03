/**
 * Executable proof for /guide/queue/control/ ("Queue Control and Maintenance").
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
  describe(`queue guide · control [${mode}]`, () => {
    test('pause and resume stop and restart consumption', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');
      let processed = 0;
      harness.worker(queue.name, async () => {
        processed++;
        return true;
      });

      await queue.pauseAsync();
      expect(await queue.isPausedAsync()).toBe(true);
      const job = await queue.add('while-paused', {}, { durable: true });
      await Bun.sleep(250);
      expect(processed).toBe(0);
      expect((await queue.getJobCountsAsync()).paused).toBe(1);

      await queue.resumeAsync();
      expect(await queue.isPausedAsync()).toBe(false);
      await waitForState(queue, job.id, 'completed');
      expect(processed).toBe(1);
    }, 20_000);

    test('the fire-and-forget pause/resume forms reach the broker', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');

      queue.pause();
      await waitUntil(() => queue.isPausedAsync(), 'the queue to report paused');

      queue.resume();
      await waitUntil(async () => !(await queue.isPausedAsync()), 'the queue to report resumed');
    });

    test('drainAsync removes waiting jobs and returns how many', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');
      await queue.pauseAsync();
      await queue.add('a', {}, { durable: true });
      await queue.add('b', {}, { durable: true });
      await queue.add('c', {}, { durable: true });

      const removed = await queue.drainAsync();

      expect(removed).toBe(3);
      expect(await queue.countAsync()).toBe(0);
    });

    test('drain() without await also empties the queue', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');
      await queue.pauseAsync();
      await queue.add('a', {}, { durable: true });

      queue.drain();

      await waitUntil(async () => (await queue.countAsync()) === 0, 'the drain to land');
    });

    test('obliterateAsync removes all data and ordering is safe afterwards', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');
      await queue.pauseAsync();
      await queue.add('gone', {}, { durable: true });

      await queue.obliterateAsync();
      expect(await queue.countAsync()).toBe(0);

      // Documented ordering guarantee: a job added after the await survives.
      const survivor = await queue.add('survivor', {}, { durable: true });
      await Bun.sleep(100);
      expect(await queue.getJobState(survivor.id)).not.toBe('unknown');
      expect(await queue.countAsync()).toBe(1);
    });

    test('remove and removeAsync delete a single job', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');
      await queue.pauseAsync();
      const first = await queue.add('first', {}, { durable: true });
      const second = await queue.add('second', {}, { durable: true });

      await queue.removeAsync(first.id);
      expect(await queue.getJobState(first.id)).toBe('unknown');

      queue.remove(second.id);
      await waitUntil(
        async () => (await queue.getJobState(second.id)) === 'unknown',
        'the fire-and-forget removal to land'
      );
      expect(await queue.countAsync()).toBe(0);
    });

    test('waitUntilReady resolves and close() is safe', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('control');

      await queue.waitUntilReady();
      const job = await queue.add('after-ready', {}, { durable: true });
      expect(await queue.getJobState(job.id)).toBe('waiting');

      queue.close();
      if (mode === 'embedded') {
        // Documented as a no-op in embedded mode: the queue keeps working.
        const next = await queue.add('after-close', {}, { durable: true });
        expect(await queue.getJobState(next.id)).toBe('waiting');
      }
    });

    test('cleanAsync removes finished jobs older than the grace window', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('maintenance');
      harness.worker(queue.name, async () => ({ ok: true }));
      const ids: string[] = [];
      for (let i = 0; i < 3; i++) {
        ids.push((await queue.add(`done-${i}`, { i }, { durable: true })).id);
      }
      for (const id of ids) await waitForState(queue, id, 'completed', 10_000);

      const removed = await queue.cleanAsync(0, 100, 'completed');

      expect(removed).toBeArray();
      expect(removed).toHaveLength(3);
      expect([...removed].sort()).toEqual([...ids].sort());
      await waitUntil(
        async () => (await queue.getCompletedCount()) === 0,
        'the completed set to be emptied'
      );
    }, 30_000);

    test('cleanAsync respects the limit argument', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('maintenance');
      harness.worker(queue.name, async () => ({ ok: true }));
      for (let i = 0; i < 4; i++) await queue.add(`done-${i}`, { i }, { durable: true });
      await waitUntil(async () => (await queue.getCompletedCount()) === 4, 'four completed jobs');

      expect(await queue.cleanAsync(0, 2, 'completed')).toHaveLength(2);
      expect(await queue.getCompletedCount()).toBe(2);
    }, 30_000);

    test('promoteJobs moves delayed jobs to waiting now', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('maintenance');
      await queue.pauseAsync();
      const delayed = [
        await queue.add('d1', {}, { delay: 600_000, durable: true }),
        await queue.add('d2', {}, { delay: 600_000, durable: true }),
      ];
      expect((await queue.getJobCountsAsync()).delayed).toBe(2);

      const promoted = await queue.promoteJobs({ count: 50 });

      expect(promoted).toBe(2);
      for (const job of delayed) {
        expect(await queue.getJobState(job.id)).not.toBe('delayed');
      }
      expect((await queue.getJobCountsAsync()).delayed).toBe(0);
    });

    test('retryJobs re-queues failed jobs from the DLQ', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('retries');
      let attempts = 0;
      harness.worker(queue.name, async () => {
        attempts++;
        if (attempts <= 2) throw new Error('boom');
        return true;
      });

      const job = await queue.add('flaky', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 15_000);
      expect(await queue.getFailedCount()).toBe(1);

      await queue.retryJobs({ state: 'failed', count: 100 });

      await waitUntil(async () => (await queue.getFailedCount()) === 0, 'the DLQ to be emptied');
    }, 30_000);

    test('retryJobs re-queues completed jobs older than a timestamp', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('retries');
      let runs = 0;
      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });

      const job = await queue.add('done', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      await queue.retryJobs({ state: 'completed', count: 100, timestamp: Date.now() });

      await waitUntil(() => runs >= 2, 'the completed job to run again', 10_000);
    }, 30_000);

    test('retryCompletedAsync re-queues every completed job', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('retries');
      let runs = 0;
      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      for (let i = 0; i < 2; i++) await queue.add(`done-${i}`, { i }, { durable: true });
      await waitUntil(async () => (await queue.getCompletedCount()) === 2, 'two completed jobs');

      expect(await queue.retryCompletedAsync()).toBe(2);

      await waitUntil(() => runs >= 4, 'both jobs to run again', 10_000);
    }, 30_000);

    test('retryCompleted(id) is sync in embedded mode and fire-and-forget over TCP', async () => {
      harness = await startHarness('control', mode);
      const queue = harness.queue('retries');
      let runs = 0;
      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      const job = await queue.add('done', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      const result = queue.retryCompleted(job.id);

      expect(result).toBe(mode === 'embedded' ? 1 : 0);
      await waitUntil(() => runs >= 2, 'the job to be processed again', 10_000);
    }, 30_000);
  });
}
