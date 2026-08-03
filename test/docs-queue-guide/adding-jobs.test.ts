/**
 * Executable proof for /guide/queue/adding-jobs/
 * ("Adding Jobs: Priorities, Delays and Bulk").
 *
 * Each documented option is added to a real broker and its documented effect is
 * observed, in embedded and TCP mode.
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
  describe(`queue guide · adding jobs [${mode}]`, () => {
    test('the documented option set is accepted and reflected on the job', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');
      const customId = `custom-id-${crypto.randomUUID()}`;

      const job = await queue.add(
        'job-name',
        { key: 'value' },
        {
          priority: 10,
          delay: 5000,
          attempts: 5,
          backoff: 2000,
          timeout: 30_000,
          jobId: customId,
          removeOnComplete: true,
        }
      );

      expect(job.id).toBe(customId);
      const fetched = await queue.getJob(customId);
      expect(fetched?.opts.priority).toBe(10);
      expect(fetched?.opts.delay).toBe(5000);
      expect(fetched?.opts.attempts).toBe(5);
      expect(fetched?.opts.backoff).toBe(2000);
      expect(fetched?.opts.timeout).toBe(30_000);
      expect(fetched?.opts.removeOnComplete).toBe(true);
      // delay wins over priority for the initial state
      expect(await queue.getJobState(customId)).toBe('delayed');
    });

    test('backoff accepts the object form', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');

      const job = await queue.add(
        'job-name',
        { key: 'value' },
        { backoff: { type: 'exponential', delay: 2000 } }
      );

      expect((await queue.getJob(job.id))?.opts.backoff).toEqual({
        type: 'exponential',
        delay: 2000,
      });
    });

    test('priority > 0 is reported as prioritized and runs first', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue<{ label: string }>('jobs');
      await queue.pauseAsync();

      const low = await queue.add('low', { label: 'low' }, { durable: true });
      const high = await queue.add('high', { label: 'high' }, { priority: 10, durable: true });

      expect(await queue.getJobState(low.id)).not.toBe('prioritized');
      expect(await queue.getJobState(high.id)).toBe('prioritized');

      const order: string[] = [];
      harness.worker<{ label: string }, boolean>(
        queue.name,
        async (job) => {
          order.push(job.data.label);
          return true;
        },
        { concurrency: 1 }
      );
      await queue.resumeAsync();

      await waitUntil(() => order.length === 2, 'both jobs processed');
      expect(order).toEqual(['high', 'low']);
    });

    test('delay holds a job back until it is due', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');

      const job = await queue.add('later', { key: 'value' }, { delay: 250, durable: true });
      expect(await queue.getJobState(job.id)).toBe('delayed');

      harness.worker(queue.name, async () => true);
      await waitForState(queue, job.id, 'completed');
    });

    test('attempts caps how many times a throwing processor runs', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        throw new Error('boom');
      });
      const job = await queue.add(
        'flaky',
        { key: 'value' },
        { attempts: 2, backoff: 10, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 10_000);
      await Bun.sleep(200);
      expect(runs).toBe(2);
    }, 30_000);

    test('timeout fails near the processing deadline with the timeout reason', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');
      let processingStarted = 0;
      let markStarted!: () => void;
      const started = new Promise<void>((resolve) => {
        markStarted = resolve;
      });

      // Never returns: the broker's deadline scheduler owns the outcome, and the
      // processor cannot report a late result after the harness shuts down.
      harness.worker(queue.name, async () => {
        processingStarted = Date.now();
        markStarted();
        return await new Promise<never>(() => undefined);
      });
      const job = await queue.add(
        'slow',
        { key: 'value' },
        { timeout: 120, attempts: 1, durable: true }
      );

      await started;
      await waitForState(queue, job.id, 'failed', 2_000);
      const elapsed = Date.now() - processingStarted;
      expect(elapsed).toBeGreaterThanOrEqual(70);
      expect(elapsed).toBeLessThan(2_000);
      const dlq = await queue.getDlqAsync();
      const entry = dlq.find((candidate) => candidate.job.id === job.id);
      expect(entry?.reason).toBe('timeout');
      expect(entry?.error ?? '').toMatch(/timeout/i);
    }, 15_000);

    test('concurrent processing timeouts fire in deadline order', async () => {
      harness = await startHarness('adding-jobs-timeout-order', mode);
      const queue = harness.queue<{ label: string }>('jobs');
      const jobs = await Promise.all([
        queue.add('long', { label: 'long' }, { timeout: 500, attempts: 1, durable: true }),
        queue.add('short', { label: 'short' }, { timeout: 100, attempts: 1, durable: true }),
        queue.add('middle', { label: 'middle' }, { timeout: 300, attempts: 1, durable: true }),
      ]);
      const labelsById = new Map(jobs.map((job) => [job.id, job.data.label]));
      const started = new Set<string>();
      const failed = new Set<string>();
      const failureOrder: string[] = [];

      harness.worker<{ label: string }, never>(
        queue.name,
        async (job) => {
          started.add(job.id);
          return await new Promise<never>(() => undefined);
        },
        { concurrency: 3, batchSize: 1 }
      );
      await waitUntil(() => started.size === 3, 'all timeout jobs to start', 5_000);
      await waitUntil(
        async () => {
          for (const job of jobs) {
            if (!failed.has(job.id) && (await queue.getJobState(job.id)) === 'failed') {
              failed.add(job.id);
              failureOrder.push(labelsById.get(job.id)!);
            }
          }
          return failed.size === 3;
        },
        'all timeout jobs to fail by deadline',
        2_000
      );

      expect(failureOrder).toEqual(['short', 'middle', 'long']);
      const dlq = await queue.getDlqAsync();
      for (const job of jobs) {
        expect(dlq.find((entry) => entry.job.id === job.id)?.reason).toBe('timeout');
      }
    }, 15_000);

    test('removeOnComplete deletes the job once it finishes', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');

      harness.worker(queue.name, async () => ({ done: true }));
      const job = await queue.add(
        'ephemeral',
        { key: 'value' },
        { removeOnComplete: true, durable: true }
      );

      await waitUntil(async () => (await queue.getJob(job.id)) === null, 'job to be removed');
    });

    test('addBulk inserts every job with its own options', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue<{ id: number }>('jobs');
      await queue.pauseAsync();

      const jobs = await queue.addBulk([
        { name: 'task-1', data: { id: 1 } },
        { name: 'task-2', data: { id: 2 }, opts: { priority: 10 } },
        { name: 'task-3', data: { id: 3 }, opts: { delay: 5000 } },
      ]);

      expect(jobs).toHaveLength(3);
      expect(jobs.map((job) => job.name)).toEqual(['task-1', 'task-2', 'task-3']);
      expect(await queue.getJobState(jobs[1].id)).toBe('prioritized');
      expect(await queue.getJobState(jobs[2].id)).toBe('delayed');
      expect((await queue.getJob(jobs[1].id))?.opts.priority).toBe(10);
      expect((await queue.getJob(jobs[2].id))?.opts.delay).toBe(5000);
    });

    test('durable: true persists the job before add() resolves', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');
      await queue.pauseAsync();

      const buffered = await queue.add('buffered', { key: 'value' });
      const durable = await queue.add('durable', { key: 'value' }, { durable: true });

      // No sleep: only the durable write is guaranteed to be on disk already.
      const persisted = await queue.getJobsAsync({ state: ['waiting', 'paused'], end: -1 });
      expect(persisted.map((job) => job.id)).toContain(durable.id);

      await waitUntil(
        async () =>
          (await queue.getJobsAsync({ state: ['waiting', 'paused'], end: -1 }))
            .map((job) => job.id)
            .includes(buffered.id),
        'the buffered job to reach disk after the write-buffer flush'
      );
    });

    test('repeat accepts every, every+limit and a cron pattern', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');

      const heartbeat = await queue.add('heartbeat', {}, { repeat: { every: 5000 } });
      const daily = await queue.add(
        'daily-report',
        {},
        { repeat: { every: 86_400_000, limit: 30 } }
      );
      const weekly = await queue.add('weekly', {}, { repeat: { pattern: '0 9 * * MON' } });

      for (const job of [heartbeat, daily, weekly]) {
        expect(job.id).toBeString();
        expect(['waiting', 'delayed', 'prioritized', 'active']).toContain(
          await queue.getJobState(job.id)
        );
      }
    });

    test('a repeating job runs again on its interval', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue('jobs');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      await queue.add('heartbeat', {}, { repeat: { every: 150 }, durable: true });

      await waitUntil(() => runs >= 3, 'three repeat executions', 15_000);
    }, 30_000);

    test('updateData changes the payload used by the next scheduled run', async () => {
      harness = await startHarness('adding-jobs', mode);
      const queue = harness.queue<{ endpoint: string }>('jobs');
      const seen: string[] = [];

      harness.worker<{ endpoint: string }, boolean>(queue.name, async (job) => {
        seen.push(job.data.endpoint);
        return true;
      });
      const job = await queue.add(
        'sync',
        { endpoint: '/api/v1' },
        { repeat: { every: 150 }, durable: true }
      );

      await waitUntil(() => seen.length >= 1, 'first repeat execution', 10_000);
      await job.updateData({ endpoint: '/api/v2' });

      await waitUntil(
        () => seen.includes('/api/v2'),
        'the next run to use the updated data',
        15_000
      );
    }, 30_000);
  });
}
