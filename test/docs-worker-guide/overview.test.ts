/**
 * Executable proof for /guide/worker/ ("Worker: Process Jobs from a Bun Queue").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Queue } from '../../src/client';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  queueEvents,
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
  describe(`worker guide · overview [${mode}]`, () => {
    test('the processor return value becomes the job result', async () => {
      harness = await startHarness('worker-overview', mode);
      const queue = harness.queue('my-queue');
      const events = queueEvents(harness, queue.name);
      await events.waitUntilReady();

      harness.worker(queue.name, async () => ({ success: true }));
      const job = await queue.add('task', { key: 'value' }, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect(await queue.waitJobUntilFinished(job.id, events, 10_000)).toEqual({ success: true });
    }, 20_000);

    // Documented: "the return value is stored as the job's result". The result is
    // stored and readable through waitJobUntilFinished, but the public Job field
    // is never populated by a queue read.
    test('getJob exposes the stored result as returnvalue', async () => {
      harness = await startHarness('worker-overview', mode);
      const queue = harness.queue('my-queue');

      harness.worker(queue.name, async () => ({ success: true }));
      const job = await queue.add('task', { key: 'value' }, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect((await queue.getJob(job.id))?.returnvalue).toEqual({ success: true });
    }, 20_000);

    test('terminal result values survive every public Job read and serialization shape', async () => {
      harness = await startHarness('worker-overview-results', mode);
      const queue = harness.queue<{ value?: unknown }>('result-shapes');
      const values: unknown[] = [{ nested: ['value'] }, null, false, 0, 'done', undefined];

      harness.worker<{ value?: unknown }, unknown>(queue.name, async (job) => job.data.value, {
        concurrency: 3,
      });
      const jobs = [];
      for (const value of values) {
        jobs.push(await queue.add('shape', { value }, { durable: true }));
      }
      for (const job of jobs) await waitForState(queue, job.id, 'completed', 10_000);

      const listed = await queue.getJobsAsync({ state: 'completed', start: 0, end: -1 });
      const byId = new Map(listed.map((job) => [job.id, job]));
      for (let index = 0; index < jobs.length; index++) {
        const expected = values[index];
        const fetched = await queue.getJob(jobs[index].id);
        expect(fetched?.returnvalue).toEqual(expected);
        expect(fetched?.toJSON().returnvalue).toEqual(expected);
        expect(fetched?.asJSON().returnvalue).toBe(
          expected === undefined ? undefined : JSON.stringify(expected)
        );
        expect(byId.get(jobs[index].id)?.returnvalue).toEqual(expected);
        expect(byId.get(jobs[index].id)?.toJSON().returnvalue).toEqual(expected);
      }
    }, 30_000);

    test('a persisted result and failure reason remain authoritative after broker restart', async () => {
      harness = await startHarness('worker-overview-restart', mode);
      const completedQueue = harness.queue('restart-result');
      const failedQueue = harness.queue('restart-failure');
      const completedWorker = harness.worker(completedQueue.name, async () => ({ persisted: 42 }));
      const failedWorker = harness.worker(failedQueue.name, async () => {
        throw new Error('persisted terminal failure');
      });
      failedWorker.on('failed', () => undefined);

      const completed = await completedQueue.add('complete', {}, { durable: true });
      const failed = await failedQueue.add('fail', {}, { attempts: 1, durable: true });
      await waitForState(completedQueue, completed.id, 'completed', 10_000);
      await waitForState(failedQueue, failed.id, 'failed', 15_000);
      await completedWorker.close();
      await failedWorker.close();
      await completedQueue.close();
      await failedQueue.close();

      await harness.restartBroker();
      const recoveredCompleted = new Queue(completedQueue.name, harness.queueOptions());
      const recoveredFailed = new Queue(failedQueue.name, harness.queueOptions());
      harness.addCleanup(() => recoveredCompleted.close());
      harness.addCleanup(() => recoveredFailed.close());

      expect((await recoveredCompleted.getJob(completed.id))?.returnvalue).toEqual({
        persisted: 42,
      });
      expect((await recoveredFailed.getJob(failed.id))?.failedReason).toContain(
        'persisted terminal failure'
      );
    }, 40_000);

    test('getJob exposes the terminal failure as failedReason', async () => {
      harness = await startHarness('worker-overview', mode);
      const queue = harness.queue('my-queue');

      harness.worker(queue.name, async () => {
        throw new Error('documented terminal reason');
      });
      const job = await queue.add('task', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 15_000);

      expect((await queue.getJob(job.id))?.failedReason).toContain('documented terminal reason');
      expect((await queue.getJob(job.id))?.toJSON().failedReason).toContain(
        'documented terminal reason'
      );
      expect((await queue.getDlqAsync())[0].job.failedReason).toContain(
        'documented terminal reason'
      );
    }, 25_000);

    test('the worker starts polling on its own (autorun)', async () => {
      harness = await startHarness('worker-overview', mode);
      const queue = harness.queue('my-queue');

      // No run() call: the job is consumed because autorun defaults to true.
      const worker = harness.worker(queue.name, async () => true);
      expect(worker.isRunning()).toBe(true);

      const job = await queue.add('task', {}, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);
    }, 20_000);

    test('a throwing processor is retried up to attempts, then lands in the DLQ', async () => {
      harness = await startHarness('worker-overview', mode);
      const queue = harness.queue('my-queue');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        throw new Error('always fails');
      });
      // attempts defaults to 3
      const job = await queue.add('doomed', {}, { backoff: 10, durable: true });

      await waitForState(queue, job.id, 'failed', 20_000);
      await waitUntil(() => runs === 3, `three attempts (saw ${runs})`, 10_000);
      expect((await queue.getDlqAsync()).map((entry) => entry.job.id)).toEqual([job.id]);
    }, 40_000);

    test('typed workers receive the queue payload type', async () => {
      harness = await startHarness('worker-overview', mode);
      const queue = harness.queue<{ userId: number }>('typed');
      const seen: number[] = [];

      harness.worker<{ userId: number }, boolean>(queue.name, async (job) => {
        seen.push(job.data.userId);
        return true;
      });
      const job = await queue.add('task', { userId: 7 }, { durable: true });
      await waitForState(queue, job.id, 'completed', 10_000);

      expect(seen).toEqual([7]);
    }, 20_000);
  });
}
