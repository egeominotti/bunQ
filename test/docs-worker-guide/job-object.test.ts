/**
 * Executable proof for /guide/worker/job-object/
 * ("The Job Object Inside a Worker Processor").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import type { Job } from '../../src/client';
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
  describe(`worker guide · the job object [${mode}]`, () => {
    test('the processor receives id, name, data, attemptsMade and timestamp', async () => {
      harness = await startHarness('worker-job-object', mode);
      const queue = harness.queue<{ key: string }>('jobs');
      const before = Date.now();
      let seen: Job<{ key: string }> | null = null;

      harness.worker<{ key: string }, boolean>(queue.name, async (job) => {
        seen = job;
        return true;
      });
      const added = await queue.add('my-task', { key: 'value' }, { durable: true });
      await waitForState(queue, added.id, 'completed', 10_000);

      const job = seen as unknown as Job<{ key: string }>;
      expect(job).not.toBeNull();
      expect(job.id).toBe(added.id);
      expect(job.name).toBe('my-task');
      expect(job.data).toEqual({ key: 'value' });
      expect(job.attemptsMade).toBe(0);
      expect(job.timestamp).toBeGreaterThanOrEqual(before);
      expect(job.queueName).toBe(queue.name);
    }, 20_000);

    test('attemptsMade counts the attempts already made', async () => {
      harness = await startHarness('worker-job-object', mode);
      const queue = harness.queue('retried');
      const attempts: number[] = [];

      harness.worker(queue.name, async (job) => {
        attempts.push(job.attemptsMade);
        if (attempts.length < 3) throw new Error('retry me');
        return true;
      });
      const job = await queue.add('flaky', {}, { attempts: 3, backoff: 10, durable: true });
      await waitForState(queue, job.id, 'completed', 20_000);

      expect(attempts).toEqual([0, 1, 2]);
    }, 30_000);

    test('job.updateProgress reports progress with a message', async () => {
      harness = await startHarness('worker-job-object', mode);
      const queue = harness.queue('progress');
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async (job) => {
        await job.updateProgress(50, 'Halfway done');
        await held;
        return true;
      });
      const job = await queue.add('long', {}, { durable: true });

      await waitUntil(
        async () => (await queue.getJob(job.id))?.progress === 50,
        'the broker to report 50% progress',
        10_000
      );
      release();
      await waitForState(queue, job.id, 'completed', 10_000);
    }, 20_000);

    test('job.log attaches a log line readable from the queue', async () => {
      harness = await startHarness('worker-job-object', mode);
      const queue = harness.queue('logging');
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async (job) => {
        await job.log('Processing step 1');
        await held;
        return true;
      });
      const job = await queue.add('long', {}, { durable: true });

      await waitUntil(
        async () => (await queue.getJobLogs(job.id, 0, 100)).count === 1,
        'the log line to be stored',
        10_000
      );
      const { logs } = await queue.getJobLogs(job.id, 0, 100);
      expect(logs[0]).toContain('Processing step 1');
      release();
      await waitForState(queue, job.id, 'completed', 10_000);
    }, 20_000);

    test('the job exposes its state helpers and children values', async () => {
      harness = await startHarness('worker-job-object', mode);
      const queue = harness.queue<{ value?: number }>('flows');
      const flow = harness.flow();
      let parentChildren: Record<string, unknown> = {};
      let parentActive = false;

      harness.worker<{ value?: number }, unknown>(queue.name, async (job) => {
        if (job.data.value === undefined) {
          parentActive = await job.isActive();
          parentChildren = await job.getChildrenValues();
          return { parent: true };
        }
        return { doubled: job.data.value * 2 };
      });

      const tree = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [{ name: 'child', queueName: queue.name, data: { value: 21 } }],
      });
      await waitForState(queue, tree.job.id, 'completed', 20_000);

      expect(parentActive).toBe(true);
      expect(Object.values(parentChildren)).toEqual([{ doubled: 42 }]);
    }, 30_000);

    test('manual acquisition exposes clean fields and reuses the broker lease', async () => {
      harness = await startHarness('worker-job-object-manual', mode);
      const queue = harness.queue<{ value: number }>('manual');
      let processorToken: string | undefined;
      const worker = harness.worker<{ value: number }, number>(
        queue.name,
        async (job) => {
          processorToken = job.token;
          return job.data.value * 2;
        },
        { autorun: false }
      );
      await worker.waitUntilReady();

      const added = await queue.add('calculate', { value: 21 }, { durable: true });
      const job = await worker.getNextJob();
      expect(job?.id).toBe(added.id);
      expect(job?.name).toBe('calculate');
      expect(job?.data).toEqual({ value: 21 });
      expect(job?.token).toBeString();
      if (!job) throw new Error('Expected a manually pulled job');

      await worker.processJobManually(job);
      await waitForState(queue, added.id, 'completed', 10_000);
      expect(processorToken).toBe(job.token);
      expect((await queue.getJob(added.id))?.returnvalue).toBe(42);
    }, 20_000);
  });
}
