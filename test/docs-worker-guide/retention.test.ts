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
  describe(`worker retention precedence [${mode}]`, () => {
    test('worker removal is transition-scoped across retries and workers', async () => {
      harness = await startHarness('worker-retention', mode);
      const queue = harness.queue('worker-scope');
      let firstRuns = 0;
      const firstWorker = harness.worker(
        queue.name,
        async () => {
          firstRuns++;
          throw new Error('first worker failure');
        },
        { removeOnFail: true }
      );
      firstWorker.on('failed', () => undefined);

      const job = await queue.add(
        'kept-by-second-worker',
        {},
        {
          attempts: 2,
          backoff: 100,
          durable: true,
        }
      );
      await waitUntil(() => firstRuns === 1, 'the first worker attempt', 10_000);
      await firstWorker.close();

      const secondWorker = harness.worker(queue.name, async () => {
        throw new Error('second worker terminal failure');
      });
      secondWorker.on('failed', () => undefined);
      await waitForState(queue, job.id, 'failed', 20_000);

      expect(await queue.getJob(job.id)).not.toBeNull();
      expect((await queue.getDlqAsync()).map((entry) => entry.job.id)).toContain(job.id);
    }, 40_000);

    test('job removal wins when worker defaults keep completed and failed jobs', async () => {
      harness = await startHarness('worker-retention', mode);
      const completedQueue = harness.queue('job-complete-policy');
      const failedQueue = harness.queue('job-fail-policy');
      harness.worker(completedQueue.name, async () => true, { removeOnComplete: false });
      const failedWorker = harness.worker(
        failedQueue.name,
        async () => {
          throw new Error('terminal');
        },
        { removeOnFail: false }
      );
      failedWorker.on('failed', () => undefined);

      const completed = await completedQueue.add(
        'complete',
        {},
        {
          removeOnComplete: true,
          durable: true,
        }
      );
      const failed = await failedQueue.add(
        'fail',
        {},
        {
          attempts: 1,
          removeOnFail: true,
          durable: true,
        }
      );
      await waitUntil(
        async () =>
          (await completedQueue.getJob(completed.id)) === null &&
          (await failedQueue.getJob(failed.id)) === null,
        'both job-level retention policies',
        15_000
      );
    }, 30_000);

    test('worker true removes jobs whose explicit job flags are false', async () => {
      harness = await startHarness('worker-retention', mode);
      const completedQueue = harness.queue('worker-complete-policy');
      const failedQueue = harness.queue('worker-fail-policy');
      harness.worker(completedQueue.name, async () => true, { removeOnComplete: true });
      const failedWorker = harness.worker(
        failedQueue.name,
        async () => {
          throw new Error('terminal');
        },
        { removeOnFail: true }
      );
      failedWorker.on('failed', () => undefined);

      const completed = await completedQueue.add(
        'complete',
        {},
        {
          removeOnComplete: false,
          durable: true,
        }
      );
      const failed = await failedQueue.add(
        'fail',
        {},
        {
          attempts: 1,
          removeOnFail: false,
          durable: true,
        }
      );
      await waitUntil(
        async () =>
          (await completedQueue.getJob(completed.id)) === null &&
          (await failedQueue.getJob(failed.id)) === null,
        'both worker-level retention policies',
        15_000
      );
    }, 30_000);

    test('addBulk forwards each job retention policy', async () => {
      harness = await startHarness('worker-retention', mode);
      const queue = harness.queue('bulk-policies');
      const worker = harness.worker(queue.name, async (job) => {
        if (job.name === 'fail') throw new Error('bulk failure');
        return true;
      });
      worker.on('failed', () => undefined);

      const [completed, failed] = await queue.addBulk([
        { name: 'complete', data: {}, opts: { removeOnComplete: true, durable: true } },
        {
          name: 'fail',
          data: {},
          opts: { attempts: 1, removeOnFail: true, durable: true },
        },
      ]);
      await waitUntil(
        async () =>
          (await queue.getJob(completed.id)) === null && (await queue.getJob(failed.id)) === null,
        'both bulk retention policies',
        15_000
      );
    }, 30_000);

    test('durable job retention policies survive a broker restart', async () => {
      harness = await startHarness('worker-retention', mode);
      const queue = harness.queue('restart-policies');
      const [completed, failed] = await queue.addBulk([
        { name: 'complete', data: {}, opts: { removeOnComplete: true, durable: true } },
        {
          name: 'fail',
          data: {},
          opts: { attempts: 1, removeOnFail: true, durable: true },
        },
      ]);
      await queue.close();
      await harness.restartBroker();

      const recovered = new Queue(queue.name, harness.queueOptions());
      harness.addCleanup(() => recovered.close());
      const worker = harness.worker(queue.name, async (job) => {
        if (job.name === 'fail') throw new Error('failure after restart');
        return true;
      });
      worker.on('failed', () => undefined);

      await waitUntil(
        async () =>
          (await recovered.getJob(completed.id)) === null &&
          (await recovered.getJob(failed.id)) === null,
        'recovered retention policies',
        20_000
      );
    }, 40_000);
  });
}
