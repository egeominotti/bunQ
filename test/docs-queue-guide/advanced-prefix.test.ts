/**
 * Executable proof for the `prefixKey` half of /guide/queue/advanced/
 * ("Namespaces, Auto-Batching and Store-and-Forward").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Queue, Worker } from '../../src/client';
import type { Job, QueueOptions, WorkerOptions } from '../../src/client';
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

/** Build a queue with an exact broker name (the harness normally uniquifies it). */
function namedQueue<T = unknown>(
  active: CoreE2eHarness,
  name: string,
  extra: QueueOptions = {}
): Queue<T> {
  const queue = new Queue<T>(name, active.queueOptions(extra));
  active.addCleanup(() => queue.close());
  return queue;
}

function namedWorker<T = unknown, R = unknown>(
  active: CoreE2eHarness,
  name: string,
  processor: (job: Job<T>) => R | Promise<R>,
  extra: WorkerOptions = {}
): Worker<T, R> {
  const worker = new Worker<T, R>(name, processor, active.workerOptions(extra));
  active.addCleanup(() => worker.close(true));
  return worker;
}

for (const mode of MODES) {
  describe(`queue guide · namespaces [${mode}]`, () => {
    test('two prefixes share a broker without seeing each other', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `emails-${crypto.randomUUID().slice(0, 8)}`;
      const devQueue = namedQueue(harness, name, { prefixKey: 'dev:' });
      const prodQueue = namedQueue(harness, name, { prefixKey: 'prod:' });

      // Queue.name keeps reporting the logical name.
      expect(devQueue.name).toBe(prodQueue.name);

      await devQueue.add('send', { to: 'tester@example.com' }, { durable: true });

      expect(await prodQueue.getJobCountsAsync()).toMatchObject({ waiting: 0 });
      expect(await devQueue.getJobCountsAsync()).toMatchObject({ waiting: 1 });
      expect(await prodQueue.countAsync()).toBe(0);
      expect(await devQueue.countAsync()).toBe(1);
    });

    test('a worker needs the same prefixKey and sees the prefixed queueName', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `emails-${crypto.randomUUID().slice(0, 8)}`;
      const devQueue = namedQueue(harness, name, { prefixKey: 'dev:' });

      const wrongPrefix: string[] = [];
      namedWorker(
        harness,
        name,
        async (job) => {
          wrongPrefix.push(job.queueName);
          return true;
        },
        { prefixKey: 'prod:' }
      );

      const job = await devQueue.add('send', { to: 'dev@example.com' }, { durable: true });
      await Bun.sleep(250);
      expect(wrongPrefix).toEqual([]);
      expect(await devQueue.getJobState(job.id)).toBe('waiting');

      const queueNames: string[] = [];
      namedWorker(
        harness,
        name,
        async (processed) => {
          queueNames.push(processed.queueName);
          return true;
        },
        { prefixKey: 'dev:' }
      );

      await waitForState(devQueue, job.id, 'completed', 10_000);
      expect(queueNames).toEqual([`dev:${name}`]);
    }, 20_000);

    test('pause, drain and obliterate stay inside one prefix', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `tasks-${crypto.randomUUID().slice(0, 8)}`;
      const devQueue = namedQueue(harness, name, { prefixKey: 'dev:' });
      const prodQueue = namedQueue(harness, name, { prefixKey: 'prod:' });
      await devQueue.add('a', {}, { durable: true });
      await prodQueue.add('b', {}, { durable: true });

      await devQueue.pauseAsync();
      expect(await devQueue.isPausedAsync()).toBe(true);
      expect(await prodQueue.isPausedAsync()).toBe(false);

      expect(await devQueue.drainAsync()).toBe(1);
      expect(await prodQueue.countAsync()).toBe(1);

      await prodQueue.add('c', {}, { durable: true });
      await devQueue.add('d', {}, { durable: true });
      await prodQueue.obliterateAsync();
      expect(await prodQueue.countAsync()).toBe(0);
      expect(await devQueue.countAsync()).toBe(1);
    });

    test('rate limits and concurrency caps are per prefix', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `limited-${crypto.randomUUID().slice(0, 8)}`;
      const devQueue = namedQueue(harness, name, { prefixKey: 'dev:' });
      const prodQueue = namedQueue(harness, name, { prefixKey: 'prod:' });

      await devQueue.setGlobalRateLimitAsync(5, 30_000);
      await devQueue.setGlobalConcurrencyAsync(2);

      expect(await devQueue.getGlobalRateLimit()).toEqual({ max: 5, duration: 30_000 });
      expect(await prodQueue.getGlobalRateLimit()).toBeNull();
      expect(await devQueue.getGlobalConcurrency()).toBe(2);
      expect(await prodQueue.getGlobalConcurrency()).toBeNull();
    });

    test('the DLQ is per prefix', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `failing-${crypto.randomUUID().slice(0, 8)}`;
      const devQueue = namedQueue(harness, name, { prefixKey: 'dev:' });
      const prodQueue = namedQueue(harness, name, { prefixKey: 'prod:' });
      namedWorker(
        harness,
        name,
        async () => {
          throw new Error('boom');
        },
        { prefixKey: 'dev:' }
      );

      const job = await devQueue.add('doomed', {}, { attempts: 1, durable: true });
      await waitForState(devQueue, job.id, 'failed', 15_000);

      expect((await devQueue.getDlqAsync()).map((entry) => entry.job.id)).toEqual([job.id]);
      expect(await prodQueue.getDlqAsync()).toEqual([]);
      expect(await prodQueue.getFailedCount()).toBe(0);
    }, 30_000);

    test('two prefixes can reuse the same schedulerId', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `scheduled-${crypto.randomUUID().slice(0, 8)}`;
      const devQueue = namedQueue(harness, name, { prefixKey: 'dev:' });
      const prodQueue = namedQueue(harness, name, { prefixKey: 'prod:' });
      const schedulerId = 'nightly';

      expect(
        await devQueue.upsertJobScheduler(
          schedulerId,
          { every: 3_600_000 },
          {
            name: 'dev-run',
            data: { env: 'dev' },
          }
        )
      ).not.toBeNull();
      expect(
        await prodQueue.upsertJobScheduler(
          schedulerId,
          { every: 3_600_000 },
          {
            name: 'prod-run',
            data: { env: 'prod' },
          }
        )
      ).not.toBeNull();

      // SchedulerInfo.id is the scheduler identity; name is the job template name.
      expect((await devQueue.getJobScheduler(schedulerId))?.id).toBe(schedulerId);
      expect((await prodQueue.getJobScheduler(schedulerId))?.id).toBe(schedulerId);
      expect((await devQueue.getJobScheduler(schedulerId))?.name).toBe('dev-run');
      expect((await prodQueue.getJobScheduler(schedulerId))?.name).toBe('prod-run');
      expect(await devQueue.getJobSchedulersCount()).toBe(1);
      expect(await prodQueue.getJobSchedulersCount()).toBe(1);

      await devQueue.removeJobScheduler(schedulerId);
      await waitUntil(
        async () => (await devQueue.getJobSchedulersCount()) === 0,
        'the dev scheduler to be removed'
      );
      expect(await prodQueue.getJobSchedulersCount()).toBe(1);
    }, 20_000);

    test('a queue without prefixKey is unaffected', async () => {
      harness = await startHarness('advanced-prefix', mode);
      const name = `plain-${crypto.randomUUID().slice(0, 8)}`;
      const plain = namedQueue(harness, name);
      const prefixed = namedQueue(harness, name, { prefixKey: 'dev:' });

      const job = await plain.add('send', {}, { durable: true });

      expect(await plain.countAsync()).toBe(1);
      expect(await prefixed.countAsync()).toBe(0);

      const seen: string[] = [];
      namedWorker(harness, name, async (processed) => {
        seen.push(processed.queueName);
        return true;
      });
      await waitForState(plain, job.id, 'completed', 10_000);
      expect(seen).toEqual([name]);
    }, 20_000);
  });
}
