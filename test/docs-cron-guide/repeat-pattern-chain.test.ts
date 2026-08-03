/** Regression coverage for Queue.add({ repeat: { pattern } }) successors. */

import { afterEach, describe, expect, test } from 'bun:test';
import type { Job, JobOptions, Queue } from '../../src/client';
import { Queue as QueueClient } from '../../src/client';
import { jobId } from '../../src/domain/types/job';
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

interface HeldExecution<T> {
  original: Job<T>;
  queue: Queue<T>;
  releasedAt: number;
  successor: Job<T>;
}

async function completeIntoPausedSuccessor<T>(
  active: CoreE2eHarness,
  label: string,
  data: T,
  options: JobOptions,
  beforeRelease?: () => Promise<void>
): Promise<HeldExecution<T>> {
  const queue = active.queue<T>(label);
  let release = (): void => undefined;
  let markStarted = (): void => undefined;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  active.worker(queue.name, async () => {
    markStarted();
    await held;
    return true;
  });

  const original = await queue.add('pattern-job', data, options);
  await started;
  await queue.pauseAsync();
  await beforeRelease?.();
  const releasedAt = Date.now();
  release();
  await waitForState(queue, original.id, 'completed', 20_000);

  let successor: Job<T> | undefined;
  await waitUntil(
    async () => {
      const pending = await queue.getJobsAsync({
        state: ['waiting', 'prioritized', 'delayed', 'paused'],
        end: -1,
      });
      successor = pending.find((job) => job.id !== original.id);
      return successor !== undefined;
    },
    'the paused repeat successor',
    20_000
  );
  if (!successor) throw new Error('repeat successor was not created');
  return { original, queue, releasedAt, successor };
}

async function waitForMillisecondWindow(minimum: number, maximum: number): Promise<void> {
  await waitUntil(
    () => {
      const fraction = Date.now() % 1_000;
      return fraction >= minimum && fraction <= maximum;
    },
    `the clock fraction to enter ${minimum}-${maximum}ms`,
    5_000
  );
}

for (const mode of MODES) {
  describe(`cron guide · Queue.add pattern chain [${mode}]`, () => {
    test('uses the cron deadline and preserves repeat and job policies', async () => {
      harness = await startHarness('repeat-pattern-chain', mode);
      const startDate = Date.now() + 2_000;
      const endDate = startDate + 10_000;
      const dedupId = `repeat-dedup-${crypto.randomUUID()}`;
      const options: JobOptions = {
        priority: 7,
        attempts: 5,
        backoff: { type: 'exponential', delay: 321 },
        timeout: 4_000,
        removeOnComplete: false,
        removeOnFail: true,
        stallTimeout: 2_500,
        lifo: true,
        stackTraceLimit: 4,
        keepLogs: 6,
        sizeLimit: 2_048,
        failParentOnFailure: true,
        removeDependencyOnFailure: true,
        continueParentOnFailure: true,
        ignoreDependencyOnFailure: true,
        durable: true,
        deduplication: { id: dedupId, ttl: 30_000, extend: true, replace: true },
        debounce: { id: `debounce-${crypto.randomUUID()}`, ttl: 30_000 },
        repeat: {
          pattern: '* * * * * *',
          limit: 2,
          startDate,
          endDate,
          tz: 'UTC',
          immediately: true,
          offset: 75,
          jobId: 'pattern-chain',
        },
      };
      const fixture = await completeIntoPausedSuccessor(harness, 'policy', { version: 1 }, options);
      const scheduledAt = fixture.successor.timestamp + fixture.successor.delay;

      expect(scheduledAt).toBeGreaterThanOrEqual(startDate);
      expect(scheduledAt).toBeLessThan(startDate + 1_500);
      expect(fixture.successor.opts.repeat).toMatchObject({
        pattern: '* * * * * *',
        limit: 2,
        startDate,
        endDate,
        tz: 'UTC',
        immediately: true,
        offset: 75,
        jobId: 'pattern-chain',
        count: 1,
      });

      const internal = await harness.brokerManager().getJob(jobId(fixture.successor.id));
      expect(internal).toMatchObject({
        priority: 7,
        maxAttempts: 5,
        backoffConfig: { type: 'exponential', delay: 321 },
        timeout: 4_000,
        removeOnComplete: false,
        removeOnFail: true,
        stallTimeout: 2_500,
        lifo: true,
        stackTraceLimit: 4,
        keepLogs: 6,
        sizeLimit: 2_048,
        failParentOnFailure: true,
        removeDependencyOnFailure: true,
        continueParentOnFailure: true,
        ignoreDependencyOnFailure: true,
        uniqueKey: dedupId,
        deduplicationTtl: 30_000,
        deduplicationExtend: true,
        deduplicationReplace: true,
      });

      await fixture.original.updateData({ version: 2 });
      await waitUntil(
        async () => (await fixture.queue.getJob(fixture.successor.id))?.data.version === 2,
        'updated data to reach the pattern successor'
      );
    }, 40_000);

    test('applies a positive offset to the current cron tick when its deadline is future', async () => {
      harness = await startHarness('repeat-pattern-positive-offset', mode);
      const positive = await completeIntoPausedSuccessor(
        harness,
        'positive',
        {},
        {
          repeat: { pattern: '* * * * * *', limit: 1, tz: 'UTC', offset: 700 },
          durable: true,
        },
        () => waitForMillisecondWindow(80, 160)
      );
      const positiveDeadline = positive.successor.timestamp + positive.successor.delay;
      const positiveSecond = Math.floor(positive.releasedAt / 1_000) * 1_000;

      expect(positiveDeadline).toBeGreaterThan(positive.releasedAt);
      expect(positiveDeadline).toBeLessThan(positiveSecond + 1_200);
    }, 30_000);

    test('advances a negatively shifted cron tick instead of scheduling it in the past', async () => {
      harness = await startHarness('repeat-pattern-negative-offset', mode);
      const negative = await completeIntoPausedSuccessor(
        harness,
        'negative',
        {},
        {
          repeat: { pattern: '* * * * * *', limit: 1, tz: 'UTC', offset: -700 },
          durable: true,
        },
        () => waitForMillisecondWindow(550, 650)
      );
      const negativeDeadline = negative.successor.timestamp + negative.successor.delay;

      expect(negativeDeadline - negative.releasedAt).toBeGreaterThan(400);
      expect(negativeDeadline - negative.releasedAt).toBeLessThan(1_200);
    }, 30_000);

    test('keeps an immediate interval successor on cadence after a negative offset', async () => {
      harness = await startHarness('repeat-interval-negative-offset', mode);
      const interval = await completeIntoPausedSuccessor(
        harness,
        'interval',
        {},
        {
          repeat: { every: 800, limit: 1, immediately: true, offset: -1_200 },
          durable: true,
        }
      );
      const deadline = interval.successor.timestamp + interval.successor.delay;

      expect(deadline - interval.releasedAt).toBeGreaterThan(250);
      expect(deadline - interval.releasedAt).toBeLessThan(800);
    }, 30_000);

    test('survives restart, keeps cron cadence, and stops at the limit', async () => {
      harness = await startHarness('repeat-pattern-restart', mode);
      const queue = harness.queue<{ generation: number }>('restart');
      const queueName = queue.name;
      let release = (): void => undefined;
      let started = false;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const firstWorker = harness.worker(queueName, async () => {
        started = true;
        await held;
        return true;
      });
      const original = await queue.add(
        'pattern-job',
        { generation: 1 },
        {
          repeat: { pattern: '* * * * * *', limit: 2, tz: 'UTC', immediately: true },
          durable: true,
        }
      );
      await waitUntil(() => started, 'the initial generation to start');
      await queue.pauseAsync();
      release();
      await waitForState(queue, original.id, 'completed', 20_000);
      await waitUntil(
        async () => (await queue.getDelayedAsync(0, -1)).length === 1,
        'the durable cron successor'
      );
      await firstWorker.close();
      await queue.close();

      await harness.restartBroker();
      const recoveredQueue = new QueueClient<{ generation: number }>(
        queueName,
        harness.queueOptions()
      );
      harness.addCleanup(() => recoveredQueue.close());
      const starts: number[] = [];
      harness.worker(queueName, async () => {
        starts.push(Date.now());
        return true;
      });
      await recoveredQueue.resumeAsync();

      await waitUntil(() => starts.length === 2, 'both recovered successors', 20_000);
      expect(starts[1] - starts[0]).toBeGreaterThan(400);
      await Bun.sleep(1_300);
      expect(starts).toHaveLength(2);
    }, 50_000);

    test('does not create a successor beyond endDate', async () => {
      harness = await startHarness('repeat-pattern-end', mode);
      const queue = harness.queue('end-date');
      let runs = 0;
      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      const original = await queue.add(
        'pattern-job',
        {},
        {
          repeat: {
            pattern: '* * * * * *',
            endDate: Date.now() - 1,
            limit: 5,
            tz: 'UTC',
          },
          durable: true,
        }
      );

      await waitForState(queue, original.id, 'completed', 20_000);
      await Bun.sleep(500);
      expect(runs).toBe(1);
    }, 30_000);

    test('rejects invalid or dependency-bound repeat definitions atomically', async () => {
      harness = await startHarness('repeat-pattern-validation', mode);
      const queue = harness.queue('validation');

      await expect(
        queue.add('invalid', {}, { repeat: { pattern: 'not a cron expression' } })
      ).rejects.toThrow(/Invalid repeat pattern/);
      await expect(
        queue.add('custom-id', {}, { jobId: 'fixed', repeat: { every: 1_000 } })
      ).rejects.toThrow(/repeat jobs cannot use JobOptions\.jobId/);
      await expect(
        queue.add(
          'child',
          {},
          {
            parent: { id: 'parent', queue: 'parents' },
            repeat: { every: 1_000 },
          }
        )
      ).rejects.toThrow(/cannot participate in parent or dependency relationships/);
      expect(await queue.countAsync()).toBe(0);
    }, 20_000);
  });
}
