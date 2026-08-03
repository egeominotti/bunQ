/**
 * Executable proof for /guide/queue/progress/
 * ("Progress, Job Logs and Dependencies").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { QueueEvents } from '../../src/client';
import type { ConnectionOptions } from '../../src/client';
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

type AnyQueueEvents = QueueEvents<unknown, unknown>;
type QueueEventsCtor = new (
  name: string,
  options: { embedded?: boolean; connection?: ConnectionOptions }
) => AnyQueueEvents;

interface ActiveJobQueue {
  name: string;
  add(
    name: string,
    data: unknown,
    opts?: { durable?: boolean; attempts?: number }
  ): Promise<{ id: string }>;
}

/**
 * Hold one job in the active state (as a worker with its lock token does) and
 * run the documented queue-level transition against it.
 */
async function withActiveJob(
  activeHarness: CoreE2eHarness,
  queue: ActiveJobQueue,
  run: (job: { id: string; token: string }) => Promise<void>,
  addOpts: { durable?: boolean; attempts?: number } = { durable: true }
): Promise<void> {
  let resolveActive: (value: { id: string; token: string }) => void = () => undefined;
  const active = new Promise<{ id: string; token: string }>((resolve) => {
    resolveActive = resolve;
  });
  let release = (): void => undefined;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });

  const worker = activeHarness.worker(queue.name, async (job) => {
    resolveActive({ id: job.id, token: job.token ?? '' });
    await held;
    return true;
  });
  // The transition under test takes the job away from the worker; its own
  // acknowledgement afterwards is expected to be rejected.
  worker.on('error', () => undefined);
  worker.on('failed', () => undefined);

  await queue.add('manual', {}, addOpts);
  const info = await active;
  try {
    await run(info);
  } finally {
    release();
    // Let the worker settle its own (now rejected) acknowledgement before the
    // harness tears the connection down.
    await worker.close(true).catch(() => undefined);
    await Bun.sleep(50);
  }
}

for (const mode of MODES) {
  describe(`queue guide · progress, logs and dependencies [${mode}]`, () => {
    // Progress belongs to the job while it is running: the broker only accepts
    // it for a job in the active (processing) state.
    test('updateJobProgress publishes progress for the running job', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue('progress');

      await withActiveJob(harness, queue, async (job) => {
        await queue.updateJobProgress(job.id, 75);
        await waitUntil(
          async () => (await queue.getJob(job.id))?.progress === 75,
          'the job to report 75% progress'
        );
      });
    }, 20_000);

    test('addJobLog and getJobLogs round-trip a log line', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue('progress');
      await queue.pauseAsync();
      const job = await queue.add('long', {}, { durable: true });

      await queue.addJobLog(job.id, 'Processing step 3 completed');
      const { logs, count } = await queue.getJobLogs(job.id, 0, 100);

      expect(count).toBe(1);
      expect(logs).toHaveLength(1);
      expect(logs[0]).toContain('Processing step 3 completed');
    });

    test('getChildrenValues returns the completed child results', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue<{ value?: number }>('flows');
      const flow = harness.flow();
      harness.worker<{ value?: number }, unknown>(queue.name, async (job) =>
        job.data.value === undefined ? { parent: true } : { doubled: job.data.value * 2 }
      );

      const tree = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          { name: 'child-a', queueName: queue.name, data: { value: 2 } },
          { name: 'child-b', queueName: queue.name, data: { value: 3 } },
        ],
      });

      await waitForState(queue, tree.job.id, 'completed', 15_000);
      const values = Object.values(await queue.getChildrenValues(tree.job.id));
      expect(values).toHaveLength(2);
      expect(values).toContainEqual({ doubled: 4 });
      expect(values).toContainEqual({ doubled: 6 });
    }, 30_000);

    test('getJobDependencies and getDependencies describe the parent link', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue<{ value?: number }>('flows');
      const flow = harness.flow();

      const tree = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          { name: 'child-a', queueName: queue.name, data: { value: 2 } },
          { name: 'child-b', queueName: queue.name, data: { value: 3 } },
        ],
      });
      const parentId = tree.job.id;

      const pending = await queue.getJobDependencies(parentId);
      expect(pending.unprocessed).toHaveLength(2);
      expect(Object.keys(pending.processed)).toHaveLength(0);
      expect(await queue.getJobDependenciesCount(parentId)).toEqual({
        processed: 0,
        unprocessed: 2,
      });
      expect(
        (await queue.getDependencies(parentId, 'unprocessed', 0, 10)).unprocessed
      ).toHaveLength(2);

      harness.worker<{ value?: number }, unknown>(queue.name, async (job) =>
        job.data.value === undefined ? { parent: true } : { doubled: job.data.value * 2 }
      );
      await waitForState(queue, parentId, 'completed', 15_000);

      const done = await queue.getJobDependencies(parentId);
      expect(Object.keys(done.processed)).toHaveLength(2);
      expect(done.unprocessed).toHaveLength(0);
      const processedPage = await queue.getDependencies(parentId, 'processed', 0, 10);
      expect(Object.keys(processedPage.processed ?? {})).toHaveLength(2);
      expect(processedPage.unprocessed).toEqual([]);
    }, 30_000);

    test('waitJobUntilFinished resolves with the job result', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue<{ value: number }>('waits');
      const Events = QueueEvents as unknown as QueueEventsCtor;
      const queueEvents =
        mode === 'embedded'
          ? new Events(queue.name, { embedded: true })
          : new Events(queue.name, { embedded: false, connection: harness.connection() });
      harness.addCleanup(() => queueEvents.close());
      await queueEvents.waitUntilReady();

      harness.worker<{ value: number }, { doubled: number }>(queue.name, async (job) => ({
        doubled: job.data.value * 2,
      }));
      const job = await queue.add('double', { value: 21 }, { durable: true });

      expect(await queue.waitJobUntilFinished(job.id, queueEvents, 30_000)).toEqual({
        doubled: 42,
      });
    }, 30_000);

    test('QueueEvents observes the documented lifecycle events', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue<{ value: number }>('events');
      const Events = QueueEvents as unknown as QueueEventsCtor;
      const queueEvents =
        mode === 'embedded'
          ? new Events(queue.name, { embedded: true })
          : new Events(queue.name, { embedded: false, connection: harness.connection() });
      harness.addCleanup(() => queueEvents.close());

      const seen: string[] = [];
      for (const event of ['waiting', 'active', 'completed']) {
        queueEvents.on(event as 'waiting', () => seen.push(event));
      }
      await queueEvents.waitUntilReady();

      harness.worker<{ value: number }, boolean>(queue.name, async () => true);
      const job = await queue.add('watched', { value: 1 }, { durable: true });
      await waitForState(queue, job.id, 'completed', 15_000);

      await waitUntil(
        () => seen.includes('waiting') && seen.includes('completed'),
        `waiting and completed events (saw ${seen.join(',')})`
      );
    }, 30_000);

    test('moveJobToCompleted finishes the running job', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue('moves');

      await withActiveJob(harness, queue, async (job) => {
        await queue.moveJobToCompleted(job.id, { success: true }, job.token);
        await waitForState(queue, job.id, 'completed');
      });
    }, 20_000);

    test('moveJobToFailed fails the running job with the given error', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue('moves');

      await withActiveJob(
        harness,
        queue,
        async (job) => {
          await queue.moveJobToFailed(job.id, new Error('reason'), job.token);
          await waitForState(queue, job.id, 'failed', 10_000);
          const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
          expect(entry?.error).toContain('reason');
        },
        { durable: true, attempts: 1 }
      );
    }, 20_000);

    test('moveJobToDelayed and moveJobToWait move a job between states', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue('moves');
      await queue.pauseAsync();
      const job = await queue.add('manual', {}, { durable: true });

      await queue.moveJobToDelayed(job.id, Date.now() + 60_000, 'token');
      await waitForState(queue, job.id, 'delayed');

      expect(await queue.moveJobToWait(job.id, 'token')).toBe(true);
      await waitUntil(
        async () => (await queue.getJobState(job.id)) !== 'delayed',
        'the job to leave the delayed state'
      );
    });

    test('moveJobToWaitingChildren parks the running job', async () => {
      harness = await startHarness('progress', mode);
      const queue = harness.queue('moves-children');

      await withActiveJob(harness, queue, async (job) => {
        expect(await queue.moveJobToWaitingChildren(job.id, job.token)).toBe(true);
        await waitForState(queue, job.id, 'waiting-children');
      });
    }, 20_000);
  });
}
