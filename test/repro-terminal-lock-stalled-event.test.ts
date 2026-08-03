import { describe, expect, test } from 'bun:test';
import { checkExpiredLocks } from '../src/application/lockManager';
import type { QueueManager } from '../src/application/queueManager';
import { CoreE2eHarness, type CoreE2eMode } from './core-e2e/support/harness';

function getLockContext(qm: QueueManager): Parameters<typeof checkExpiredLocks>[0] {
  return (
    qm as unknown as {
      contextFactory: {
        getLockContext(): Parameters<typeof checkExpiredLocks>[0];
      };
    }
  ).contextFactory.getLockContext();
}

async function waitForState(
  getState: () => Promise<string>,
  expected: string,
  timeoutMs = 2_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while ((await getState()) !== expected && Date.now() < deadline) {
    await Bun.sleep(10);
  }
  expect(await getState()).toBe(expected);
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe('terminal lock-expiry stalled event parity', () => {
  for (const mode of ['embedded', 'tcp'] as const satisfies readonly CoreE2eMode[]) {
    test(`${mode} Worker receives stalled before a lock-expired job reaches the DLQ`, async () => {
      const harness = await CoreE2eHarness.start(mode, 'terminal-lock-stalled-event');
      const queue = harness.queue<{ value: number }>('jobs');
      const processor = deferred();
      const worker = harness.worker(queue.name, async () => processor.promise, {
        heartbeatInterval: 0,
        useLocks: true,
        lockDuration: 50,
      });
      worker.on('error', () => undefined);
      const brokerEvents: string[] = [];
      const unsubscribe = harness.brokerManager().subscribe((event) => {
        if (
          event.queue === queue.name &&
          (event.eventType === 'stalled' || event.eventType === 'failed')
        ) {
          brokerEvents.push(event.eventType);
        }
      });

      try {
        await queue.setStallConfigAsync({
          enabled: true,
          stallInterval: 60_000,
          maxStalls: 1,
          gracePeriod: 0,
        });
        await worker.waitUntilReady();

        const stalled = new Promise<{ jobId: string; reason: string }>((resolve) => {
          worker.once('stalled', (jobId, reason) => resolve({ jobId, reason }));
        });
        const job = await queue.add('terminal-stall', { value: 1 }, { durable: true });
        await waitForState(() => queue.getJobState(job.id), 'active');
        await Bun.sleep(100);

        await checkExpiredLocks(getLockContext(harness.brokerManager()));

        expect(
          await Promise.race([
            stalled,
            Bun.sleep(1_000).then(() => {
              throw new Error('Worker stalled event timed out');
            }),
          ])
        ).toEqual({ jobId: job.id, reason: 'active' });
        expect(brokerEvents).toEqual(['stalled', 'failed']);
        expect(await queue.getJobState(job.id)).toBe('failed');
      } finally {
        processor.resolve();
        unsubscribe();
        await harness.close();
      }
    });

    test(`${mode} overlapping lock sweeps reclaim one generation exactly once`, async () => {
      const harness = await CoreE2eHarness.start(mode, 'overlapping-lock-sweeps');
      const queue = harness.queue<{ value: number }>('jobs');
      const processor = deferred();
      const worker = harness.worker(queue.name, async () => processor.promise, {
        heartbeatInterval: 0,
        useLocks: true,
        lockDuration: 50,
      });
      worker.on('error', () => undefined);
      const brokerEvents: string[] = [];
      const unsubscribe = harness.brokerManager().subscribe((event) => {
        if (event.queue === queue.name && event.eventType === 'stalled') {
          brokerEvents.push(event.eventType);
        }
      });

      try {
        await queue.setStallConfigAsync({
          enabled: true,
          stallInterval: 60_000,
          maxStalls: 3,
          gracePeriod: 0,
        });
        await worker.waitUntilReady();
        const job = await queue.add('single-recovery', { value: 1 }, { durable: true });
        await waitForState(() => queue.getJobState(job.id), 'active');
        worker.pause();
        await Bun.sleep(100);

        const context = getLockContext(harness.brokerManager());
        await Promise.all([checkExpiredLocks(context), checkExpiredLocks(context)]);

        expect(await queue.getJobState(job.id)).toBe('waiting');
        const recovered = await queue.getJob(job.id);
        expect(recovered?.attemptsMade).toBe(1);
        expect(recovered?.attemptsStarted).toBe(1);
        expect(recovered?.stalledCounter).toBe(1);
        expect(recovered?.toJSON().attemptsMade).toBe(1);
        expect(recovered?.asJSON().attemptsMade).toBe('1');
        const listed = await queue.getJobsAsync({ state: 'waiting' });
        expect(listed).toHaveLength(1);
        expect(listed[0]?.attemptsMade).toBe(1);
        expect(listed[0]?.attemptsStarted).toBe(1);
        expect(listed[0]?.stalledCounter).toBe(1);
        expect(brokerEvents).toEqual(['stalled']);
        expect(await queue.getJobCountsAsync()).toMatchObject({
          waiting: 1,
          active: 0,
          failed: 0,
        });
      } finally {
        processor.resolve();
        unsubscribe();
        await harness.close();
      }
    });
  }
});
