import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { Queue, shutdownManager } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';
import {
  closeTcpHarness,
  completeJob,
  failJob,
  makeTcpQueue,
  startTcpHarness,
  type TcpHarness,
} from './tcp-harness';

let harness: TcpHarness | null = null;

function name(prefix: string): string {
  return `${prefix}-${Bun.randomUUIDv7()}`;
}

afterEach(async () => {
  await closeTcpHarness(harness);
  harness = null;
  shutdownManager();
});

describe('retryJobs implements every exposed state', () => {
  test('embedded completed jobs are retried up to the generated count', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 5 }),
        fc.integer({ min: 1, max: 5 }),
        async (jobCount, requestedCount) => {
          const queueName = name('retry-completed-embedded');
          const queue = new Queue<{ index: number }>(queueName, { embedded: true });
          const manager = getSharedManager();
          const ids: string[] = [];
          try {
            for (let index = 0; index < jobCount; index++) {
              const job = await queue.add('task', { index });
              const pulled = await manager.pull(queueName);
              if (!pulled) throw new Error('Expected a completed job');
              await manager.ack(pulled.id, index);
              ids.push(job.id);
            }
            await queue.retryJobs({ state: 'completed', count: requestedCount });
            const states = await Promise.all(ids.map((id) => queue.getJobState(id)));
            expect(states.filter((state) => state === 'waiting')).toHaveLength(
              Math.min(jobCount, requestedCount)
            );
            expect(states.filter((state) => state === 'completed')).toHaveLength(
              Math.max(0, jobCount - requestedCount)
            );
          } finally {
            await queue.close();
            shutdownManager();
          }
        }
      ),
      { numRuns: 8 }
    );
  });

  test('TCP completed jobs are retried by the public method', async () => {
    harness = startTcpHarness();
    const queueName = name('retry-completed-tcp');
    const queue = makeTcpQueue(harness, queueName);
    const first = await completeJob(harness, queue, queueName, { index: 1 });
    const second = await completeJob(harness, queue, queueName, { index: 2 });
    await queue.retryJobs({ state: 'completed', count: 1 });
    const states = await Promise.all([queue.getJobState(first), queue.getJobState(second)]);
    expect(states.filter((state) => state === 'waiting')).toHaveLength(1);
    expect(states.filter((state) => state === 'completed')).toHaveLength(1);
  });

  test('completed retry honors the terminal timestamp cutoff', async () => {
    harness = startTcpHarness();
    const queueName = name('retry-completed-timestamp');
    const queue = makeTcpQueue(harness, queueName);
    const first = await completeJob(harness, queue, queueName, { index: 1 });
    const firstSnapshot = await harness.manager.getJob(
      first as import('../../src/domain/types/job').JobId
    );
    const cutoff = firstSnapshot?.completedAt;
    if (!cutoff) throw new Error('Expected a completion timestamp');
    await Bun.sleep(2);
    const second = await completeJob(harness, queue, queueName, { index: 2 });
    await queue.retryJobs({ state: 'completed', timestamp: cutoff });
    expect(await queue.getJobState(first)).toBe('waiting');
    expect(await queue.getJobState(second)).toBe('completed');
  });

  test('failed retry honors the terminal timestamp cutoff', async () => {
    harness = startTcpHarness();
    const queueName = name('retry-failed-timestamp');
    const queue = makeTcpQueue(harness, queueName);
    const first = await failJob(harness, queue, queueName, { index: 1 });
    const cutoff = harness.manager
      .getDlqEntries(queueName)
      .find((entry) => String(entry.job.id) === first)?.enteredAt;
    if (!cutoff) throw new Error('Expected a DLQ timestamp');
    await Bun.sleep(2);
    const second = await failJob(harness, queue, queueName, { index: 2 });
    await queue.retryJobs({ state: 'failed', timestamp: cutoff });
    expect(await queue.getJobState(first)).toBe('waiting');
    expect(await queue.getJobState(second)).toBe('failed');
  });

  test('an invalid completed count cannot retry every job', async () => {
    harness = startTcpHarness();
    const queueName = name('retry-completed-invalid-count');
    const queue = makeTcpQueue(harness, queueName);
    const id = await completeJob(harness, queue, queueName, { index: 1 });
    await queue.retryJobs({ state: 'completed', count: Number.NaN });
    expect(await queue.getJobState(id)).toBe('completed');
  });
});
