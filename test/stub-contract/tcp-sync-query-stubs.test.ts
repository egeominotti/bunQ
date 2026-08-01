import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import type { Queue } from '../../src/client';
import {
  closeTcpHarness,
  completeJob,
  failJob,
  makeTcpQueue,
  startTcpHarness,
  type TcpHarness,
} from './tcp-harness';

let harness: TcpHarness | null = null;
const runs = 8;

function setup(): TcpHarness {
  harness = startTcpHarness();
  return harness;
}

function name(prefix: string): string {
  return `${prefix}-${Bun.randomUUIDv7()}`;
}

async function closeQueue(queue: Queue<unknown>): Promise<void> {
  await queue.close();
}

afterEach(async () => {
  await closeTcpHarness(harness);
  harness = null;
});

describe('TCP query async companions are authoritative', () => {
  test('getJobsAsync returns the server jobs', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 6 }), async (count) => {
        const queue = makeTcpQueue<{ index: number }>(h, name('get-jobs'));
        for (let index = 0; index < count; index++) await queue.add('task', { index });
        expect((await queue.getJobsAsync()).map((job) => job.data.index)).toEqual(
          Array.from({ length: count }, (_, index) => index)
        );
        await closeQueue(queue);
      }),
      { numRuns: runs }
    );
  });

  test('getWaitingAsync returns waiting jobs', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('get-waiting'));
    const job = await queue.add('task', { state: 'waiting' });
    expect((await queue.getWaitingAsync()).map((entry) => entry.id)).toContain(job.id);
  });

  test('getDelayedAsync returns delayed jobs', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('get-delayed'));
    const job = await queue.add('task', { state: 'delayed' }, { delay: 60_000 });
    expect((await queue.getDelayedAsync()).map((entry) => entry.id)).toContain(job.id);
  });

  test('getActiveAsync returns active jobs', async () => {
    const h = setup();
    const queueName = name('get-active');
    const queue = makeTcpQueue(h, queueName);
    const job = await queue.add('task', { state: 'active' });
    expect(await h.manager.pull(queueName)).not.toBeNull();
    expect((await queue.getActiveAsync()).map((entry) => entry.id)).toContain(job.id);
  });

  test('getCompletedAsync returns completed jobs', async () => {
    const h = setup();
    const queueName = name('get-completed');
    const queue = makeTcpQueue(h, queueName);
    const id = await completeJob(h, queue, queueName, { state: 'completed' });
    expect((await queue.getCompletedAsync()).map((entry) => entry.id)).toContain(id);
  });

  test('getFailedAsync returns failed jobs', async () => {
    const h = setup();
    const queueName = name('get-failed');
    const queue = makeTcpQueue(h, queueName);
    const id = await failJob(h, queue, queueName, { state: 'failed' });
    expect((await queue.getFailedAsync()).map((entry) => entry.id)).toContain(id);
  });
});

describe('TCP count, control, and clean async companions are authoritative', () => {
  test('countAsync returns the server-side count', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 8 }), async (count) => {
        const queue = makeTcpQueue(h, name('count'));
        for (let index = 0; index < count; index++) await queue.add('task', { index });
        expect(await queue.countAsync()).toBe(count);
        await closeQueue(queue);
      }),
      { numRuns: runs }
    );
  });

  test('getCountsPerPriorityAsync returns every configured priority bucket', async () => {
    const h = setup();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 100 }), async (priority) => {
        const queue = makeTcpQueue(h, name('priority-count'));
        await queue.add('task', { index: 1 }, { priority });
        await queue.add('task', { index: 2 }, { priority });
        expect((await queue.getCountsPerPriorityAsync())[priority]).toBe(2);
        await closeQueue(queue);
      }),
      { numRuns: runs }
    );
  });

  test('isPausedAsync observes the server-side pause state', async () => {
    const h = setup();
    const queue = makeTcpQueue(h, name('is-paused'));
    await queue.pauseAsync();
    expect(await queue.isPausedAsync()).toBe(true);
  });

  test('cleanAsync returns removed IDs and removes completed jobs', async () => {
    const h = setup();
    const queueName = name('clean');
    const queue = makeTcpQueue(h, queueName);
    const id = await completeJob(h, queue, queueName, { value: 1 });
    const removed = await queue.cleanAsync(0, 10, 'completed');
    expect(removed).toContain(id);
    expect(await queue.getJob(id)).toBeNull();
  });
});
