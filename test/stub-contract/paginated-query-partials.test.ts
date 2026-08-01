import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { type JobOptions, Queue, shutdownManager } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';
import { closeTcpHarness, makeTcpQueue, startTcpHarness, type TcpHarness } from './tcp-harness';

let harness: TcpHarness | null = null;

function name(prefix: string): string {
  return `${prefix}-${Bun.randomUUIDv7()}`;
}

async function addInChunks(
  queue: Queue<{ index: number }>,
  total: number,
  options: (index: number) => JobOptions
): Promise<string[]> {
  const ids: string[] = [];
  const chunkSize = 500;
  for (let offset = 0; offset < total; offset += chunkSize) {
    const count = Math.min(chunkSize, total - offset);
    const jobs = await queue.addBulk(
      Array.from({ length: count }, (_, localIndex) => {
        const index = offset + localIndex;
        return { name: 'task', data: { index }, opts: options(index) };
      })
    );
    ids.push(...jobs.map((job) => job.id));
  }
  return ids;
}

function expectCompletePage(jobs: Array<{ id: string }>, total: number): void {
  expect(jobs).toHaveLength(total);
  expect(new Set(jobs.map((job) => job.id))).toHaveLength(total);
}

async function failPulledJobs(manager: TcpHarness['manager'], queueName: string): Promise<void> {
  const jobs = await manager.pullBatch(queueName, 1001, 0);
  expect(jobs).toHaveLength(1001);
  for (const job of jobs) await manager.fail(job.id, 'terminal query fixture');
}

afterEach(async () => {
  await closeTcpHarness(harness);
  harness = null;
  shutdownManager();
});

describe('unbounded public state queries', () => {
  test('embedded getJobs honors the exposed descending order option', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.integer({ min: 1, max: 1_000_000 }), {
          minLength: 3,
          maxLength: 8,
        }),
        async (timestamps) => {
          const queue = new Queue<{ timestamp: number }>(name('embedded-query-order'), {
            embedded: true,
          });
          await queue.addBulk(
            timestamps.map((timestamp) => ({
              name: 'task',
              data: { timestamp },
              opts: { timestamp },
            }))
          );

          const expected = [...timestamps].sort((a, b) => b - a);
          const jobs = queue.getJobs({ state: 'waiting', start: 0, end: -1, asc: false });
          expect(jobs.map((job) => job.data.timestamp)).toEqual(expected);
          await queue.close();
        }
      ),
      { numRuns: 3 }
    );
  });

  test('TCP getJobsAsync honors the exposed descending order option', async () => {
    harness = startTcpHarness();
    await fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.integer({ min: 1, max: 1_000_000 }), {
          minLength: 3,
          maxLength: 8,
        }),
        async (timestamps) => {
          const queue = makeTcpQueue<{ timestamp: number }>(harness!, name('tcp-query-order'));
          await queue.addBulk(
            timestamps.map((timestamp) => ({
              name: 'task',
              data: { timestamp },
              opts: { timestamp },
            }))
          );

          const expected = [...timestamps].sort((a, b) => b - a);
          const jobs = await queue.getJobsAsync({
            state: 'waiting',
            start: 0,
            end: -1,
            asc: false,
          });
          expect(jobs.map((job) => job.data.timestamp)).toEqual(expected);
        }
      ),
      { numRuns: 3 }
    );
  });

  test('embedded getPrioritized crosses the default embedded query page', async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (extra) => {
        const queue = new Queue<{ index: number }>(name('embedded-prioritized'), {
          embedded: true,
        });
        const total = 100 + extra;
        await addInChunks(queue, total, () => ({ priority: 1 }));

        expect((await queue.getPrioritized()).length).toBe(total);
        expect(await queue.getPrioritizedCount()).toBe(total);
        await queue.close();
      }),
      { numRuns: 2 }
    );
  });

  test('getPrioritized and getPrioritizedCount cross the first TCP page', async () => {
    harness = startTcpHarness();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (extra) => {
        const queue = makeTcpQueue<{ index: number }>(harness!, name('prioritized-pages'));
        const total = 1000 + extra;
        await addInChunks(queue, total, () => ({ priority: 1 }));

        expect(await queue.getPrioritizedCount()).toBe(total);
        expect((await queue.getPrioritized()).length).toBe(total);
      }),
      { numRuns: 2 }
    );
  });

  test('getJobsAsync and its waiting alias honor end=-1 beyond one page', async () => {
    harness = startTcpHarness();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (extra) => {
        const queue = makeTcpQueue<{ index: number }>(harness!, name('waiting-pages'));
        const total = 1000 + extra;
        await addInChunks(queue, total, (index) => ({ timestamp: index + 1 }));

        const generic = await queue.getJobsAsync({ state: 'waiting', start: 0, end: -1 });
        const alias = await queue.getWaitingAsync(0, -1);
        const tail = await queue.getJobsAsync({ state: 'waiting', start: 7, end: -1 });
        const descending = await queue.getJobsAsync({
          state: 'waiting',
          start: 0,
          end: -1,
          asc: false,
        });
        expect(generic).toHaveLength(total);
        expect(new Set(generic.map((job) => job.id)).size).toBe(total);
        expect(alias).toHaveLength(total);
        expect(tail).toHaveLength(total - 7);
        expect(descending.map((job) => job.data.index)).toEqual(
          Array.from({ length: total }, (_, index) => total - index - 1)
        );
        expect(await queue.getWaitingAsync(5, 15)).toHaveLength(10);
      }),
      { numRuns: 2 }
    );
  });

  test('every embedded synchronous state alias honors end=-1', async () => {
    const manager = getSharedManager();
    const total = 101;

    const waitingName = name('embedded-waiting-pages');
    const waiting = new Queue<{ index: number }>(waitingName, { embedded: true });
    await addInChunks(waiting, total, () => ({}));
    expectCompletePage(waiting.getJobs({ state: 'waiting', end: -1 }), total);
    expectCompletePage(waiting.getWaiting(0, -1), total);

    const delayed = new Queue<{ index: number }>(name('embedded-delayed-pages'), {
      embedded: true,
    });
    await addInChunks(delayed, total, () => ({ delay: 60_000 }));
    expectCompletePage(delayed.getJobs({ state: 'delayed', end: -1 }), total);
    expectCompletePage(delayed.getDelayed(0, -1), total);

    const lifecycleName = name('embedded-lifecycle-pages');
    const lifecycle = new Queue<{ index: number }>(lifecycleName, { embedded: true });
    await addInChunks(lifecycle, total, () => ({}));
    const active = await manager.pullBatch(lifecycleName, total, 0);
    expectCompletePage(lifecycle.getJobs({ state: 'active', end: -1 }), total);
    expectCompletePage(lifecycle.getActive(0, -1), total);
    await manager.ackBatch(active.map((job) => job.id));
    expectCompletePage(lifecycle.getJobs({ state: 'completed', end: -1 }), total);
    expectCompletePage(lifecycle.getCompleted(0, -1), total);

    const failedName = name('embedded-failed-pages');
    const failed = new Queue<{ index: number }>(failedName, { embedded: true });
    await addInChunks(failed, total, () => ({ attempts: 1 }));
    const failedJobs = await manager.pullBatch(failedName, total, 0);
    for (const job of failedJobs) await manager.fail(job.id, 'terminal query fixture');
    expectCompletePage(failed.getJobs({ state: 'failed', end: -1 }), total);
    expectCompletePage(failed.getFailed(0, -1), total);

    await Promise.all([waiting.close(), delayed.close(), lifecycle.close(), failed.close()]);
  });

  test('every TCP asynchronous state alias honors end=-1', async () => {
    harness = startTcpHarness();
    const total = 1001;

    const waitingName = name('tcp-waiting-pages');
    const waiting = makeTcpQueue<{ index: number }>(harness, waitingName);
    await addInChunks(waiting, total, () => ({}));
    expectCompletePage(await waiting.getWaitingAsync(0, -1), total);

    const delayed = makeTcpQueue<{ index: number }>(harness, name('tcp-delayed-pages'));
    await addInChunks(delayed, total, () => ({ delay: 60_000 }));
    expectCompletePage(await delayed.getDelayedAsync(0, -1), total);

    const lifecycleName = name('tcp-lifecycle-pages');
    const lifecycle = makeTcpQueue<{ index: number }>(harness, lifecycleName);
    await addInChunks(lifecycle, total, () => ({}));
    const active = await harness.manager.pullBatch(lifecycleName, total, 0);
    expectCompletePage(await lifecycle.getActiveAsync(0, -1), total);
    await harness.manager.ackBatch(active.map((job) => job.id));
    expectCompletePage(await lifecycle.getCompletedAsync(0, -1), total);

    const failedName = name('tcp-failed-pages');
    const failed = makeTcpQueue<{ index: number }>(harness, failedName);
    await addInChunks(failed, total, () => ({ attempts: 1 }));
    await failPulledJobs(harness.manager, failedName);
    expectCompletePage(await failed.getFailedAsync(0, -1), total);
  });

  test('embedded waiting-children list and count cross the default query page', async () => {
    const queue = new Queue<{ index: number }>(name('embedded-waiting-child-pages'), {
      embedded: true,
    });
    const childQueue = new Queue<{ index: number }>(name('embedded-dependency-pages'), {
      embedded: true,
    });
    const total = 101;
    const childIds = await addInChunks(childQueue, total, () => ({}));
    await addInChunks(queue, total, (index) => ({ dependsOn: [childIds[index]] }));

    expect(await queue.getWaitingChildrenCount()).toBe(total);
    expectCompletePage(await queue.getWaitingChildren(), total);
    await Promise.all([queue.close(), childQueue.close()]);
  });

  test('getWaitingChildren and getWaitingChildrenCount cross the first TCP page', async () => {
    harness = startTcpHarness();
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (extra) => {
        const queue = makeTcpQueue<{ index: number }>(harness!, name('waiting-child-pages'));
        const childQueue = makeTcpQueue<{ index: number }>(harness!, name('dependency-pages'));
        const total = 1000 + extra;
        const childIds = await addInChunks(childQueue, total, () => ({}));
        await addInChunks(queue, total, (index) => ({
          dependsOn: [childIds[index]],
        }));

        expect(await queue.getWaitingChildrenCount()).toBe(total);
        expect((await queue.getWaitingChildren()).length).toBe(total);
      }),
      { numRuns: 2 }
    );
  });
});
