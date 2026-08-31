import { afterEach, describe, expect, test } from 'bun:test';
import { Queue, shutdownManager, Worker } from '../src/client';
import { resolveProcessorResult } from '../src/client/worker/processorResult';

const queues: Queue<unknown>[] = [];
const workers: Worker<unknown, unknown>[] = [];

function queue<T>(name: string): Queue<T> {
  const instance = new Queue<T>(name, { autoBatch: { enabled: false }, embedded: true });
  queues.push(instance as Queue<unknown>);
  return instance;
}

afterEach(async () => {
  await Promise.allSettled(workers.splice(0).map((worker) => worker.close(true)));
  for (const instance of queues.splice(0)) instance.close();
  shutdownManager();
});

describe('BullMQ Pro Worker regressions', () => {
  test('a native batch consumes one limiter slot per member', async () => {
    const name = `pro-batch-limit-${Bun.randomUUIDv7()}`;
    const producer = queue<{ value: number }>(name);
    await producer.addBulk(
      [1, 2, 3].map((value) => ({ name: 'job', data: { value }, opts: { attempts: 1 } }))
    );
    const release = Promise.withResolvers<string>();
    const firstTwo = Promise.withResolvers<undefined>();
    const active: number[] = [];
    const worker = new Worker<{ value: number }, string>(name, () => release.promise, {
      autorun: false,
      batch: { size: 2 },
      concurrency: 2,
      embedded: true,
      limiter: { max: 2, duration: 1_000 },
    });
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('active', (job) => {
      active.push(job.data.value);
      if (active.length === 2) firstTwo.resolve(undefined);
    });
    worker.run();

    await firstTwo.promise;
    await Bun.sleep(50);
    expect(active).toHaveLength(2);
    expect(worker.getRateLimiterInfo()?.current).toBe(2);
    release.resolve('done');
  });

  test('waiting for batch minSize does not consume limiter slots', async () => {
    const name = `pro-batch-min-limit-${Bun.randomUUIDv7()}`;
    const producer = queue(name);
    await producer.add('job', {}, { attempts: 1 });
    let invocations = 0;
    const worker = new Worker(
      name,
      () => {
        invocations++;
      },
      {
        batch: { size: 2, minSize: 2 },
        embedded: true,
        limiter: { max: 2, duration: 1_000 },
      }
    );
    workers.push(worker as Worker<unknown, unknown>);

    await Bun.sleep(80);
    expect(invocations).toBe(0);
    expect(worker.getRateLimiterInfo()?.current).toBe(0);
  });

  test('a synchronously throwing batch processor runs once', async () => {
    const name = `pro-batch-sync-throw-${Bun.randomUUIDv7()}`;
    const producer = queue(name);
    await producer.addBulk(
      [1, 2, 3].map((value) => ({ name: 'job', data: { value }, opts: { attempts: 1 } }))
    );
    const failed = Promise.withResolvers<undefined>();
    let failures = 0;
    let invocations = 0;
    const worker = new Worker(
      name,
      () => {
        invocations++;
        throw new Error('synchronous batch failure');
      },
      { autorun: false, batch: { size: 3, minSize: 3 }, embedded: true }
    );
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('failed', () => {
      if (++failures === 3) failed.resolve(undefined);
    });
    worker.run();

    await failed.promise;
    expect(invocations).toBe(1);
  });

  test('synchronous Observable settlement cleans up exactly once', async () => {
    const name = `pro-observable-sync-${Bun.randomUUIDv7()}`;
    const producer = queue<{ outcome: 'complete' | 'error' }>(name);
    await producer.add('complete', { outcome: 'complete' }, { attempts: 1 });
    await producer.add('error', { outcome: 'error' }, { attempts: 1 });
    const settled = Promise.withResolvers<undefined>();
    const cleanups = { complete: 0, error: 0 };
    let outcomes = 0;
    const worker = new Worker<{ outcome: 'complete' | 'error' }, string>(
      name,
      (job) => ({
        subscribe(observer) {
          if (job.data.outcome === 'complete') {
            observer.next('done');
            observer.complete();
          } else {
            observer.error(new Error('observable failure'));
          }
          return () => cleanups[job.data.outcome]++;
        },
      }),
      { autorun: false, concurrency: 1, embedded: true }
    );
    workers.push(worker as Worker<unknown, unknown>);
    const onOutcome = () => {
      if (++outcomes === 2) settled.resolve(undefined);
    };
    worker.on('completed', onOutcome);
    worker.on('failed', onOutcome);
    worker.run();

    await settled.promise;
    expect(cleanups).toEqual({ complete: 1, error: 1 });
  });

  test('an aborted Observable cleans up exactly once', async () => {
    const name = `pro-observable-abort-${Bun.randomUUIDv7()}`;
    const producer = queue(name);
    const job = await producer.add('abort', {}, { attempts: 1 });
    const subscribed = Promise.withResolvers<undefined>();
    const failed = Promise.withResolvers<undefined>();
    let cleanups = 0;
    const worker = new Worker(
      name,
      () => ({
        subscribe() {
          subscribed.resolve(undefined);
          return () => cleanups++;
        },
      }),
      { autorun: false, embedded: true }
    );
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('failed', () => failed.resolve(undefined));
    worker.run();

    await subscribed.promise;
    expect(worker.cancelJob(job.id, 'abort regression')).toBe(true);
    await failed.promise;
    expect(cleanups).toBe(1);
  });

  test('cancelling a non-leader member aborts the shared batch processor', async () => {
    const name = `pro-batch-member-cancel-${Bun.randomUUIDv7()}`;
    const producer = queue(name);
    const jobs = await producer.addBulk([
      { name: 'leader', data: {}, opts: { attempts: 1 } },
      { name: 'member', data: {}, opts: { attempts: 1 } },
    ]);
    const started = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const settled = Promise.withResolvers<undefined>();
    let signalObserved = false;
    let outcomes = 0;
    const worker = new Worker(
      name,
      async (_job, { signal }) => {
        started.resolve(undefined);
        await release.promise;
        signalObserved = signal.aborted;
        if (signal.aborted) throw signal.reason;
        return 'completed without cancellation';
      },
      { autorun: false, batch: { size: 2, minSize: 2 }, embedded: true }
    );
    workers.push(worker as Worker<unknown, unknown>);
    const onOutcome = () => {
      if (++outcomes === 2) settled.resolve(undefined);
    };
    worker.on('completed', onOutcome);
    worker.on('failed', onOutcome);
    worker.run();

    await started.promise;
    expect(worker.cancelJob(jobs[1].id, 'cancel batch member')).toBe(true);
    release.resolve(undefined);
    await settled.promise;
    expect(signalObserved).toBe(true);
    expect(await Promise.all(jobs.map((job) => job.getState()))).toEqual(['failed', 'failed']);
  });

  test('a batch larger than limiter capacity runs in bounded chunks', async () => {
    const name = `pro-batch-limit-capacity-${Bun.randomUUIDv7()}`;
    const producer = queue(name);
    await producer.addBulk(
      [1, 2, 3].map((value) => ({ name: 'job', data: { value }, opts: { attempts: 1 } }))
    );
    const completed = Promise.withResolvers<undefined>();
    const batchSizes: number[] = [];
    let completedCount = 0;
    const worker = new Worker<{ value: number }, number>(
      name,
      (job) => {
        batchSizes.push(job.getBatch?.().length ?? 1);
        return job.data.value;
      },
      {
        autorun: false,
        batch: { size: 3 },
        embedded: true,
        limiter: { max: 2, duration: 100 },
      }
    );
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('completed', () => {
      if (++completedCount === 3) completed.resolve(undefined);
    });
    worker.run();

    await Promise.race([completed.promise, Bun.sleep(750)]);
    expect(completedCount).toBe(3);
    expect(batchSizes).toEqual([2, 1]);
  });

  test('batch minSize cannot exceed limiter capacity', () => {
    const name = `pro-batch-invalid-capacity-${Bun.randomUUIDv7()}`;
    expect(
      () =>
        new Worker(name, () => undefined, {
          autorun: false,
          batch: { size: 3, minSize: 3 },
          embedded: true,
          limiter: { max: 2, duration: 100 },
        })
    ).toThrow('batch.minSize cannot exceed limiter.max');
  });

  test('per-group concurrency permits minSize above each group capacity', () => {
    const worker = new Worker(`pro-batch-group-capacity-${Bun.randomUUIDv7()}`, () => undefined, {
      autorun: false,
      batch: { size: 3, minSize: 3 },
      embedded: true,
      limiter: { max: 2, duration: 0, groupKey: 'tenant' },
    });
    workers.push(worker as Worker<unknown, unknown>);

    expect(worker).toBeInstanceOf(Worker);
  });

  test('an already-aborted rejected Promise is still observed', async () => {
    const controller = new AbortController();
    controller.abort(new Error('cancel before resolution'));
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);

    try {
      await expect(
        resolveProcessorResult(
          Promise.reject(new Error('late processor rejection')),
          controller.signal
        )
      ).rejects.toThrow('cancel before resolution');
      await Bun.sleep(0);
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });
});
