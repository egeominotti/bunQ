import { afterEach, describe, expect, test } from 'bun:test';
import {
  FlowProducer,
  Queue,
  QueueEventsPro,
  QueuePro,
  shutdownManager,
  Worker,
  WorkerPro,
} from '../src/client';

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

describe('BullMQ Pro advanced compatibility', () => {
  test('enforces group max size, pause and intra-group priority', async () => {
    const name = `pro-groups-${Bun.randomUUIDv7()}`;
    const producer = queue<{ value: string }>(name);
    await producer.add('job', { value: 'low' }, { group: { id: 'A', maxSize: 2, priority: 9 } });
    await producer.add('job', { value: 'high' }, { group: { id: 'A', maxSize: 2, priority: 1 } });
    await expect(
      producer.add('job', { value: 'overflow' }, { group: { id: 'A', maxSize: 2 } })
    ).rejects.toThrow('maximum size');

    expect(await producer.getCountsPerPriorityForGroup('A')).toEqual({ 1: 1, 9: 1 });
    expect((await producer.getGroupJobs('A')).map((job) => job.data.value)).toEqual([
      'low',
      'high',
    ]);
    expect(await producer.pauseGroup('A')).toBe(true);
    expect(await producer.pauseGroup('A')).toBe(false);
    expect(await producer.isGroupPaused('A')).toBe(true);
    expect(await shutdownlessPull(name)).toBeNull();
    expect(await producer.resumeGroup('A')).toBe(true);
    expect((await shutdownlessPull(name))?.data).toEqual({ value: 'high' });
    expect((await shutdownlessPull(name))?.data).toEqual({ value: 'low' });
  });

  test('validates the BullMQ Pro intra-group priority range', async () => {
    const producer = queue(`pro-group-priority-${Bun.randomUUIDv7()}`);

    await expect(producer.add('job', {}, { group: { id: 'A', priority: -1 } })).rejects.toThrow(
      'group.priority must be between 0 and 2097151'
    );
    await expect(
      producer.add('job', {}, { group: { id: 'A', priority: 2_097_152 } })
    ).rejects.toThrow('group.priority must be between 0 and 2097151');

    const accepted = await producer.add('job', {}, { group: { id: 'A', priority: 2_097_151 } });
    expect(accepted.priority).toBe(2_097_151);
  });

  test('rejects invalid group priorities atomically in batches and flows', async () => {
    const name = `pro-group-priority-atomic-${Bun.randomUUIDv7()}`;
    const producer = queue(name);
    await expect(
      producer.addBulk([
        { name: 'valid', data: {}, opts: { group: { id: 'A', priority: 1 } } },
        { name: 'invalid', data: {}, opts: { group: { id: 'A', priority: -1 } } },
      ])
    ).rejects.toThrow('group.priority must be between 0 and 2097151');
    expect(await producer.countAsync()).toBe(0);

    const flow = new FlowProducer({ embedded: true });
    try {
      await expect(
        flow.add({
          name: 'invalid-flow',
          queueName: name,
          data: {},
          opts: { group: { id: 'A', priority: 2_097_152 } },
        })
      ).rejects.toThrow('group.priority must be between 0 and 2097151');
      expect(await producer.countAsync()).toBe(0);
    } finally {
      await flow.close();
    }
  });

  test('processes a native batch once and supports selective failure', async () => {
    const name = `pro-batch-${Bun.randomUUIDv7()}`;
    const producer = queue<{ value: number }>(name);
    await producer.addBulk([1, 2, 3].map((value) => ({ name: 'job', data: { value } })));
    const finished = Promise.withResolvers<undefined>();
    const completed: number[] = [];
    const failed: number[] = [];
    let invocations = 0;
    const worker = new Worker<{ value: number }, string>(
      name,
      async (job) => {
        invocations++;
        const batch = job.getBatch?.() ?? [];
        expect(batch.map((member) => member.data.value)).toEqual([1, 2, 3]);
        batch[1].setAsFailed?.(new Error('selective failure'));
        return 'batch-result';
      },
      { batch: { size: 3, minSize: 3, timeout: 100 }, concurrency: 1, embedded: true }
    );
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('completed', (job) => {
      completed.push(job.data.value);
      if (completed.length + failed.length === 3) finished.resolve(undefined);
    });
    worker.on('failed', (job) => {
      failed.push(job.data.value);
      if (completed.length + failed.length === 3) finished.resolve(undefined);
    });
    await finished.promise;
    expect(invocations).toBe(1);
    expect(completed.sort((left, right) => left - right)).toEqual([1, 3]);
    expect(failed).toEqual([2]);
  });

  test('waits indefinitely for minSize and ignores it for non-affinity groups', async () => {
    const waitingName = `pro-batch-min-${Bun.randomUUIDv7()}`;
    const waitingQueue = queue<{ value: number }>(waitingName);
    await waitingQueue.add('job', { value: 1 });
    const fullBatch = Promise.withResolvers<number[]>();
    const waitingWorker = new Worker<{ value: number }, void>(
      waitingName,
      async (job) => {
        fullBatch.resolve((job.getBatch?.() ?? []).map((member) => member.data.value));
      },
      { batch: { size: 2, minSize: 2 }, embedded: true }
    );
    workers.push(waitingWorker as Worker<unknown, unknown>);
    await Bun.sleep(50);
    await waitingQueue.add('job', { value: 2 });
    expect(await fullBatch.promise).toEqual([1, 2]);

    const groupedName = `pro-batch-group-min-${Bun.randomUUIDv7()}`;
    const groupedQueue = queue(groupedName);
    await groupedQueue.add('job', {}, { group: { id: 'A' } });
    const groupedBatch = Promise.withResolvers<undefined>();
    const groupedWorker = new Worker(groupedName, async () => groupedBatch.resolve(undefined), {
      batch: { size: 2, minSize: 2, timeout: 1_000 },
      embedded: true,
    });
    workers.push(groupedWorker as Worker<unknown, unknown>);
    await Promise.race([
      groupedBatch.promise,
      Bun.sleep(300).then(() => {
        throw new Error('grouped batch incorrectly waited for minSize');
      }),
    ]);
  });

  test('consumes Observable results and aborts cancelled processors', async () => {
    const name = `pro-observable-${Bun.randomUUIDv7()}`;
    const producer = queue<{ kind: string }>(name);
    await producer.add('observable', { kind: 'observable' });
    await producer.add('cancel', { kind: 'cancel' });
    const finished = Promise.withResolvers<undefined>();
    const outcomes: string[] = [];
    const worker = new Worker<{ kind: string }, string>(
      name,
      (job, { signal } = { signal: new AbortController().signal }) => {
        if (job.data.kind === 'cancel') {
          return new Promise<string>((_resolve, reject) => {
            signal.addEventListener('abort', () => reject(signal.reason), { once: true });
          });
        }
        return {
          subscribe(observer) {
            observer.next('first');
            observer.next('last');
            observer.complete();
          },
        };
      },
      { concurrency: 1, embedded: true }
    );
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('active', (job) => {
      if (job.data.kind === 'cancel') worker.cancelJob(job.id, 'risk limit breached');
    });
    worker.on('completed', (_job, result) => outcomes.push(`completed:${result}`));
    worker.on('failed', (_job, error) => {
      outcomes.push(`failed:${error.message}`);
      finished.resolve(undefined);
    });
    await finished.promise;
    expect(outcomes).toEqual(['completed:last', 'failed:risk limit breached']);
  });

  test('manually rate limits and requeues only the current group', async () => {
    const name = `pro-manual-rate-${Bun.randomUUIDv7()}`;
    const producer = queue<{ value: string }>(name);
    await producer.add('job', { value: 'A' }, { group: { id: 'A' } });
    const completed = Promise.withResolvers<undefined>();
    let attempts = 0;
    const worker = new Worker<{ value: string }, string>(
      name,
      async (job) => {
        attempts++;
        if (attempts === 1) {
          await worker.rateLimitGroup(job, 30);
          throw Worker.RateLimitError();
        }
        return job.data.value;
      },
      { embedded: true, group: { concurrency: 1 } }
    );
    workers.push(worker as Worker<unknown, unknown>);
    worker.on('completed', () => completed.resolve(undefined));
    await completed.promise;
    expect(attempts).toBe(2);
  });

  test('exports Pro class aliases', () => {
    expect(QueuePro).toBe(Queue);
    expect(WorkerPro).toBe(Worker);
    expect(QueueEventsPro.name).toBe('QueueEvents');
  });
});

async function shutdownlessPull(name: string) {
  const { getSharedManager } = await import('../src/client/manager');
  return await getSharedManager().pull(name, 0);
}
