import { afterEach, describe, expect, test } from 'bun:test';
import { FlowProducer, Queue, Worker, shutdownManager } from '../../src/client';
import { getFlowDependencies } from '../../src/client/flowJobDependencies';
import type { TcpConnectionPool } from '../../src/client/tcpPool';
import { closeTcpHarness, makeTcpQueue, startTcpHarness, type TcpHarness } from './tcp-harness';

let harness: TcpHarness | null = null;
const producers: FlowProducer[] = [];
const workers: Worker<unknown, unknown>[] = [];

function name(prefix: string): string {
  return `${prefix}-${Bun.randomUUIDv7()}`;
}

afterEach(async () => {
  for (const worker of workers.splice(0).reverse()) {
    try {
      await worker.close(true);
    } catch {
      // Best-effort cleanup after an expected red assertion.
    }
  }
  for (const producer of producers.splice(0).reverse()) {
    try {
      await producer.close();
    } catch {
      // Best-effort cleanup after an expected red assertion.
    }
  }
  await closeTcpHarness(harness);
  harness = null;
  shutdownManager();
});

describe('embedded Queue worker discovery', () => {
  test('getWorkers lists a live embedded Worker for this queue', async () => {
    const queueName = name('embedded-workers');
    const queue = new Queue(queueName, { embedded: true });
    const worker = new Worker(queueName, async () => undefined, {
      embedded: true,
      autorun: true,
    });
    workers.push(worker as Worker<unknown, unknown>);

    const listed = await queue.getWorkers();
    expect(listed.some((entry) => entry.name === queueName)).toBe(true);
    await queue.close();
  });

  test('getWorkersCount counts live embedded Workers for this queue', async () => {
    const queueName = name('embedded-worker-count');
    const queue = new Queue(queueName, { embedded: true });
    for (let index = 0; index < 2; index++) {
      workers.push(
        new Worker(queueName, async () => undefined, {
          embedded: true,
          autorun: true,
        }) as Worker<unknown, unknown>
      );
    }

    expect(await queue.getWorkersCount()).toBe(2);
    await queue.close();
  });
});

describe('embedded dependency queries', () => {
  test('a missing parent returns empty dependency collections and counts', async () => {
    const queue = new Queue(name('embedded-missing-parent'), { embedded: true });

    expect(await queue.getDependencies('missing-parent')).toEqual({
      processed: {},
      unprocessed: [],
    });
    expect(await queue.getJobDependenciesCount('missing-parent')).toEqual({
      processed: 0,
      unprocessed: 0,
    });
    await queue.close();
  });
});

describe('dependency query error boundaries', () => {
  function fakeTcp(
    send: (command: Record<string, unknown>) => Promise<Record<string, unknown>>
  ): TcpConnectionPool {
    return { send } as unknown as TcpConnectionPool;
  }

  test.each([
    'backend unavailable',
    'Job not found or forbidden',
  ])('a non-matching broker error is propagated: %s', async (error) => {
    const tcp = fakeTcp(async () => ({ ok: false, error }));
    await expect(getFlowDependencies('parent', 'queue', false, tcp)).rejects.toThrow(error);
  });

  test('an error without a message uses the operation fallback', async () => {
    const tcp = fakeTcp(async () => ({ ok: false }));
    await expect(getFlowDependencies('parent', 'queue', false, tcp)).rejects.toThrow(
      'GetJob failed'
    );
  });

  test('a referenced child that is missing remains an integrity error', async () => {
    let requests = 0;
    const tcp = fakeTcp(async () => {
      requests++;
      return requests === 1
        ? { ok: true, job: { childrenIds: ['missing-child'] } }
        : { ok: false, error: 'Job not found' };
    });
    await expect(getFlowDependencies('parent', 'queue', false, tcp)).rejects.toThrow(
      'Job not found'
    );
  });

  test('a transport rejection is propagated', async () => {
    const tcp = fakeTcp(async () => {
      throw new Error('connection closed');
    });
    await expect(getFlowDependencies('parent', 'queue', false, tcp)).rejects.toThrow(
      'connection closed'
    );
  });
});

describe('TCP Queue worker discovery', () => {
  test('getWorkers decodes the server payload and filters workers by queue', async () => {
    harness = startTcpHarness();
    const queueName = name('tcp-workers');
    const otherQueue = name('tcp-workers-other');
    const queue = makeTcpQueue(harness, queueName);
    harness.manager.registerWorker(queueName, [queueName], 1, { workerId: 'matching-worker' });
    harness.manager.registerWorker(otherQueue, [otherQueue], 1, { workerId: 'other-worker' });

    expect((await queue.getWorkers()).map((worker) => worker.id)).toEqual(['matching-worker']);
  });

  test('getWorkersCount counts only workers registered for this queue', async () => {
    harness = startTcpHarness();
    const queueName = name('tcp-worker-count');
    const queue = makeTcpQueue(harness, queueName);
    harness.manager.registerWorker(queueName, [queueName], 1, { workerId: 'worker-one' });
    harness.manager.registerWorker(queueName, [queueName], 2, { workerId: 'worker-two' });
    harness.manager.registerWorker('unrelated', ['unrelated'], 1, { workerId: 'worker-other' });

    expect(await queue.getWorkersCount()).toBe(2);
  });
});

interface FlowFixture {
  parentQueue: Queue<Record<string, unknown>>;
  parentId: string;
  childId: string;
  childQueueName: string;
}

async function makeTcpFlow(h: TcpHarness): Promise<FlowFixture> {
  const parentQueueName = name('parent');
  const childQueueName = name('child');
  const parentQueue = makeTcpQueue<Record<string, unknown>>(h, parentQueueName);
  const producer = new FlowProducer({
    embedded: false,
    connection: { host: '127.0.0.1', port: h.port, poolSize: 1, pingInterval: 0 },
  });
  producers.push(producer);
  const flow = await producer.add<Record<string, unknown>>({
    name: 'parent',
    queueName: parentQueueName,
    data: { role: 'parent' },
    children: [
      {
        name: 'child',
        queueName: childQueueName,
        data: { role: 'child' },
      },
    ],
  });
  const child = flow.children?.[0];
  if (!child) throw new Error('Expected one flow child');
  return {
    parentQueue,
    parentId: flow.job.id,
    childId: child.job.id,
    childQueueName,
  };
}

describe('TCP waiting-children and dependency APIs', () => {
  test('a missing parent returns empty dependency collections and counts', async () => {
    harness = startTcpHarness();
    const queue = makeTcpQueue(harness, name('tcp-missing-parent'));

    expect(await queue.getDependencies('missing-parent')).toEqual({
      processed: {},
      unprocessed: [],
    });
    expect(await queue.getJobDependencies('missing-parent')).toMatchObject({
      processed: {},
      unprocessed: [],
    });
    expect(await queue.getJobDependenciesCount('missing-parent')).toEqual({
      processed: 0,
      unprocessed: 0,
    });
  });

  test('getWaitingChildren returns flow parents blocked on children', async () => {
    harness = startTcpHarness();
    const flow = await makeTcpFlow(harness);
    expect((await flow.parentQueue.getWaitingChildren()).map((job) => job.id)).toContain(
      flow.parentId
    );
  });

  test('getWaitingChildrenCount counts flow parents blocked on children', async () => {
    harness = startTcpHarness();
    const flow = await makeTcpFlow(harness);
    expect(await flow.parentQueue.getWaitingChildrenCount()).toBe(1);
  });

  test('getJobDependencies returns the real unprocessed child keys', async () => {
    harness = startTcpHarness();
    const flow = await makeTcpFlow(harness);
    const dependencies = await flow.parentQueue.getJobDependencies(flow.parentId);
    expect(dependencies.unprocessed).toEqual([`${flow.childQueueName}:${flow.childId}`]);
    expect(dependencies.processed).toEqual({});
  });

  test('getJobDependenciesCount returns the real child counts', async () => {
    harness = startTcpHarness();
    const flow = await makeTcpFlow(harness);
    expect(await flow.parentQueue.getJobDependenciesCount(flow.parentId)).toEqual({
      processed: 0,
      unprocessed: 1,
    });
  });

  test('moveJobToWaitingChildren moves an active TCP job', async () => {
    harness = startTcpHarness();
    const queueName = name('move-waiting-children');
    const queue = makeTcpQueue(harness, queueName);
    const job = await queue.add('parent', { role: 'dynamic-parent' });
    expect(await harness.manager.pull(queueName)).not.toBeNull();

    expect(await queue.moveJobToWaitingChildren(job.id)).toBe(true);
    expect(await queue.getJobState(job.id)).toBe('waiting-children');
  });
});
