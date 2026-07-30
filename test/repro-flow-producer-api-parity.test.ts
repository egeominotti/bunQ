import { afterEach, describe, expect, test } from 'bun:test';
import { FlowProducer, Queue, shutdownManager } from '../src/client';
import { closeAllSharedPools, type TcpConnectionPool } from '../src/client/tcpPool';

function snapshotsFor(command: Record<string, unknown>): Record<string, unknown>[] {
  return (command.jobs as Array<{ id: string; queue: string; input: object }>).map(
    ({ id, queue, input }) => ({
      id,
      queue,
      ...(input as object),
      createdAt: 1,
      runAt: 1,
      startedAt: null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 3,
      backoff: 1_000,
      backoffConfig: null,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      repeat: null,
      lastHeartbeat: 1,
      stallTimeout: null,
      stallCount: 0,
      stackTraceLimit: 10,
      keepLogs: null,
      sizeLimit: null,
      failParentOnFailure: false,
      removeDependencyOnFailure: false,
      continueParentOnFailure: false,
      ignoreDependencyOnFailure: false,
      deduplicationTtl: null,
      deduplicationExtend: false,
      deduplicationReplace: false,
      debounceId: null,
      debounceTtl: null,
      timeline: [],
      stacktrace: null,
    })
  );
}

afterEach(() => {
  shutdownManager();
  closeAllSharedPools();
});

describe('FlowProducer API parity', () => {
  test('returned jobs expose the committed metadata snapshot', async () => {
    const queueName = 'repro-flow-metadata';
    const queue = new Queue(queueName, { embedded: true });
    const producer = new FlowProducer({ embedded: true });
    queue.obliterate();

    try {
      const node = await producer.add({
        name: 'parent',
        queueName,
        data: { value: 1 },
        opts: {
          priority: 7,
          delay: 4_321,
          attempts: 5,
          jobId: 'flow-metadata-parent',
        },
      });
      const fetched = await queue.getJob(node.job.id);

      expect(fetched).not.toBeNull();
      expect(node.job.priority).toBe(fetched!.priority);
      expect(node.job.delay).toBe(fetched!.delay);
      expect(node.job.timestamp).toBe(fetched!.timestamp);
      expect(node.job.opts).toEqual(fetched!.opts);
      expect(node.job.toJSON()).toEqual(fetched!.toJSON());

      const tree = await producer.add({
        name: 'tree-parent',
        queueName,
        data: { public: 'parent' },
        children: [
          {
            name: 'tree-child',
            queueName,
            data: { public: 'child' },
          },
        ],
      });
      expect(tree.children![0].job.toJSON().data).toEqual({ public: 'child' });
      expect(tree.children![0].job.asJSON().data).toBe('{"public":"child"}');
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('TCP add sends one atomic flow command and no parent back-patch', async () => {
    const producer = new FlowProducer({
      embedded: false,
      connection: { host: '127.0.0.1', port: 56_790, poolSize: 1 },
    });
    const commands: Record<string, unknown>[] = [];
    const fakePool = {
      async send(command: Record<string, unknown>) {
        commands.push(command);
        return { ok: true, data: { jobs: snapshotsFor(command) } };
      },
      close() {},
    } as unknown as TcpConnectionPool;
    Object.defineProperty(producer, 'tcp', { value: fakePool });

    try {
      await producer.add({
        name: 'parent',
        queueName: 'tcp-flow-parent',
        data: {},
        children: [{ name: 'child', queueName: 'tcp-flow-child', data: {} }],
      });
      expect(commands.map((command) => command.cmd)).toEqual(['PUSHF']);
    } finally {
      await producer.close();
    }
  });

  test('TCP job methods surface broker errors instead of reporting success', async () => {
    const producer = new FlowProducer({
      embedded: false,
      connection: { host: '127.0.0.1', port: 56_791, poolSize: 1 },
    });
    const fakePool = {
      async send(command: Record<string, unknown>) {
        if (command.cmd === 'PUSHF') {
          return { ok: true, data: { jobs: snapshotsFor(command) } };
        }
        return { ok: false, error: `rejected ${String(command.cmd)}` };
      },
      close() {},
    } as unknown as TcpConnectionPool;
    Object.defineProperty(producer, 'tcp', { value: fakePool });

    try {
      await expect(producer.waitUntilReady()).rejects.toThrow('rejected Ping');
      const node = await producer.add({
        name: 'parent',
        queueName: 'tcp-flow-errors',
        data: {},
      });
      await expect(node.job.updateData({ changed: true })).rejects.toThrow('rejected Update');
      await expect(node.job.remove()).rejects.toThrow('rejected Cancel');
      await expect(node.job.extendLock('token', 1_000)).rejects.toThrow('rejected ExtendLock');
    } finally {
      await producer.close();
    }
  });

  test('TCP object progress uses the numeric wire field and serialized message', async () => {
    const producer = new FlowProducer({
      embedded: false,
      connection: { host: '127.0.0.1', port: 56_792, poolSize: 1 },
    });
    const commands: Record<string, unknown>[] = [];
    const fakePool = {
      async send(command: Record<string, unknown>) {
        commands.push(command);
        if (command.cmd === 'PUSHF') {
          return { ok: true, data: { jobs: snapshotsFor(command) } };
        }
        return { ok: true };
      },
      close() {},
    } as unknown as TcpConnectionPool;
    Object.defineProperty(producer, 'tcp', { value: fakePool });

    try {
      const node = await producer.add({
        name: 'parent',
        queueName: 'tcp-flow-progress',
        data: {},
      });
      await node.job.updateProgress({ phase: 'indexing', completed: 4 });
      expect(commands.at(-1)).toMatchObject({
        cmd: 'Progress',
        id: node.job.id,
        progress: 0,
        message: '{"phase":"indexing","completed":4}',
      });
    } finally {
      await producer.close();
    }
  });
});
