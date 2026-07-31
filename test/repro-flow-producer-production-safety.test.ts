import { afterEach, describe, expect, test } from 'bun:test';
import { FlowProducer, Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { pushJobWithParent, type PushContext } from '../src/client/flowPush';
import { closeAllSharedPools, type TcpConnectionPool } from '../src/client/tcpPool';
import { planFlows } from '../src/client/flowPlan';
import { jobId } from '../src/domain/types/job';

async function until(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`Condition not met within ${timeoutMs}ms`);
}

afterEach(() => {
  shutdownManager();
  closeAllSharedPools();
});

describe('FlowProducer production safety', () => {
  test('does not expose a child to workers before the whole flow is committed', async () => {
    const queueName = 'repro-flow-atomic-visibility';
    const queue = new Queue(queueName, { embedded: true });
    const producer = new FlowProducer({ embedded: true });
    queue.obliterate();

    let childStarted = false;
    let topologyWasCommitted = false;
    const worker = new Worker(
      queueName,
      async (job) => {
        if ((job.data as { role: string }).role === 'child') {
          const parent = await getSharedManager().getJob(jobId('atomic-flow-parent'));
          const child = await getSharedManager().getJob(jobId('atomic-flow-child'));
          topologyWasCommitted =
            parent?.childrenIds.map(String).includes('atomic-flow-child') === true &&
            String(child?.parentId) === 'atomic-flow-parent';
          childStarted = true;
        }
        return null;
      },
      { embedded: true }
    );

    const manager = getSharedManager();
    const originalPush = manager.push;
    manager.push = async (name, input) => {
      if ((input.data as { role?: string }).role === 'parent') await Bun.sleep(250);
      return originalPush.call(manager, name, input);
    };

    try {
      const adding = producer.add({
        name: 'parent',
        queueName,
        data: { role: 'parent' },
        opts: { jobId: 'atomic-flow-parent' },
        children: [
          {
            name: 'child',
            queueName,
            data: { role: 'child' },
            opts: { jobId: 'atomic-flow-child' },
          },
        ],
      });

      await until(() => childStarted);
      await adding;
      expect(topologyWasCommitted).toBe(true);
    } finally {
      manager.push = originalPush;
      await worker.close();
      await producer.close();
      await queue.close();
    }
  });

  test('addBulk validates the whole batch before publishing its first flow', async () => {
    const queueName = 'repro-flow-addbulk-rollback';
    const queue = new Queue(queueName, { embedded: true });
    const producer = new FlowProducer({ embedded: true });
    queue.obliterate();

    try {
      await expect(
        producer.addBulk([
          { name: 'first', queueName, data: { branch: 1 } },
          {
            name: 'second-parent',
            queueName,
            data: { branch: 2 },
            opts: { attempts: 0 },
            children: [{ name: 'second-child', queueName, data: { branch: 2 } }],
          },
        ])
      ).rejects.toThrow('attempts');

      const counts = await queue.getJobCountsAsync();
      expect(
        counts.waiting + counts.prioritized + counts.delayed + counts['waiting-children']
      ).toBe(0);
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('queue defaults cannot own a per-job flow identity', () => {
    expect(() =>
      planFlows([{ name: 'job', queueName: 'repro-flow-default-identity', data: {} }], {
        queuesOptions: {
          // @ts-expect-error Runtime guard protects JavaScript and untyped callers.
          'repro-flow-default-identity': { jobId: 'shared-default-id' },
        },
      })
    ).toThrow('jobId cannot be a queue default');
  });

  test('a pre-existing job cannot be silently reparented into a flow', async () => {
    const queueName = 'repro-flow-rollback-ownership';
    const queue = new Queue(queueName, { embedded: true });
    const producer = new FlowProducer({ embedded: true });
    queue.obliterate();

    const existing = await queue.add(
      'existing',
      { owner: 'outside-flow' },
      { jobId: 'existing-flow-child' }
    );

    try {
      await expect(
        producer.add({
          name: 'parent',
          queueName,
          data: {},
          children: [
            {
              name: 'child',
              queueName,
              data: {},
              opts: { jobId: existing.id },
            },
          ],
        })
      ).rejects.toThrow('already exists');

      expect(await queue.getJobState(existing.id)).toBe('waiting');
      expect((await queue.getJob(existing.id))?.data).toEqual({ owner: 'outside-flow' });
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('TCP parent back-patching rejects an explicit server error', async () => {
    const commands: Record<string, unknown>[] = [];
    const tcp = {
      async send(command: Record<string, unknown>) {
        commands.push(command);
        if (command.cmd === 'PUSH') return { ok: true, id: 'parent-id' };
        return { ok: false, error: 'child vanished before linkage' };
      },
    } as unknown as TcpConnectionPool;
    const ctx: PushContext = { embedded: false, tcp };

    await expect(
      pushJobWithParent(ctx, {
        queueName: 'parents',
        data: {},
        opts: {},
        parentRef: null,
        childIds: ['child-id'],
      })
    ).rejects.toThrow('child vanished before linkage');
    expect(commands.map((command) => command.cmd)).toEqual(['PUSH', 'UpdateParent']);
  });

  test('cross-queue dependency keys use each child queue', async () => {
    const producer = new FlowProducer({ embedded: true });
    const parentQueue = new Queue('repro-flow-deps-parent', { embedded: true });
    const childQueue = new Queue('repro-flow-deps-child', { embedded: true });
    parentQueue.obliterate();
    childQueue.obliterate();

    try {
      const node = await producer.add({
        name: 'parent',
        queueName: parentQueue.name,
        data: {},
        children: [{ name: 'child', queueName: childQueue.name, data: {} }],
      });
      const childId = node.children![0].job.id;

      expect(await node.job.getDependencies()).toEqual({
        processed: {},
        unprocessed: [`${childQueue.name}:${childId}`],
      });
    } finally {
      await producer.close();
      await parentQueue.close();
      await childQueue.close();
    }
  });

  test('getFlow maxChildren=0 returns no children', async () => {
    const queueName = 'repro-flow-zero-children';
    const producer = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    try {
      const node = await producer.add({
        name: 'parent',
        queueName,
        data: {},
        children: [{ name: 'child', queueName, data: {} }],
      });
      const fetched = await producer.getFlow({
        id: node.job.id,
        queueName,
        maxChildren: 0,
      });

      expect(fetched?.children).toBeUndefined();
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('getFlow rejects invalid traversal bounds', async () => {
    const producer = new FlowProducer({ embedded: true });
    try {
      await expect(producer.getFlow({ id: 'any', queueName: 'any', depth: -1 })).rejects.toThrow(
        'depth must be a non-negative integer'
      );
      await expect(
        producer.getFlow({ id: 'any', queueName: 'any', maxChildren: 1.5 })
      ).rejects.toThrow('maxChildren must be a non-negative integer');
    } finally {
      await producer.close();
    }
  });

  test('getFlow detects a corrupted cyclic topology', async () => {
    const queueName = 'repro-flow-cycle-read';
    const producer = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    try {
      const node = await producer.add({ name: 'root', queueName, data: {} });
      const stored = await getSharedManager().getJob(jobId(node.job.id));
      stored!.childrenIds = [stored!.id];

      await expect(producer.getFlow({ id: node.job.id, queueName })).rejects.toThrow(
        'contains a cycle'
      );
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('close is idempotent for a shared TCP pool', async () => {
    const connection = { host: '127.0.0.1', port: 56_789 };
    const first = new FlowProducer({ embedded: false, connection });
    const second = new FlowProducer({ embedded: false, connection });
    const pool = (first as unknown as { tcp: TcpConnectionPool }).tcp;
    const secondPool = (second as unknown as { tcp: TcpConnectionPool }).tcp;

    expect(secondPool).toBe(pool);
    try {
      await first.close();
      await first.close();
      expect(pool.isClosed()).toBe(false);
    } finally {
      await second.close();
    }
  });

  test('a missing child cannot be reported as successfully back-patched', async () => {
    const manager = getSharedManager();
    await expect(
      manager.updateJobParent(jobId('missing-flow-child'), jobId('missing-flow-parent'))
    ).rejects.toThrow('Child job not found');
  });
});
