import { afterEach, describe, expect, test } from 'bun:test';
import { FlowProducer, Queue, shutdownManager, type FlowStep } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { closeAllSharedPools } from '../src/client/tcpPool';
import { jobId } from '../src/domain/types/job';

afterEach(() => {
  shutdownManager();
  closeAllSharedPools();
});

describe('FlowProducer validation boundaries', () => {
  test('the broker rejects malformed runtime fields before inserting a job', async () => {
    const queueName = 'repro-flow-runtime-validation';
    const queue = new Queue(queueName, { embedded: true });
    const manager = getSharedManager();
    queue.obliterate();

    try {
      await expect(
        manager.pushFlow({
          jobs: [
            {
              id: 42 as never,
              queue: queueName,
              input: { customId: '42', data: { name: 'malformed-id' } },
            },
          ],
        })
      ).rejects.toThrow('flow job id must be a string');
      await expect(
        manager.pushFlow({
          jobs: [
            {
              id: jobId('malformed-flow-boolean'),
              queue: queueName,
              input: { data: { name: 'malformed-option' }, lifo: 'yes' as never },
            },
          ],
        })
      ).rejects.toThrow('lifo must be a boolean');

      const counts = await queue.getJobCountsAsync();
      expect(
        counts.waiting + counts.prioritized + counts.delayed + counts['waiting-children']
      ).toBe(0);
    } finally {
      await queue.close();
    }
  });

  test('legacy addTree enforces the same bounded planning depth as add', async () => {
    const queueName = 'repro-flow-add-tree-depth';
    const producer = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();
    let root: FlowStep<Record<string, never>> = {
      name: 'level-101',
      queueName,
      data: {},
    };
    for (let level = 100; level >= 0; level--) {
      root = {
        name: `level-${level}`,
        queueName,
        data: {},
        children: [root],
      };
    }

    try {
      await expect(producer.addTree(root)).rejects.toThrow(
        'flow exceeds the 100 level depth limit'
      );
      const counts = await queue.getJobCountsAsync();
      expect(
        counts.waiting + counts.prioritized + counts.delayed + counts['waiting-children']
      ).toBe(0);
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('a removeOnComplete tombstone cannot make a reused flow id look complete', async () => {
    const queueName = 'repro-flow-completion-tombstone';
    const producer = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    const manager = getSharedManager();
    queue.obliterate();

    try {
      const first = await producer.add({
        name: 'first-generation',
        queueName,
        data: {},
        opts: { jobId: 'flow-completion-tombstone-id', removeOnComplete: true },
      });
      expect((await manager.pull(queueName))?.id).toBe(first.job.id);
      await manager.ack(jobId(first.job.id), { generation: 1 });

      await expect(
        producer.add({
          name: 'second-generation',
          queueName,
          data: {},
          opts: { jobId: first.job.id },
        })
      ).rejects.toThrow('already exists');
      expect(await queue.getJob(first.job.id)).toBeNull();
    } finally {
      await producer.close();
      await queue.close();
    }
  });

  test('getFlow rejects a descendant that does not point back to its parent', async () => {
    const queueName = 'repro-flow-reader-ownership';
    const producer = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    const manager = getSharedManager();
    queue.obliterate();

    try {
      const root = await producer.add({ name: 'root', queueName, data: {} });
      const unrelated = await producer.add({ name: 'unrelated', queueName, data: {} });
      const storedRoot = await manager.getJob(jobId(root.job.id));
      storedRoot!.childrenIds = [jobId(unrelated.job.id)];

      await expect(producer.getFlow({ id: root.job.id, queueName })).rejects.toThrow(
        'does not point back to parent'
      );
    } finally {
      await producer.close();
      await queue.close();
    }
  });
});
