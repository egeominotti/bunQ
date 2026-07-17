import { afterEach, expect, test } from 'bun:test';
import { FlowProducer, Queue, shutdownManager } from '../src/client';

const QUEUE = 'repro-model-obliterate-dependencies';

afterEach(() => {
  shutdownManager();
});

test('obliterate removes waiting dependency jobs from every live index and count', async () => {
  const queue = new Queue(QUEUE, { embedded: true });
  const flow = new FlowProducer({ embedded: true });

  try {
    queue.obliterate();
    await flow.add({
      children: [{ data: { role: 'child' }, name: 'child', queueName: QUEUE }],
      data: { role: 'parent' },
      name: 'parent',
      queueName: QUEUE,
    });

    expect(queue.getJobs({ end: 100, start: 0, state: 'waiting-children' })).toHaveLength(1);

    queue.obliterate();

    expect(queue.getJobs({ end: 100, start: 0 })).toHaveLength(0);
    expect(queue.getJobs({ end: 100, start: 0, state: 'waiting-children' })).toHaveLength(0);
    expect(queue.getJobCounts()).toMatchObject({
      active: 0,
      completed: 0,
      delayed: 0,
      failed: 0,
      prioritized: 0,
      waiting: 0,
      'waiting-children': 0,
    });
  } finally {
    await flow.close();
    queue.close();
  }
});
