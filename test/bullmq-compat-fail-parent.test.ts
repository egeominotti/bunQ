/**
 * BullMQ v5 Compatibility: failParentOnFailure
 *
 * When a child job fails terminally AND has failParentOnFailure: true,
 * the parent job should also be moved to the Failed state immediately.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('BullMQ v5 - failParentOnFailure', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('parent job moves to failed when child with failParentOnFailure fails terminally', async () => {
    const queueName = 'fail-parent-on-failure-test';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    let parentJobId: string | undefined;
    let childJobId: string | undefined;

    // Track when child finishes failing
    let resolveChildFailed: () => void;
    const childFailed = new Promise<void>((r) => (resolveChildFailed = r));

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          throw new Error('Child job intentionally failed');
        }
        // Parent should never be processed — it should fail before promotion
        return { done: true };
      },
      { embedded: true, concurrency: 5 }
    );

    worker.on('failed', (job) => {
      if (job && job.id === childJobId) {
        resolveChildFailed();
      }
    });

    // Create flow: parent with one child that has failParentOnFailure: true
    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: {
            failParentOnFailure: true,
            attempts: 1, // No retries — terminal failure on first attempt
          },
        },
      ],
    });

    parentJobId = result.job.id;
    childJobId = result.children![0].job.id;

    // Wait for the child to fail
    const timeoutPromise = new Promise<void>((_, reject) =>
      setTimeout(() => reject(new Error('Timed out waiting for child to fail')), 10000)
    );
    await Promise.race([childFailed, timeoutPromise]);

    // Allow time for failParentOnFailure propagation (async)
    await Bun.sleep(500);

    // Verify child is in failed state
    const childState = await queue.getJobState(childJobId!);
    expect(childState).toBe('failed');

    // Verify parent is also in failed state (moved to DLQ by failParentOnFailure)
    const parentState = await queue.getJobState(parentJobId!);
    expect(parentState).toBe('failed');

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});
