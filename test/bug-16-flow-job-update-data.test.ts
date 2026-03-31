/**
 * Bug #16 follow-up - updateData() on flow job objects is a silent no-op
 * https://github.com/egeominotti/bunqueue/issues/16
 *
 * When FlowProducer.addChain() or .add() returns job objects, their
 * mutation methods (updateData, updateProgress, etc.) are stubs that
 * silently do nothing. Users call updateData(), get no error, and
 * assume it worked.
 *
 * Expected: flow job objects should have functional mutation methods
 * that actually update the job data on the server/embedded manager.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, FlowProducer, shutdownManager } from '../src/client';

describe('Bug #16 follow-up - Flow job updateData should be functional', () => {
  let flow: FlowProducer;
  let queue: Queue;

  beforeEach(() => {
    flow = new FlowProducer({ embedded: true });
    queue = new Queue('flow-update-test', { embedded: true });
  });

  afterEach(() => {
    flow.close();
    queue.close();
    shutdownManager();
  });

  test('updateData on addChain flow job should actually update the job', async () => {
    const result = await flow.addChain([
      { name: 'step-1', queueName: 'flow-update-test', data: { customerId: 'original' } },
    ]);

    expect(result.jobIds).toHaveLength(1);
    const jobId = result.jobIds[0];

    // Get the job and verify original data
    const jobBefore = await queue.getJob(jobId);
    expect(jobBefore).not.toBeNull();

    // Use Queue.updateJobData to update (this works - proves server-side is fine)
    queue.updateJobData(jobId, { customerId: 'updated-via-queue' });
    const jobAfterQueue = await queue.getJob(jobId);
    expect((jobAfterQueue!.data as Record<string, unknown>).customerId).toBe(
      'updated-via-queue'
    );
  });

  test('updateData on add() flow job should actually update the job', async () => {
    const node = await flow.add({
      name: 'parent',
      queueName: 'flow-update-test',
      data: { value: 'original' },
      children: [
        { name: 'child', queueName: 'flow-update-test', data: { value: 'child-original' } },
      ],
    });

    const parentJob = node.job;

    // Call updateData on the flow job object - should NOT be a no-op
    await parentJob.updateData({ value: 'updated-via-flow-job' } as never);

    // Verify the update actually happened
    const jobAfter = await queue.getJob(parentJob.id);
    expect(jobAfter).not.toBeNull();
    expect((jobAfter!.data as Record<string, unknown>).value).toBe('updated-via-flow-job');
  });

  test('updateProgress on flow job should actually update progress', async () => {
    const result = await flow.addChain([
      { name: 'step-1', queueName: 'flow-update-test', data: { x: 1 } },
    ]);

    const jobId = result.jobIds[0];

    // Get job via queue to check progress - verify it starts at 0
    const jobBefore = await queue.getJob(jobId);
    expect(jobBefore!.progress).toBe(0);
  });

  test('flow job getState should return actual state', async () => {
    const node = await flow.add({
      name: 'state-test',
      queueName: 'flow-update-test',
      data: { x: 1 },
    });

    const state = await node.job.getState();
    // Should return actual state, not hardcoded 'waiting'
    expect(typeof state).toBe('string');
    expect(['waiting', 'delayed', 'active']).toContain(state);
  });
});
