/**
 * Bug: FlowProducer TCP mode - children get parentId: 'pending' instead of real parent ID
 *
 * In flow.ts, children are created first with a temporary parentRef = { id: 'pending' }.
 * In embedded mode, updateJobParent() is called after parent creation to fix the parentId.
 * In TCP mode, this update step is MISSING because there's no UpdateParent TCP command.
 *
 * This test verifies that after creating a flow with children, ALL children
 * have the real parent's ID (not 'pending') in their data.__parentId field.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { FlowProducer, Queue, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { jobId } from '../src/domain/types/job';

describe('Bug: FlowProducer children should have real parentId (not pending)', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('embedded mode: children parentId should be real parent ID after flow.add()', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-parent-bug', { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'parent-job',
      queueName: 'flow-parent-bug',
      data: { role: 'parent' },
      children: [
        {
          name: 'child-a',
          queueName: 'flow-parent-bug',
          data: { role: 'child', idx: 1 },
        },
        {
          name: 'child-b',
          queueName: 'flow-parent-bug',
          data: { role: 'child', idx: 2 },
        },
      ],
    });

    const parentId = result.job.id;
    expect(parentId).toBeDefined();
    expect(parentId).not.toBe('pending');

    // Verify children have the real parent ID
    const children = result.children;
    expect(children).toBeDefined();
    expect(children!.length).toBe(2);

    const manager = getSharedManager();

    for (const childNode of children!) {
      const childJob = await manager.getJob(jobId(childNode.job.id));
      expect(childJob).not.toBeNull();

      const childData = childJob!.data as Record<string, unknown>;
      // This is the key assertion: parentId must be the real parent, not 'pending'
      expect(childData.__parentId).toBe(parentId);
      expect(childData.__parentId).not.toBe('pending');
    }

    flow.close();
    queue.close();
  });

  test('embedded mode: deeply nested flow children have correct parentIds', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-parent-deep', { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'root',
      queueName: 'flow-parent-deep',
      data: { level: 0 },
      children: [
        {
          name: 'mid',
          queueName: 'flow-parent-deep',
          data: { level: 1 },
          children: [
            {
              name: 'leaf',
              queueName: 'flow-parent-deep',
              data: { level: 2 },
            },
          ],
        },
      ],
    });

    const rootId = result.job.id;
    const midNode = result.children![0];
    const midId = midNode.job.id;
    const leafNode = midNode.children![0];
    const leafId = leafNode.job.id;

    const manager = getSharedManager();

    // Mid-level should have root as parent
    const midJob = await manager.getJob(jobId(midId));
    expect(midJob).not.toBeNull();
    const midData = midJob!.data as Record<string, unknown>;
    expect(midData.__parentId).toBe(rootId);
    expect(midData.__parentId).not.toBe('pending');

    // Leaf should have mid as parent
    const leafJob = await manager.getJob(jobId(leafId));
    expect(leafJob).not.toBeNull();
    const leafData = leafJob!.data as Record<string, unknown>;
    expect(leafData.__parentId).toBe(midId);
    expect(leafData.__parentId).not.toBe('pending');

    flow.close();
    queue.close();
  });
});
