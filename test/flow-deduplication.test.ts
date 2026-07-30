/**
 * Flow + Deduplication Tests (Embedded Mode)
 *
 * Tests interaction between flow parent-child dependencies and
 * custom ID / deduplication logic to ensure they work correctly together.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('Flow + Deduplication - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('flow with an existing parent custom ID rejects a topology rewrite', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-parent', { embedded: true });
    queue.obliterate();

    // First flow with custom ID on parent
    const result1 = await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-parent',
      data: { role: 'parent' },
      opts: { jobId: 'dedup-parent-1' },
      children: [{ name: 'child-a', queueName: 'flow-dedup-parent', data: { role: 'child-a' } }],
    });

    await expect(
      flow.add({
        name: 'parent',
        queueName: 'flow-dedup-parent',
        data: { role: 'parent-dup' },
        opts: { jobId: 'dedup-parent-1' },
        children: [{ name: 'child-b', queueName: 'flow-dedup-parent', data: { role: 'child-b' } }],
      })
    ).rejects.toThrow('already exists');
    expect(((await queue.getJob(result1.job.id))?.data as { role: string }).role).toBe('parent');
    expect((await queue.getJobCountsAsync())['waiting-children']).toBe(1);

    await flow.close();
    queue.close();
  }, 15000);

  test('one custom-ID child cannot be owned by two parents', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-children', { embedded: true });
    queue.obliterate();

    // Add a flow with custom ID children
    const result1 = await flow.add({
      name: 'parent-1',
      queueName: 'flow-dedup-children',
      data: { role: 'parent' },
      children: [
        {
          name: 'child-dup',
          queueName: 'flow-dedup-children',
          data: { role: 'child', value: 1 },
          opts: { jobId: 'child-custom-1' },
        },
        {
          name: 'child-unique',
          queueName: 'flow-dedup-children',
          data: { role: 'child', value: 2 },
        },
      ],
    });

    // Add another flow where a child has the same custom ID
    await expect(
      flow.add({
        name: 'parent-2',
        queueName: 'flow-dedup-children',
        data: { role: 'parent-2' },
        children: [
          {
            name: 'child-dup',
            queueName: 'flow-dedup-children',
            data: { role: 'child', value: 99 },
            opts: { jobId: 'child-custom-1' },
          },
        ],
      })
    ).rejects.toThrow('already exists');

    const child1Id = result1.children![0].job.id;
    const uniqueChildId = result1.children![1].job.id;
    expect(uniqueChildId).not.toBe(child1Id);
    expect((await queue.getJob(child1Id))?.parent?.id).toBe(result1.job.id);

    await flow.close();
    queue.close();
  }, 15000);

  test('a standalone custom-ID job cannot be silently reparented', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-existing', { embedded: true });
    queue.obliterate();

    // Add a standalone job with a custom ID
    const standaloneJob = await queue.add(
      'standalone',
      { role: 'standalone' },
      { jobId: 'shared-custom-id' }
    );

    // Add a flow where a child uses the same custom ID
    await expect(
      flow.add({
        name: 'parent',
        queueName: 'flow-dedup-existing',
        data: { role: 'parent' },
        children: [
          {
            name: 'child',
            queueName: 'flow-dedup-existing',
            data: { role: 'child' },
            opts: { jobId: 'shared-custom-id' },
          },
        ],
      })
    ).rejects.toThrow('already exists');
    expect((await queue.getJob(standaloneJob.id))?.data).toEqual({ role: 'standalone' });

    await flow.close();
    queue.close();
  }, 15000);

  test('re-adding a parent custom ID rejects instead of orphaning new children', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-readd', { embedded: true });
    queue.obliterate();

    const result1 = await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-readd',
      data: { role: 'parent', attempt: 1 },
      opts: { jobId: 'readd-parent' },
      children: [
        { name: 'child-1', queueName: 'flow-dedup-readd', data: { role: 'child', idx: 1 } },
      ],
    });

    await expect(
      flow.add({
        name: 'parent',
        queueName: 'flow-dedup-readd',
        data: { role: 'parent', attempt: 2 },
        opts: { jobId: 'readd-parent' },
        children: [
          { name: 'child-2', queueName: 'flow-dedup-readd', data: { role: 'child', idx: 2 } },
        ],
      })
    ).rejects.toThrow('already exists');
    const originalData = (await queue.getJob(result1.job.id))?.data as {
      role: string;
      attempt: number;
    };
    expect({ role: originalData.role, attempt: originalData.attempt }).toEqual({
      role: 'parent',
      attempt: 1,
    });

    await flow.close();
    queue.close();
  }, 15000);

  test('flow with unique children custom IDs all created', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-unique', { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-unique',
      data: { role: 'parent' },
      children: [
        {
          name: 'child-a',
          queueName: 'flow-dedup-unique',
          data: { role: 'child', idx: 'a' },
          opts: { jobId: 'unique-child-a' },
        },
        {
          name: 'child-b',
          queueName: 'flow-dedup-unique',
          data: { role: 'child', idx: 'b' },
          opts: { jobId: 'unique-child-b' },
        },
        {
          name: 'child-c',
          queueName: 'flow-dedup-unique',
          data: { role: 'child', idx: 'c' },
          opts: { jobId: 'unique-child-c' },
        },
      ],
    });

    // All children should have distinct IDs
    expect(result.children).toHaveLength(3);
    const childIds = result.children!.map((c) => c.job.id);
    const uniqueIds = new Set(childIds);
    expect(uniqueIds.size).toBe(3);

    // Parent should be different from all children
    for (const childId of childIds) {
      expect(result.job.id).not.toBe(childId);
    }

    await flow.close();
    queue.close();
  }, 15000);

  test('deduplication does not break parent-child dependency tracking', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-deps', { embedded: true });
    queue.obliterate();

    const executionOrder: string[] = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-dedup-deps',
      async (job) => {
        const data = job.data as { role: string; idx?: number };
        executionOrder.push(`${data.role}-${data.idx ?? 0}`);
        if (data.role === 'parent') resolve!();
        await Bun.sleep(30);
        return { role: data.role, idx: data.idx };
      },
      { embedded: true, concurrency: 5 }
    );

    // Create flow with unique custom IDs on children
    await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-deps',
      data: { role: 'parent' },
      children: [
        {
          name: 'child-1',
          queueName: 'flow-dedup-deps',
          data: { role: 'child', idx: 1 },
          opts: { jobId: 'dep-child-1' },
        },
        {
          name: 'child-2',
          queueName: 'flow-dedup-deps',
          data: { role: 'child', idx: 2 },
          opts: { jobId: 'dep-child-2' },
        },
      ],
    });

    await done;
    await Bun.sleep(300);

    // Parent should execute after all children (dependency ordering preserved)
    const parentIdx = executionOrder.indexOf('parent-0');
    const child1Idx = executionOrder.indexOf('child-1');
    const child2Idx = executionOrder.indexOf('child-2');

    expect(parentIdx).toBeGreaterThan(-1);
    expect(child1Idx).toBeGreaterThan(-1);
    expect(child2Idx).toBeGreaterThan(-1);
    expect(child1Idx).toBeLessThan(parentIdx);
    expect(child2Idx).toBeLessThan(parentIdx);

    await worker.close();
    await flow.close();
    queue.close();
  }, 15000);
});
