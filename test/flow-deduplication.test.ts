/**
 * Flow + Deduplication Tests (Embedded Mode)
 *
 * Tests interaction between flow parent-child dependencies and
 * custom ID / deduplication logic to ensure they work correctly together.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('Flow + Deduplication - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('flow with custom ID on parent - parent is deduplicated', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-parent', { embedded: true });
    queue.obliterate();

    // First flow with custom ID on parent
    const result1 = await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-parent',
      data: { role: 'parent' },
      opts: { jobId: 'dedup-parent-1' },
      children: [
        { name: 'child-a', queueName: 'flow-dedup-parent', data: { role: 'child-a' } },
      ],
    });

    // Second flow with same custom ID on parent - should get existing parent
    const result2 = await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-parent',
      data: { role: 'parent-dup' },
      opts: { jobId: 'dedup-parent-1' },
      children: [
        { name: 'child-b', queueName: 'flow-dedup-parent', data: { role: 'child-b' } },
      ],
    });

    // The parent from result2 should have the same ID as result1 (deduplicated)
    expect(result2.job.id).toBe(result1.job.id);

    // Children should have different IDs (they don't share custom IDs)
    expect(result1.children![0].job.id).not.toBe(result2.children![0].job.id);

    flow.close();
    queue.close();
  }, 15000);

  test('flow with custom ID on children - duplicate children are deduplicated', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-dedup-children', { embedded: true });
    queue.obliterate();

    const manager = getSharedManager();

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
    const result2 = await flow.add({
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
    });

    // The child with same custom ID should have the same job ID
    const child1Id = result1.children![0].job.id;
    const child2Id = result2.children![0].job.id;
    expect(child2Id).toBe(child1Id);

    // The unique child should be different
    const uniqueChildId = result1.children![1].job.id;
    expect(uniqueChildId).not.toBe(child1Id);

    flow.close();
    queue.close();
  }, 15000);

  test('flow child with same custom ID as existing non-flow job', async () => {
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
    const flowResult = await flow.add({
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
    });

    // The child should get the same ID as the standalone job (deduplicated)
    expect(flowResult.children![0].job.id).toBe(standaloneJob.id);

    flow.close();
    queue.close();
  }, 15000);

  test('re-adding flow with same parent custom ID returns existing', async () => {
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

    const result2 = await flow.add({
      name: 'parent',
      queueName: 'flow-dedup-readd',
      data: { role: 'parent', attempt: 2 },
      opts: { jobId: 'readd-parent' },
      children: [
        { name: 'child-2', queueName: 'flow-dedup-readd', data: { role: 'child', idx: 2 } },
      ],
    });

    // Parent IDs should match (deduplicated)
    expect(result2.job.id).toBe(result1.job.id);

    flow.close();
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

    flow.close();
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
    flow.close();
    queue.close();
  }, 15000);
});
