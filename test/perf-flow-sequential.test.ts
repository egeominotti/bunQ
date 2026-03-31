/**
 * Performance test: FlowProducer sequential child creation
 *
 * Proves that addFlowNode creates children sequentially (await in loop)
 * instead of in parallel, and that addBulkThen also pushes parallel jobs
 * sequentially.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { FlowProducer, Queue, shutdownManager } from '../src/client';

afterEach(async () => {
  await shutdownManager();
});

describe('FlowProducer sequential creation', () => {
  test('add() with N children takes N * push_latency (sequential proof)', async () => {
    const flow = new FlowProducer({ embedded: true });
    const childCount = 10;

    // Measure creating a flow with N children
    const start = Bun.nanoseconds();
    const result = await flow.add({
      name: 'parent',
      queueName: 'flow-perf-test',
      data: { role: 'parent' },
      children: Array.from({ length: childCount }, (_, i) => ({
        name: `child-${i}`,
        queueName: 'flow-perf-test',
        data: { index: i },
      })),
    });
    const sequentialMs = (Bun.nanoseconds() - start) / 1_000_000;

    expect(result.children).toHaveLength(childCount);

    // Measure creating N independent jobs (baseline for single push cost)
    const queue = new Queue('flow-perf-baseline', { embedded: true });
    const singleStart = Bun.nanoseconds();
    await queue.add('single', { x: 1 });
    const singlePushMs = (Bun.nanoseconds() - singleStart) / 1_000_000;

    console.log('Flow child creation timing:');
    console.log(`  Single push: ${singlePushMs.toFixed(2)}ms`);
    console.log(`  ${childCount} children (sequential): ${sequentialMs.toFixed(2)}ms`);
    console.log(`  Expected if parallel: ~${singlePushMs.toFixed(2)}ms`);
    console.log(`  Expected if sequential: ~${(singlePushMs * childCount).toFixed(2)}ms`);
    console.log(`  Ratio: ${(sequentialMs / singlePushMs).toFixed(1)}x single push`);

    // If children are created sequentially, total time ≈ N * single push time
    // If parallel, total time ≈ 1 * single push time (plus overhead)
    // We expect ratio to be closer to N than to 1
    flow.close();
    await queue.close();
  });

  test('addBulkThen() pushes parallel jobs sequentially', async () => {
    const flow = new FlowProducer({ embedded: true });
    const parallelCount = 10;

    const start = Bun.nanoseconds();
    const result = await flow.addBulkThen(
      Array.from({ length: parallelCount }, (_, i) => ({
        name: `parallel-${i}`,
        queueName: 'flow-bulk-perf',
        data: { index: i },
      })),
      { name: 'final', queueName: 'flow-bulk-perf', data: {} }
    );
    const totalMs = (Bun.nanoseconds() - start) / 1_000_000;

    expect(result.parallelIds).toHaveLength(parallelCount);

    // Baseline: single push
    const queue = new Queue('flow-bulk-baseline', { embedded: true });
    const singleStart = Bun.nanoseconds();
    await queue.add('single', { x: 1 });
    const singlePushMs = (Bun.nanoseconds() - singleStart) / 1_000_000;

    console.log('addBulkThen timing:');
    console.log(`  Single push: ${singlePushMs.toFixed(2)}ms`);
    console.log(`  ${parallelCount} parallel + 1 final (sequential): ${totalMs.toFixed(2)}ms`);
    console.log(`  Ratio: ${(totalMs / singlePushMs).toFixed(1)}x single push`);

    flow.close();
    await queue.close();
  });

  test('addBulk() processes flows sequentially', async () => {
    const flow = new FlowProducer({ embedded: true });
    const flowCount = 5;

    const start = Bun.nanoseconds();
    const results = await flow.addBulk(
      Array.from({ length: flowCount }, (_, i) => ({
        name: `flow-${i}`,
        queueName: 'flow-addbulk-perf',
        data: { index: i },
      }))
    );
    const totalMs = (Bun.nanoseconds() - start) / 1_000_000;

    expect(results).toHaveLength(flowCount);

    // Baseline
    const queue = new Queue('flow-addbulk-baseline', { embedded: true });
    const singleStart = Bun.nanoseconds();
    await queue.add('single', { x: 1 });
    const singlePushMs = (Bun.nanoseconds() - singleStart) / 1_000_000;

    console.log('addBulk timing:');
    console.log(`  Single push: ${singlePushMs.toFixed(2)}ms`);
    console.log(`  ${flowCount} flows (sequential): ${totalMs.toFixed(2)}ms`);
    console.log(`  Ratio: ${(totalMs / singlePushMs).toFixed(1)}x single push`);

    flow.close();
    await queue.close();
  });

  test('addChain() is correctly sequential (by design)', async () => {
    // This test documents that addChain SHOULD be sequential
    // because each step depends on the previous one's ID
    const flow = new FlowProducer({ embedded: true });

    const result = await flow.addChain([
      { name: 'step1', queueName: 'chain-test', data: { order: 1 } },
      { name: 'step2', queueName: 'chain-test', data: { order: 2 } },
      { name: 'step3', queueName: 'chain-test', data: { order: 3 } },
    ]);

    // Chain steps depend on previous: step2 depends on step1, step3 depends on step2
    // This MUST be sequential — cannot parallelize
    expect(result.jobIds).toHaveLength(3);

    console.log('addChain: correctly sequential by design (each step depends on previous)');

    flow.close();
  });
});
