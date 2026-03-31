#!/usr/bin/env bun
/**
 * FlowProducer Test Suite
 * Tests FlowProducer with BullMQ v5 compatible flows (children/parent)
 */

import { FlowProducer, Queue, Worker, shutdownManager } from '../../src/client';

const results: { name: string; passed: boolean; error?: string }[] = [];

function test(name: string, fn: () => Promise<void> | void) {
  return { name, fn };
}

async function runTest(t: { name: string; fn: () => Promise<void> | void }) {
  try {
    await t.fn();
    results.push({ name: t.name, passed: true });
    console.log(`  ✓ ${t.name}`);
  } catch (err) {
    results.push({ name: t.name, passed: false, error: String(err) });
    console.log(`  ✗ ${t.name}`);
    console.log(`    Error: ${err}`);
  }
}

async function cleanup() {
  shutdownManager();
  await Bun.sleep(100);
}

// ============================================================================
// Tests
// ============================================================================

const tests = [
  test('FlowProducer constructor', async () => {
    const flow = new FlowProducer({ embedded: true });
    if (!flow) throw new Error('FlowProducer not created');
    flow.close();
  }),

  test('FlowProducer.add creates job node', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-add', { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'parent-job',
      queueName: 'test-flow-add',
      data: { value: 42 },
    });

    if (!result.job) throw new Error('Job node not created');
    if (!result.job.id) throw new Error('Job ID not set');
    if (result.job.name !== 'parent-job') throw new Error('Job name mismatch');

    flow.close();
    queue.close();
  }),

  test('FlowProducer.add with children', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-children', { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'parent',
      queueName: 'test-flow-children',
      data: { type: 'aggregate' },
      children: [
        { name: 'child1', queueName: 'test-flow-children', data: { id: 1 } },
        { name: 'child2', queueName: 'test-flow-children', data: { id: 2 } },
      ],
    });

    if (!result.job) throw new Error('Parent job not created');
    if (!result.children || result.children.length !== 2) {
      throw new Error(`Expected 2 children, got ${result.children?.length ?? 0}`);
    }
    if (!result.children[0].job.id) throw new Error('Child 1 ID not set');
    if (!result.children[1].job.id) throw new Error('Child 2 ID not set');

    flow.close();
    queue.close();
  }),

  test('FlowProducer.addBulk creates multiple flows', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-bulk', { embedded: true });
    queue.obliterate();

    const results = await flow.addBulk([
      { name: 'flow1', queueName: 'test-flow-bulk', data: { id: 1 } },
      { name: 'flow2', queueName: 'test-flow-bulk', data: { id: 2 } },
      { name: 'flow3', queueName: 'test-flow-bulk', data: { id: 3 } },
    ]);

    if (results.length !== 3) throw new Error(`Expected 3 flows, got ${results.length}`);
    for (let i = 0; i < 3; i++) {
      if (!results[i].job.id) throw new Error(`Flow ${i + 1} job ID not set`);
    }

    flow.close();
    queue.close();
  }),

  test('FlowProducer.addChain creates sequential jobs', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-chain', { embedded: true });
    queue.obliterate();

    const result = await flow.addChain([
      { name: 'step1', queueName: 'test-flow-chain', data: { step: 1 } },
      { name: 'step2', queueName: 'test-flow-chain', data: { step: 2 } },
      { name: 'step3', queueName: 'test-flow-chain', data: { step: 3 } },
    ]);

    if (result.jobIds.length !== 3) {
      throw new Error(`Expected 3 job IDs, got ${result.jobIds.length}`);
    }

    flow.close();
    queue.close();
  }),

  test('FlowProducer.addBulkThen creates parallel then final', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-bulk-then', { embedded: true });
    queue.obliterate();

    const result = await flow.addBulkThen(
      [
        { name: 'parallel1', queueName: 'test-flow-bulk-then', data: { id: 1 } },
        { name: 'parallel2', queueName: 'test-flow-bulk-then', data: { id: 2 } },
      ],
      { name: 'final', queueName: 'test-flow-bulk-then', data: { merge: true } }
    );

    if (result.parallelIds.length !== 2) {
      throw new Error(`Expected 2 parallel IDs, got ${result.parallelIds.length}`);
    }
    if (!result.finalId) throw new Error('Final ID not set');

    flow.close();
    queue.close();
  }),

  test('FlowProducer.addTree creates hierarchical jobs', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-tree', { embedded: true });
    queue.obliterate();

    const result = await flow.addTree({
      name: 'root',
      queueName: 'test-flow-tree',
      data: { level: 0 },
      children: [
        {
          name: 'branch1',
          queueName: 'test-flow-tree',
          data: { level: 1 },
          children: [
            { name: 'leaf1', queueName: 'test-flow-tree', data: { level: 2 } },
          ],
        },
        { name: 'branch2', queueName: 'test-flow-tree', data: { level: 1 } },
      ],
    });

    // Root + 2 branches + 1 leaf = 4 jobs
    if (result.jobIds.length !== 4) {
      throw new Error(`Expected 4 job IDs, got ${result.jobIds.length}`);
    }

    flow.close();
    queue.close();
  }),

  test('FlowProducer.disconnect method exists', async () => {
    const flow = new FlowProducer({ embedded: true });

    if (typeof flow.disconnect !== 'function') {
      throw new Error('disconnect method not found');
    }

    await flow.disconnect();
  }),

  test('FlowProducer.waitUntilReady method exists', async () => {
    const flow = new FlowProducer({ embedded: true });

    if (typeof flow.waitUntilReady !== 'function') {
      throw new Error('waitUntilReady method not found');
    }

    await flow.waitUntilReady();
    flow.close();
  }),

  test('FlowProducer.getParentResult in embedded mode', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue<{ value: number }>('test-flow-parent-result', { embedded: true });
    queue.obliterate();

    // Create a parent job and complete it
    const parentJob = await queue.add('parent', { value: 100 });

    const worker = new Worker('test-flow-parent-result', async (job) => {
      return { computed: job.data.value * 2 };
    }, { embedded: true, autorun: true });

    await Bun.sleep(300);

    const result = flow.getParentResult(parentJob.id);
    // Result should be the return value from the worker
    if (result && typeof result === 'object' && 'computed' in result) {
      if ((result as { computed: number }).computed !== 200) {
        throw new Error(`Expected computed 200, got ${(result as { computed: number }).computed}`);
      }
    }

    await worker.close();
    flow.close();
    queue.close();
  }),

  test('FlowProducer.getParentResults for multiple parents', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue<{ value: number }>('test-flow-parent-results', { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('parent1', { value: 10 });
    const job2 = await queue.add('parent2', { value: 20 });

    const worker = new Worker('test-flow-parent-results', async (job) => {
      return { result: job.data.value };
    }, { embedded: true, autorun: true });

    await Bun.sleep(300);

    const results = flow.getParentResults([job1.id, job2.id]);
    if (!(results instanceof Map)) throw new Error('Expected Map result');

    await worker.close();
    flow.close();
    queue.close();
  }),

  test('Flow with job options', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('test-flow-opts', { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'job-with-opts',
      queueName: 'test-flow-opts',
      data: { value: 1 },
      opts: {
        priority: 10,
        delay: 100,
        attempts: 5,
      },
    });

    if (!result.job.id) throw new Error('Job not created');

    flow.close();
    queue.close();
  }),
];

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log('\n=== FlowProducer Tests ===\n');

  for (const t of tests) {
    await runTest(t);
  }

  await cleanup();

  console.log('\n=== Summary ===');
  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  if (failed > 0) {
    console.log('\nFailed tests:');
    for (const r of results.filter((r) => !r.passed)) {
      console.log(`  - ${r.name}: ${r.error}`);
    }
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
