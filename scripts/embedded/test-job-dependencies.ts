#!/usr/bin/env bun
/**
 * Test Job Dependencies: parent-child, dependsOn
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker, FlowProducer } from '../../src/client';

const QUEUE_NAME = 'test-dependencies';

async function main() {
  console.log('=== Test Job Dependencies ===\n');

  const queue = new Queue<{ step: string }>(QUEUE_NAME, { embedded: true });
  const flow = new FlowProducer();
  let passed = 0;
  let failed = 0;

  // Test 1: Add chain of dependent jobs
  console.log('1. Testing CHAIN DEPENDENCIES...');
  try {
    const result = await flow.addChain([
      { name: 'step-1', queueName: QUEUE_NAME, data: { step: 'first' } },
      { name: 'step-2', queueName: QUEUE_NAME, data: { step: 'second' } },
      { name: 'step-3', queueName: QUEUE_NAME, data: { step: 'third' } },
    ]);

    if (result.jobIds.length === 3) {
      console.log(`   ✅ Chain created with ${result.jobIds.length} jobs`);
      passed++;
    } else {
      console.log(`   ❌ Expected 3 jobs, got ${result.jobIds.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Chain creation failed: ${e}`);
    failed++;
  }

  // Test 2: Process chain jobs
  console.log('\n2. Testing CHAIN EXECUTION...');
  try {
    const executed: string[] = [];
    const worker = new Worker<{ step: string }>(
      QUEUE_NAME,
      async (job) => {
        executed.push((job.data as { step: string }).step);
        await Bun.sleep(50); // Simulate work
        return { completed: (job.data as { step: string }).step };
      },
      { concurrency: 1, embedded: true }
    );

    // Wait for all jobs (longer timeout for dependency resolution)
    await Bun.sleep(3000);
    await worker.close();

    if (JSON.stringify(executed) === JSON.stringify(['first', 'second', 'third'])) {
      console.log(`   ✅ Chain executed exactly once and in order: ${executed.join(' -> ')}`);
      passed++;
    } else {
      console.log(`   ❌ Expected first -> second -> third, got: ${executed.join(' -> ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Chain execution failed: ${e}`);
    failed++;
  }

  // Test 3: Bulk then (fan-out fan-in)
  console.log('\n3. Testing BULK THEN (Fan-out Fan-in)...');
  try {
    const result = await flow.addBulkThen(
      [
        { name: 'parallel-1', queueName: QUEUE_NAME, data: { step: 'p1' } },
        { name: 'parallel-2', queueName: QUEUE_NAME, data: { step: 'p2' } },
        { name: 'parallel-3', queueName: QUEUE_NAME, data: { step: 'p3' } },
      ],
      { name: 'final', queueName: QUEUE_NAME, data: { step: 'final' } }
    );

    if (result.parallelIds.length === 3 && result.finalId) {
      console.log(`   ✅ Bulk-then created: ${result.parallelIds.length} parallel + 1 final`);
      passed++;
    } else {
      console.log(`   ❌ Unexpected result: ${JSON.stringify(result)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk-then creation failed: ${e}`);
    failed++;
  }

  // Test 4: Process bulk-then
  console.log('\n4. Testing BULK THEN EXECUTION...');
  try {
    const parallelCompleted: string[] = [];
    let finalExecuted = false;

    const worker = new Worker<{ step: string }>(
      QUEUE_NAME,
      (job) => {
        if (job.data.step.startsWith('p')) {
          parallelCompleted.push(job.data.step);
        } else if (job.data.step === 'final') {
          finalExecuted = true;
        }
        return { done: job.data.step };
      },
      { concurrency: 5, embedded: true }
    );

    await Bun.sleep(1000);
    await worker.close();

    if (parallelCompleted.length === 3 && finalExecuted) {
      console.log(`   ✅ All parallel jobs (${parallelCompleted.join(', ')}) + final executed`);
      passed++;
    } else {
      console.log(`   ❌ Parallel: ${parallelCompleted.length}, Final: ${finalExecuted}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk-then execution failed: ${e}`);
    failed++;
  }

  // Test 5: Tree structure
  console.log('\n5. Testing TREE STRUCTURE...');
  try {
    const result = await flow.addTree({
      name: 'root',
      queueName: QUEUE_NAME,
      data: { step: 'root' },
      children: [
        {
          name: 'child-1',
          queueName: QUEUE_NAME,
          data: { step: 'child-1' },
          children: [{ name: 'grandchild-1', queueName: QUEUE_NAME, data: { step: 'gc-1' } }],
        },
        { name: 'child-2', queueName: QUEUE_NAME, data: { step: 'child-2' } },
      ],
    });

    if (result.jobIds.length === 4) {
      console.log(`   ✅ Tree created with ${result.jobIds.length} nodes`);
      passed++;
    } else {
      console.log(`   ❌ Expected 4 nodes, got ${result.jobIds.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Tree creation failed: ${e}`);
    failed++;
  }

  // Test 6: Process tree
  console.log('\n6. Testing TREE EXECUTION...');
  try {
    const executed: string[] = [];
    const worker = new Worker<{ step: string }>(
      QUEUE_NAME,
      (job) => {
        executed.push((job.data as { step: string }).step);
        return { done: (job.data as { step: string }).step };
      },
      { concurrency: 1, embedded: true }
    );

    await Bun.sleep(2000);
    await worker.close();

    // All 4 nodes should execute (order depends on dependency resolution)
    if (executed.length === 4) {
      console.log(`   ✅ Tree executed: ${executed.join(' -> ')}`);
      passed++;
    } else {
      console.log(
        `   ❌ Expected 4 nodes, executed: ${executed.length} (${executed.join(' -> ')})`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Tree execution failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
