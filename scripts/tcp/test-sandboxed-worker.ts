#!/usr/bin/env bun
/**
 * Test SandboxedWorker in TCP Mode
 * Tests crash-isolated job processing over TCP connection
 */

import { Queue, SandboxedWorker } from '../../src/client';
import { unlink } from 'fs/promises';

const QUEUE_NAME = 'tcp-test-sandboxed-worker';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test SandboxedWorker TCP Mode ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Create a temporary processor file
  const processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/tcp-sandboxed-processor-${Date.now()}.ts`;
  await Bun.write(
    processorPath,
    `
    export default async (job: { id: string; data: any; queue: string; progress: (n: number) => void }) => {
      job.progress(50);
      await Bun.sleep(10);
      job.progress(100);
      return { processed: true, value: job.data.value * 2 };
    };
  `
  );

  // Test 1: SandboxedWorker processes jobs via TCP
  console.log('1. Testing SandboxedWorker job processing over TCP...');
  try {
    const completedJobs: { id: string; result: unknown }[] = [];
    const activeJobs: string[] = [];

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: processorPath,
      concurrency: 2,
      timeout: 10000,
      connection: { port: TCP_PORT },
      heartbeatInterval: 5000,
    });

    worker.on('active', (job) => {
      activeJobs.push(String(job.id));
    });

    worker.on('completed', (job, result) => {
      completedJobs.push({ id: String(job.id), result });
    });

    worker.on('error', (err) => {
      console.log(`   Worker error: ${err.message}`);
    });

    await worker.start();

    // Push a job
    const job = await queue.add('multiply', { value: 21 });
    console.log(`   Pushed job: ${job.id}`);

    // Wait for processing
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (completedJobs.length > 0) break;
    }

    if (completedJobs.length === 1 && (completedJobs[0].result as { value: number }).value === 42) {
      console.log(`   ✅ Job processed correctly: value=${(completedJobs[0].result as { value: number }).value}`);
      passed++;
    } else {
      console.log(`   ❌ Job not processed correctly: ${JSON.stringify(completedJobs)}`);
      failed++;
    }

    if (activeJobs.length === 1) {
      console.log(`   ✅ Active event emitted`);
      passed++;
    } else {
      console.log(`   ❌ Active event not emitted (got ${activeJobs.length})`);
      failed++;
    }

    await worker.stop();
  } catch (e) {
    console.log(`   ❌ Error: ${e}`);
    failed++;
  }

  // Test 2: Multiple concurrent jobs
  console.log('\n2. Testing concurrent job processing over TCP...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const completedJobs: { id: string; result: unknown }[] = [];

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: processorPath,
      concurrency: 4,
      timeout: 10000,
      connection: { port: TCP_PORT },
    });

    worker.on('completed', (job, result) => {
      completedJobs.push({ id: String(job.id), result });
    });

    await worker.start();

    // Push 4 jobs
    await Promise.all([
      queue.add('multiply', { value: 1 }),
      queue.add('multiply', { value: 2 }),
      queue.add('multiply', { value: 3 }),
      queue.add('multiply', { value: 4 }),
    ]);

    // Wait for all jobs
    for (let i = 0; i < 100; i++) {
      await Bun.sleep(100);
      if (completedJobs.length >= 4) break;
    }

    if (completedJobs.length === 4) {
      const values = completedJobs
        .map((j) => (j.result as { value: number }).value)
        .sort((a, b) => a - b);
      if (JSON.stringify(values) === JSON.stringify([2, 4, 6, 8])) {
        console.log(`   ✅ All 4 jobs processed correctly: ${values}`);
        passed++;
      } else {
        console.log(`   ❌ Results incorrect: ${values}`);
        failed++;
      }
    } else {
      console.log(`   ❌ Not all jobs completed: ${completedJobs.length}/4`);
      failed++;
    }

    await worker.stop();
  } catch (e) {
    console.log(`   ❌ Error: ${e}`);
    failed++;
  }

  // Test 3: Error handling over TCP
  console.log('\n3. Testing error handling over TCP...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const errorProcessorPath = `${Bun.env.TMPDIR ?? '/tmp'}/tcp-error-processor-${Date.now()}.ts`;
    await Bun.write(
      errorProcessorPath,
      `
      export default async (job: any) => {
        throw new Error('TCP processor error');
      };
    `
    );

    const failedJobs: { id: string; error: string }[] = [];

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: errorProcessorPath,
      concurrency: 1,
      timeout: 5000,
      connection: { port: TCP_PORT },
    });

    worker.on('failed', (job, error) => {
      failedJobs.push({ id: String(job.id), error: error.message });
    });

    await worker.start();

    await queue.add('will-fail', { value: 1 });

    // Wait for failure
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (failedJobs.length > 0) break;
    }

    if (failedJobs.length === 1 && failedJobs[0].error.includes('TCP processor error')) {
      console.log(`   ✅ Error handled correctly: ${failedJobs[0].error}`);
      passed++;
    } else {
      console.log(`   ❌ Error not handled correctly: ${JSON.stringify(failedJobs)}`);
      failed++;
    }

    await worker.stop();
    try { await unlink(errorProcessorPath); } catch { /* ignore */ }
  } catch (e) {
    console.log(`   ❌ Error: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();
  queue.close();
  try { await unlink(processorPath); } catch { /* ignore */ }

  // Summary
  console.log(`\n=== Summary ===`);
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
