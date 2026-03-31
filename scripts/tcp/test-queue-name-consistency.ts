/**
 * Test: Queue Name Consistency
 *
 * Verifies that workers only process jobs from their registered queue.
 * This test prevents regression of the bug where Date.now() was called
 * twice, creating different queue names for worker and queue.
 *
 * Run: bun scripts/tcp/test-queue-name-consistency.ts
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const TIMEOUT_MS = 10000;

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
}

const results: TestResult[] = [];

function test(name: string, passed: boolean, error?: string) {
  results.push({ name, passed, error });
  const status = passed ? '✓' : '✗';
  console.log(`  ${status} ${name}${error ? `: ${error}` : ''}`);
}

async function main() {
  console.log('\n📋 Queue Name Consistency Tests\n');
  console.log('═'.repeat(50));

  // Test 1: Same queue name works
  console.log('\n1. Worker and Queue with same name');
  {
    const queueName = `test-same-name-${Date.now()}`;
    let processed = 0;

    const worker = new Worker(
      queueName,
      async () => {
        processed++;
        return { ok: true };
      },
      {
        connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
        concurrency: 1,
        heartbeatInterval: 0,
      }
    );

    const queue = new Queue(queueName, {
      connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
    });

    await sleep(200);

    // Push 5 jobs
    for (let i = 0; i < 5; i++) {
      await queue.add('job', { i });
    }

    // Wait for processing
    const start = Date.now();
    while (processed < 5 && Date.now() - start < TIMEOUT_MS) {
      await sleep(50);
    }

    test('Jobs processed with same queue name', processed === 5, processed === 5 ? undefined : `Only ${processed}/5 processed`);

    await worker.close();
    await queue.close();
  }

  // Test 2: Different queue names are isolated
  console.log('\n2. Worker and Queue with different names are isolated');
  {
    const workerQueueName = `test-worker-queue-${Date.now()}`;
    const pushQueueName = `test-push-queue-${Date.now() + 1}`; // Intentionally different

    let workerProcessed = 0;

    const worker = new Worker(
      workerQueueName,
      async () => {
        workerProcessed++;
        return { ok: true };
      },
      {
        connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
        concurrency: 1,
        heartbeatInterval: 0,
      }
    );

    const queue = new Queue(pushQueueName, {
      connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
    });

    await sleep(200);

    // Push 5 jobs to a different queue
    for (let i = 0; i < 5; i++) {
      await queue.add('job', { i });
    }

    // Wait a bit - worker should NOT process these
    await sleep(1000);

    test('Worker does not process jobs from different queue', workerProcessed === 0, workerProcessed === 0 ? undefined : `Unexpectedly processed ${workerProcessed} jobs`);

    await worker.close();
    await queue.close();
  }

  // Test 3: Using stored queue name (correct pattern)
  console.log('\n3. Stored queue name pattern (correct usage)');
  {
    const queueName = `test-stored-name-${Date.now()}`;
    let processed = 0;

    // Create both using the SAME variable (correct pattern)
    const worker = new Worker(
      queueName,
      async () => {
        processed++;
        return { ok: true };
      },
      {
        connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
        concurrency: 5,
        heartbeatInterval: 0,
      }
    );

    const queue = new Queue(queueName, {
      connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
    });

    await sleep(200);

    // Push 100 jobs
    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: 'job',
      data: { i },
    }));
    await queue.addBulk(jobs);

    // Wait for processing
    const start = Date.now();
    while (processed < 100 && Date.now() - start < TIMEOUT_MS) {
      await sleep(50);
    }

    test('All 100 jobs processed with stored queue name', processed === 100, processed === 100 ? undefined : `Only ${processed}/100 processed`);

    await worker.close();
    await queue.close();
  }

  // Test 4: Parallel workers on same queue
  console.log('\n4. Multiple workers on same queue');
  {
    const queueName = `test-parallel-workers-${Date.now()}`;
    let processed = 0;

    const workers = [];
    for (let w = 0; w < 3; w++) {
      workers.push(
        new Worker(
          queueName,
          async () => {
            processed++;
            return { ok: true };
          },
          {
            connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
            concurrency: 2,
            heartbeatInterval: 0,
          }
        )
      );
    }

    const queue = new Queue(queueName, {
      connection: { host: 'localhost', port: TCP_PORT, poolSize: 2 },
    });

    await sleep(300);

    // Push 50 jobs
    for (let i = 0; i < 50; i++) {
      await queue.add('job', { i });
    }

    // Wait for processing
    const start = Date.now();
    while (processed < 50 && Date.now() - start < TIMEOUT_MS) {
      await sleep(50);
    }

    test('All jobs processed by multiple workers', processed === 50, processed === 50 ? undefined : `Only ${processed}/50 processed`);

    for (const worker of workers) {
      await worker.close();
    }
    await queue.close();
  }

  // Summary
  console.log('\n' + '═'.repeat(50));
  const passed = results.filter((r) => r.passed).length;
  const total = results.length;
  console.log(`\nResults: ${passed}/${total} tests passed\n`);

  if (passed < total) {
    console.log('Failed tests:');
    for (const r of results.filter((r) => !r.passed)) {
      console.log(`  - ${r.name}: ${r.error}`);
    }
    process.exit(1);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error('Test failed:', err);
  process.exit(1);
});
