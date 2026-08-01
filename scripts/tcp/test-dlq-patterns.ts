#!/usr/bin/env bun
/**
 * DLQ Patterns Tests (TCP Mode)
 *
 * Tests covering core DLQ behaviors over TCP:
 * 1. Failed job goes to DLQ
 * 2. DLQ with different error types
 * 3. Retry from DLQ
 * 4. Purge DLQ
 * 5. Multiple failures accumulate in DLQ
 *
 * Uses acknowledged TCP commands and exact broker-side counts throughout.
 */

import { Queue, Worker } from '../../src/client';
import { TcpConnectionPool } from '../../src/client/tcpPool';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   [PASS] ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   [FAIL] ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

/** Send a raw Dlq command to the server and return the jobs array */
async function queryDlq(pool: TcpConnectionPool, queue: string, count?: number) {
  const res = await pool.send({ cmd: 'Dlq', queue, count });
  return (res.jobs as Array<Record<string, unknown>>) ?? [];
}

/** Send a raw RetryDlq command and return the count */
async function sendRetryDlq(pool: TcpConnectionPool, queue: string) {
  const res = await pool.send({ cmd: 'RetryDlq', queue });
  return (res.count as number) ?? 0;
}

/** Send a raw PurgeDlq command and return the count */
async function sendPurgeDlq(pool: TcpConnectionPool, queue: string) {
  const res = await pool.send({ cmd: 'PurgeDlq', queue });
  return (res.count as number) ?? 0;
}

async function main() {
  console.log('=== DLQ Patterns Tests (TCP) ===\n');

  // Create a raw TCP pool for direct DLQ queries
  const rawPool = new TcpConnectionPool({
    host: 'localhost',
    port: TCP_PORT,
    poolSize: 2,
  });

  // ─────────────────────────────────────────────────
  // Test 1: Failed job goes to DLQ
  // Push a job that throws with attempts:1, verify it appears in DLQ
  // ─────────────────────────────────────────────────
  console.log('1. Testing FAILED JOB GOES TO DLQ...');
  {
    const qName = 'tcp-dlq-pat-failed';
    const q = makeQueue(qName);
    await q.obliterateAsync();

    await q.add('will-fail', { value: 42 }, { attempts: 1, backoff: 0 });

    let failCount = 0;
    const worker = new Worker(
      qName,
      async () => {
        throw new Error('intentional failure');
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('failed', () => {
      failCount++;
    });

    // Wait for the job to fail and land in DLQ
    for (let i = 0; i < 60; i++) {
      if (failCount >= 1) break;
      await Bun.sleep(100);
    }
    await worker.close();

    // Query DLQ via raw TCP command
    const dlqJobs = await queryDlq(rawPool, qName);

    if (failCount === 1 && dlqJobs.length === 1) {
      ok('The failed job appeared exactly once in the DLQ');
    } else {
      fail(`Job did not fail (failCount=${failCount}, dlq=${dlqJobs.length})`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: DLQ with different error types
  // 3 jobs fail with different errors, verify all in DLQ
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing DLQ WITH DIFFERENT ERROR TYPES...');
  {
    const qName = 'tcp-dlq-pat-errors';
    const q = makeQueue(qName);
    await q.obliterateAsync();

    const errorMessages = [
      'database connection timeout',
      'invalid input format',
      'rate limit exceeded',
    ];

    for (const msg of errorMessages) {
      await q.add('error-job', { errorType: msg }, { attempts: 1, backoff: 0 });
    }

    let failCount = 0;
    const worker = new Worker(
      qName,
      async (job) => {
        const data = job.data as { errorType: string };
        throw new Error(data.errorType);
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('failed', () => {
      failCount++;
    });

    // Wait for all 3 to fail
    for (let i = 0; i < 60; i++) {
      if (failCount >= 3) break;
      await Bun.sleep(100);
    }
    await worker.close();

    // Query DLQ via raw TCP
    const dlqJobs = await queryDlq(rawPool, qName);

    if (failCount === 3 && dlqJobs.length === 3) {
      ok('All 3 jobs with different errors entered the DLQ exactly once');
    } else {
      fail(`Expected 3 failures, got failCount=${failCount}, dlq=${dlqJobs.length}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 3: Retry from DLQ
  // Job fails to DLQ, call retryDlq(), verify job is retried
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing RETRY FROM DLQ...');
  {
    const qName = 'tcp-dlq-pat-retry';
    const q = makeQueue(qName);
    await q.obliterateAsync();

    await q.add('retry-me', { key: 'retry-test' }, { attempts: 1, backoff: 0 });

    // First worker: always fails
    let failCount = 0;
    const worker1 = new Worker(
      qName,
      async () => {
        throw new Error('fail on first pass');
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker1.on('failed', () => {
      failCount++;
    });

    // Wait for job to fail
    for (let i = 0; i < 60; i++) {
      if (failCount >= 1) break;
      await Bun.sleep(100);
    }
    await worker1.close();

    // Verify job is in DLQ
    const dlqBefore = await queryDlq(rawPool, qName);

    // Retry from DLQ
    const retried = await sendRetryDlq(rawPool, qName);

    // Second worker: succeeds
    let processedCount = 0;
    const worker2 = new Worker(
      qName,
      async () => {
        processedCount++;
        return { success: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for retried job to be processed
    for (let i = 0; i < 60; i++) {
      if (processedCount >= 1) break;
      await Bun.sleep(100);
    }
    await worker2.close();

    // DLQ should be empty after retry
    const dlqAfter = await queryDlq(rawPool, qName);

    if (dlqBefore.length === 1 && retried === 1 && processedCount === 1 && dlqAfter.length === 0) {
      ok('One DLQ job was retried, completed once, and removed from the DLQ');
    } else {
      fail(
        `Retry from DLQ failed (retried=${retried}, processed=${processedCount}, dlqBefore=${dlqBefore.length})`
      );
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Purge DLQ
  // 5 jobs fail to DLQ, call purgeDlq(), verify DLQ empty
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing PURGE DLQ...');
  {
    const qName = 'tcp-dlq-pat-purge';
    const q = makeQueue(qName);
    await q.obliterateAsync();

    // Push 5 jobs that will all fail
    for (let i = 0; i < 5; i++) {
      await q.add('purge-job', { idx: i }, { attempts: 1, backoff: 0 });
    }

    let failCount = 0;
    const worker = new Worker(
      qName,
      async () => {
        throw new Error('always fails');
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('failed', () => {
      failCount++;
    });

    // Wait for all 5 to fail
    for (let i = 0; i < 60; i++) {
      if (failCount >= 5) break;
      await Bun.sleep(100);
    }
    await worker.close();

    // Query DLQ before purge
    const dlqBefore = await queryDlq(rawPool, qName);

    // Purge the DLQ
    const purged = await sendPurgeDlq(rawPool, qName);

    // Query DLQ after purge
    const dlqAfter = await queryDlq(rawPool, qName);

    if (failCount === 5 && dlqBefore.length === 5 && purged === 5 && dlqAfter.length === 0) {
      ok('Purge removed exactly all 5 DLQ entries');
    } else {
      fail(
        `Purge DLQ failed (failCount=${failCount}, dlqBefore=${dlqBefore.length}, purged=${purged}, dlqAfter=${dlqAfter.length})`
      );
    }
  }

  // ─────────────────────────────────────────────────
  // Test 5: Multiple failures accumulate in DLQ
  // 10 failing jobs, verify all 10 in DLQ
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing MULTIPLE FAILURES ACCUMULATE...');
  {
    const qName = 'tcp-dlq-pat-accumulate';
    const q = makeQueue(qName);
    await q.obliterateAsync();

    for (let i = 0; i < 10; i++) {
      await q.add(`fail-${i}`, { seq: i }, { attempts: 1, backoff: 0 });
    }

    let failCount = 0;
    const worker = new Worker(
      qName,
      async () => {
        throw new Error('sequential failure');
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('failed', () => {
      failCount++;
    });

    // Wait for all 10 to fail
    for (let i = 0; i < 100; i++) {
      if (failCount >= 10) break;
      await Bun.sleep(100);
    }
    await worker.close();

    // Query DLQ
    const dlqJobs = await queryDlq(rawPool, qName);

    if (failCount === 10 && dlqJobs.length === 10) {
      ok('All 10 failures accumulated exactly once in the DLQ');
    } else {
      fail(`Expected 10 failures, got failCount=${failCount}, dlq=${dlqJobs.length}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Cleanup
  // ─────────────────────────────────────────────────
  for (const q of queues) {
    await q.obliterateAsync();
    q.close();
  }
  rawPool.close();

  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
