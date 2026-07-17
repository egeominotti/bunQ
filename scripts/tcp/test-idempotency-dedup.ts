#!/usr/bin/env bun
/**
 * Test Idempotency & Deduplication (TCP Mode)
 *
 * Tests for custom jobId deduplication, concurrent duplicate pushes,
 * data preservation on dedup, broker-global custom IDs, bulk dedup,
 * and retry compatibility with custom jobIds.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function main() {
  console.log('=== Idempotency & Deduplication Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Custom jobId deduplication
  // Push same job twice with same jobId, verify only 1 job exists
  // ─────────────────────────────────────────────────
  console.log('1. Testing CUSTOM JOBID DEDUPLICATION...');
  {
    const q = makeQueue('tcp-dedup-single');
    q.obliterate();
    await Bun.sleep(200);

    try {
      const job1 = await q.add('task', { value: 1 }, { jobId: 'dedup-single-1' });
      const job2 = await q.add('task', { value: 2 }, { jobId: 'dedup-single-1' });

      if (job1.id === job2.id) {
        ok(`Both adds returned same job ID: ${job1.id}`);
      } else {
        fail(`Expected same ID, got job1=${job1.id}, job2=${job2.id}`);
      }

      const counts = await q.getJobCountsAsync();
      const waiting = counts?.waiting ?? -1;
      if (waiting === 1) {
        ok('Queue has exactly 1 waiting job');
      } else {
        fail(`Expected 1 waiting job, got ${waiting}`);
      }
    } catch (e) {
      fail(`Custom jobId dedup test failed: ${e}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: Concurrent duplicate push
  // Push 10 jobs simultaneously with same jobId, verify only 1 processed
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing CONCURRENT DUPLICATE PUSH (10 jobs, same jobId)...');
  {
    const q = makeQueue('tcp-dedup-concurrent');
    q.obliterate();
    await Bun.sleep(200);

    try {
      const promises = Array.from({ length: 10 }, (_, i) =>
        q.add('task', { index: i }, { jobId: 'concurrent-dedup-key' })
      );
      const jobs = await Promise.all(promises);

      const uniqueIds = new Set(jobs.map((j) => j.id));
      if (uniqueIds.size === 1) {
        ok(`All 10 concurrent adds returned same ID: ${jobs[0].id}`);
      } else {
        fail(`Expected 1 unique ID, got ${uniqueIds.size}`);
      }

      // Process and verify only 1 execution
      let processedCount = 0;
      const worker = new Worker(
        'tcp-dedup-concurrent',
        async () => {
          processedCount++;
          return { ok: true };
        },
        { connection: connOpts, useLocks: false, concurrency: 5 }
      );

      await Bun.sleep(1500);
      await worker.close();

      if (processedCount === 1) {
        ok('Only 1 job was processed');
      } else {
        fail(`Expected 1 processed, got ${processedCount}`);
      }
    } catch (e) {
      fail(`Concurrent duplicate push test failed: ${e}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 3: Different data same jobId — first data wins
  // Push 2 jobs with same jobId but different data, verify first wins
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing DIFFERENT DATA SAME JOBID (first data wins)...');
  {
    const q = makeQueue('tcp-dedup-firstwins');
    q.obliterate();
    await Bun.sleep(200);

    try {
      const job1 = await q.add('task', { msg: 'first' }, { jobId: 'same-id-diff-data' });
      const job2 = await q.add('task', { msg: 'second' }, { jobId: 'same-id-diff-data' });

      if (job1.id === job2.id) {
        ok('Both adds returned same job ID (dedup worked)');
      } else {
        fail(`Expected same ID, got job1=${job1.id}, job2=${job2.id}`);
      }

      // Fetch the job and verify first data is preserved
      const fetched = await q.getJob(job1.id);
      if (fetched && (fetched.data as { msg: string }).msg === 'first') {
        ok('First data preserved (first wins)');
      } else {
        const actualMsg = fetched ? (fetched.data as { msg: string }).msg : 'null';
        fail(`Expected msg='first', got '${actualMsg}'`);
      }
    } catch (e) {
      fail(`Different data same jobId test failed: ${e}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Custom jobId across queues — the jobs.id key is broker-global.
  // A live ID remains owned by its first queue and the second push is a no-op.
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing CUSTOM JOBID ACROSS QUEUES (global idempotency)...');
  {
    const qA = makeQueue('tcp-dedup-cross-a');
    const qB = makeQueue('tcp-dedup-cross-b');
    qA.obliterate();
    qB.obliterate();
    await Bun.sleep(200);

    try {
      const sharedJobId = 'cross-queue-shared-id';
      const jobA = await qA.add('task', { source: 'A' }, { jobId: sharedJobId });
      const jobB = await qB.add('task', { source: 'B' }, { jobId: sharedJobId });

      if (jobA.id === jobB.id) {
        ok('Both adds returned the same broker-global job ID');
      } else {
        fail(`Expected the same ID, got jobA=${jobA.id}, jobB=${jobB.id}`);
      }

      const countsA = await qA.getJobCountsAsync();
      const countsB = await qB.getJobCountsAsync();
      const waitA = countsA?.waiting ?? -1;
      const waitB = countsB?.waiting ?? -1;

      if (waitA === 1 && waitB === 0) {
        ok('The first queue retains the only live generation');
      } else {
        fail(`Expected qA=1 and qB=0, got qA=${waitA}, qB=${waitB}`);
      }
    } catch (e) {
      fail(`Cross-queue jobId test failed: ${e}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 5: Bulk push with duplicates
  // addBulk with duplicate jobIds, verify dedup behavior
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing BULK PUSH WITH DUPLICATES...');
  {
    const q = makeQueue('tcp-dedup-bulk');
    q.obliterate();
    await Bun.sleep(200);

    try {
      // 10 jobs: indices 0,3,6 share 'dup-a', 1,4,7 share 'dup-b',
      // 2,5,8 share 'dup-c', index 9 has unique 'unique-9'
      const bulkJobs = Array.from({ length: 10 }, (_, i) => {
        let jobId: string;
        if (i === 9) {
          jobId = 'unique-9';
        } else {
          jobId = ['dup-a', 'dup-b', 'dup-c'][i % 3];
        }
        return {
          name: `job-${i}`,
          data: { index: i },
          opts: { jobId },
        };
      });

      const results = await q.addBulk(bulkJobs);

      if (results.length === 10) {
        ok('Bulk add returned 10 results (one per input)');
      } else {
        fail(`Expected 10 results, got ${results.length}`);
      }

      // dup-a: indices 0, 3, 6 should share same id
      if (results[3].id === results[0].id && results[6].id === results[0].id) {
        ok('dup-a group all share same ID');
      } else {
        fail(`dup-a IDs differ: ${results[0].id}, ${results[3].id}, ${results[6].id}`);
      }

      // dup-b: indices 1, 4, 7
      if (results[4].id === results[1].id && results[7].id === results[1].id) {
        ok('dup-b group all share same ID');
      } else {
        fail(`dup-b IDs differ: ${results[1].id}, ${results[4].id}, ${results[7].id}`);
      }

      // dup-c: indices 2, 5, 8
      if (results[5].id === results[2].id && results[8].id === results[2].id) {
        ok('dup-c group all share same ID');
      } else {
        fail(`dup-c IDs differ: ${results[2].id}, ${results[5].id}, ${results[8].id}`);
      }

      // Should be 4 unique IDs total
      const allIds = new Set(results.map((r) => r.id));
      if (allIds.size === 4) {
        ok('4 unique jobs total (3 dedup groups + 1 unique)');
      } else {
        fail(`Expected 4 unique IDs, got ${allIds.size}`);
      }

      // Queue should have exactly 4 waiting jobs
      const counts = await q.getJobCountsAsync();
      const waiting = counts?.waiting ?? -1;
      if (waiting === 4) {
        ok('Queue has exactly 4 waiting jobs');
      } else {
        fail(`Expected 4 waiting jobs, got ${waiting}`);
      }
    } catch (e) {
      fail(`Bulk push with duplicates test failed: ${e}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 6: Custom jobId with retry
  // Failing job with custom jobId and attempts:3, verify retries work
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing CUSTOM JOBID WITH RETRY...');
  {
    const q = makeQueue('tcp-dedup-retry');
    q.obliterate();
    await Bun.sleep(200);

    try {
      const customId = 'retry-dedup-test-id';
      let attemptCount = 0;

      const job = await q.add(
        'task',
        { value: 42 },
        {
          jobId: customId,
          attempts: 3,
          backoff: 100,
        }
      );

      if (job.id) {
        ok(`Job created with custom jobId: ${job.id}`);
      } else {
        fail('Job not created');
      }

      const worker = new Worker(
        'tcp-dedup-retry',
        async () => {
          attemptCount++;
          if (attemptCount < 3) {
            throw new Error(`Deliberate failure attempt ${attemptCount}`);
          }
          return { success: true };
        },
        { connection: connOpts, useLocks: false, concurrency: 1 }
      );

      // Wait for retries to complete (3 attempts with 100ms backoff)
      await Bun.sleep(3000);
      await worker.close();

      if (attemptCount === 3) {
        ok(`Job retried correctly: ${attemptCount} attempts`);
      } else {
        fail(`Expected 3 attempts, got ${attemptCount}`);
      }
    } catch (e) {
      fail(`Custom jobId with retry test failed: ${e}`);
    }
  }

  // ─────────────────────────────────────────────────
  // Cleanup
  // ─────────────────────────────────────────────────
  for (const q of queues) {
    q.obliterate();
    q.close();
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
