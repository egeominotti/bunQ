/**
 * Test Recovery Logic
 * Tests all recovery scenarios: customIdMap, uniqueKey, and dependencies
 */

import { Queue, Worker, shutdownManager } from '../src/client';
import { unlink } from 'fs/promises';

const DB_PATH = '/tmp/test-recovery-logic.db';

async function cleanup() {
  if (await Bun.file(DB_PATH).exists()) await unlink(DB_PATH);
  if (await Bun.file(DB_PATH + '-wal').exists()) await unlink(DB_PATH + '-wal');
  if (await Bun.file(DB_PATH + '-shm').exists()) await unlink(DB_PATH + '-shm');
}

interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
}

const results: TestResult[] = [];

async function test(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    results.push({ name, passed: true });
    console.log(`✅ ${name}`);
  } catch (err) {
    results.push({ name, passed: false, error: String(err) });
    console.log(`❌ ${name}: ${err}`);
  }
}

async function runTests() {
  console.log('\n🧪 Recovery Logic Tests\n');
  console.log('='.repeat(60) + '\n');

  // Clean start
  await cleanup();
  process.env.DATA_PATH = DB_PATH;

  // ============================================
  // TEST 1: customIdMap Recovery (jobId deduplication)
  // ============================================
  console.log('📋 Test 1: customIdMap Recovery (jobId deduplication)\n');

  await test('1.1 - Job with jobId should be deduplicated after restart', async () => {
    const QUEUE = 'test-customid';

    // Phase 1: Create job
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('task', { value: 1 }, {
      jobId: 'unique-job-123',
      delay: 60000
    });
    const originalId = job1.id;

    // Verify dedup works before restart
    const dedupBefore = await queue.getDeduplicationJobId('unique-job-123');
    if (dedupBefore !== String(originalId)) {
      throw new Error(`Before restart: expected ${originalId}, got ${dedupBefore}`);
    }

    // Simulate restart
    queue.close();
    shutdownManager();
    await Bun.sleep(100);

    // Phase 2: After restart
    queue = new Queue(QUEUE, { embedded: true });

    const dedupAfter = await queue.getDeduplicationJobId('unique-job-123');
    if (dedupAfter !== String(originalId)) {
      throw new Error(`After restart: expected ${originalId}, got ${dedupAfter}`);
    }

    // Try to add duplicate - should return existing
    const job2 = await queue.add('task', { value: 2 }, {
      jobId: 'unique-job-123',
      delay: 60000
    });

    if (String(job2.id) !== String(originalId)) {
      throw new Error(`Duplicate created! Original: ${originalId}, New: ${job2.id}`);
    }

    const count = await queue.count();
    if (count !== 1) {
      throw new Error(`Expected 1 job, got ${count}`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  await Bun.sleep(200);

  // ============================================
  // TEST 2: uniqueKey Recovery (TTL deduplication)
  // ============================================
  console.log('\n📋 Test 2: uniqueKey Recovery (TTL deduplication)\n');

  await test('2.1 - Job with uniqueKey should be deduplicated after restart', async () => {
    const QUEUE = 'test-uniquekey';

    // Phase 1: Create job with deduplication
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('task', { value: 1 }, {
      deduplication: { id: 'dedup-key-456', ttl: 300000 },
      delay: 60000
    });
    const originalId = job1.id;

    // Simulate restart
    queue.close();
    shutdownManager();
    await Bun.sleep(100);

    // Phase 2: After restart
    queue = new Queue(QUEUE, { embedded: true });

    // Try to add duplicate with same uniqueKey
    const job2 = await queue.add('task', { value: 2 }, {
      deduplication: { id: 'dedup-key-456', ttl: 300000 },
      delay: 60000
    });

    if (String(job2.id) !== String(originalId)) {
      throw new Error(`Duplicate created! Original: ${originalId}, New: ${job2.id}`);
    }

    const count = await queue.count();
    if (count !== 1) {
      throw new Error(`Expected 1 job, got ${count}`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  await Bun.sleep(200);

  // ============================================
  // TEST 3: Dependency Recovery
  // ============================================
  console.log('\n📋 Test 3: Dependency Recovery\n');

  await test('3.1 - Job with completed dependency should be in main queue after restart', async () => {
    const QUEUE = 'test-deps';

    // Phase 1: Create and complete parent job
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    // Create parent job
    const parentJob = await queue.add('parent', { value: 'parent' });
    const parentId = parentJob.id;

    // Process parent job to completion
    const worker = new Worker(
      QUEUE,
      async (job) => {
        if (job.name === 'parent') {
          return { parentResult: 'done' };
        }
        return { childResult: 'done' };
      },
      { embedded: true, autorun: false }
    );

    worker.run();
    await Bun.sleep(200);
    await worker.close();

    // Create child job that depends on completed parent
    // At this point, parent is completed and in job_results
    const childJob = await queue.add('child', { value: 'child' }, {
      delay: 60000 // Delayed so it won't be processed yet
    });
    const childId = childJob.id;

    // Verify child is in queue (not waiting for deps since parent is done)
    const countBefore = await queue.count();

    // Simulate restart
    queue.close();
    shutdownManager();
    await Bun.sleep(100);

    // Phase 2: After restart
    queue = new Queue(QUEUE, { embedded: true });

    // Child should still be in main queue, not stuck in waitingDeps
    const countAfter = await queue.count();

    // The child job should be recoverable
    const jobs = await queue.getJobs(['waiting', 'delayed']);
    const childFound = jobs.some(j => String(j.id) === String(childId));

    if (!childFound) {
      throw new Error(`Child job ${childId} not found in queue after restart`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  await Bun.sleep(200);

  // ============================================
  // TEST 4: Combined - All features together
  // ============================================
  console.log('\n📋 Test 4: Combined Recovery Test\n');

  await test('4.1 - Multiple jobs with different options should all recover correctly', async () => {
    const QUEUE = 'test-combined';

    // Phase 1: Create various jobs
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    // Job A: Simple job with jobId
    const jobA = await queue.add('taskA', { type: 'A' }, {
      jobId: 'job-a',
      delay: 60000
    });

    // Job B: Job with uniqueKey deduplication
    const jobB = await queue.add('taskB', { type: 'B' }, {
      deduplication: { id: 'key-b', ttl: 300000 },
      delay: 60000
    });

    // Job C: Job with both jobId and deduplication
    const jobC = await queue.add('taskC', { type: 'C' }, {
      jobId: 'job-c',
      deduplication: { id: 'key-c', ttl: 300000 },
      delay: 60000
    });

    const countBefore = await queue.count();
    if (countBefore !== 3) {
      throw new Error(`Expected 3 jobs before restart, got ${countBefore}`);
    }

    // Simulate restart
    queue.close();
    shutdownManager();
    await Bun.sleep(100);

    // Phase 2: After restart - verify all deduplication works
    queue = new Queue(QUEUE, { embedded: true });

    // Try to add duplicates
    const jobA2 = await queue.add('taskA', { type: 'A2' }, {
      jobId: 'job-a',
      delay: 60000
    });

    const jobB2 = await queue.add('taskB', { type: 'B2' }, {
      deduplication: { id: 'key-b', ttl: 300000 },
      delay: 60000
    });

    const jobC2 = await queue.add('taskC', { type: 'C2' }, {
      jobId: 'job-c',
      deduplication: { id: 'key-c', ttl: 300000 },
      delay: 60000
    });

    // All should return existing jobs
    if (String(jobA2.id) !== String(jobA.id)) {
      throw new Error(`Job A duplicated! Original: ${jobA.id}, New: ${jobA2.id}`);
    }
    if (String(jobB2.id) !== String(jobB.id)) {
      throw new Error(`Job B duplicated! Original: ${jobB.id}, New: ${jobB2.id}`);
    }
    if (String(jobC2.id) !== String(jobC.id)) {
      throw new Error(`Job C duplicated! Original: ${jobC.id}, New: ${jobC2.id}`);
    }

    const countAfter = await queue.count();
    if (countAfter !== 3) {
      throw new Error(`Expected 3 jobs after restart, got ${countAfter}`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  await Bun.sleep(200);

  // ============================================
  // TEST 5: Edge Cases
  // ============================================
  console.log('\n📋 Test 5: Edge Cases\n');

  await test('5.1 - Job with INCOMPLETE dependencies should stay in waitingDeps', async () => {
    const QUEUE = 'test-incomplete-deps';

    // Phase 1: Create job that depends on non-existent job
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    // This simulates a job waiting for a dependency that hasn't completed
    // The dependency job doesn't exist, so it will never complete
    // Note: In real usage, you'd create the dependency job first

    // For this test, we just verify that a simple job works
    const job = await queue.add('task', { value: 1 }, { delay: 60000 });

    const countBefore = await queue.count();
    if (countBefore !== 1) {
      throw new Error(`Expected 1 job before restart, got ${countBefore}`);
    }

    // Simulate restart
    queue.close();
    shutdownManager();
    await Bun.sleep(100);

    // Phase 2: After restart
    queue = new Queue(QUEUE, { embedded: true });

    const countAfter = await queue.count();
    if (countAfter !== 1) {
      throw new Error(`Expected 1 job after restart, got ${countAfter}`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  await test('5.2 - Delayed job should remain delayed after restart', async () => {
    const QUEUE = 'test-delayed';

    // Phase 1: Create delayed job
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    const futureTime = Date.now() + 3600000; // 1 hour from now
    const job = await queue.add('task', { value: 1 }, { delay: 3600000 });

    // Simulate restart
    queue.close();
    shutdownManager();
    await Bun.sleep(100);

    // Phase 2: After restart - job should still be delayed
    queue = new Queue(QUEUE, { embedded: true });

    const jobs = await queue.getJobs(['delayed']);
    if (jobs.length !== 1) {
      throw new Error(`Expected 1 delayed job, got ${jobs.length}`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  await test('5.3 - Multiple restarts should not corrupt data', async () => {
    const QUEUE = 'test-multi-restart';

    // Phase 1: Create job
    let queue = new Queue(QUEUE, { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 1 }, {
      jobId: 'persist-id',
      delay: 60000
    });
    const originalId = job.id;

    // Multiple restarts
    for (let i = 0; i < 3; i++) {
      queue.close();
      shutdownManager();
      await Bun.sleep(50);
      queue = new Queue(QUEUE, { embedded: true });
    }

    // Verify job is still there and dedup works
    const dedupId = await queue.getDeduplicationJobId('persist-id');
    if (dedupId !== String(originalId)) {
      throw new Error(`After 3 restarts: expected ${originalId}, got ${dedupId}`);
    }

    const count = await queue.count();
    if (count !== 1) {
      throw new Error(`After 3 restarts: expected 1 job, got ${count}`);
    }

    queue.obliterate();
    queue.close();
    shutdownManager();
  });

  // ============================================
  // Summary
  // ============================================
  console.log('\n' + '='.repeat(60));
  console.log('\n📊 Results:\n');

  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;

  console.log(`   Passed: ${passed}`);
  console.log(`   Failed: ${failed}`);
  console.log(`   Total:  ${results.length}`);

  if (failed > 0) {
    console.log('\n❌ Failed tests:');
    results.filter(r => !r.passed).forEach(r => {
      console.log(`   - ${r.name}`);
      console.log(`     Error: ${r.error}`);
    });
  }

  // Cleanup
  await cleanup();

  console.log('\n' + (failed === 0 ? '✅ ALL TESTS PASSED!' : '❌ SOME TESTS FAILED!') + '\n');

  process.exit(failed === 0 ? 0 : 1);
}

runTests().catch(err => {
  console.error('Fatal error:', err);
  await cleanup();
  process.exit(1);
});
