/**
 * Recovery Logic Tests
 * Tests that all recovery scenarios work correctly after server restart
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';
import { unlink } from 'fs/promises';

// Use unique DB path per test file execution to avoid parallel test interference
const TEST_RUN_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const DB_PATH = `/tmp/test-recovery-logic-${TEST_RUN_ID}.db`;

async function cleanupDb() {
  if (await Bun.file(DB_PATH).exists()) await unlink(DB_PATH);
  if (await Bun.file(DB_PATH + '-wal').exists()) await unlink(DB_PATH + '-wal');
  if (await Bun.file(DB_PATH + '-shm').exists()) await unlink(DB_PATH + '-shm');
}

describe('Recovery Logic', () => {
  beforeEach(async () => {
    // Always shutdown any existing manager first to ensure clean state
    shutdownManager();
    await cleanupDb();
    Bun.env.DATA_PATH = DB_PATH;
  });

  afterEach(async () => {
    shutdownManager();
    await cleanupDb();
    delete Bun.env.DATA_PATH;
  });

  describe('customIdMap Recovery (jobId deduplication)', () => {
    test('should find job by jobId after restart', async () => {
      const QUEUE = 'test-customid-1';

      // Phase 1: Create job
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      const job1 = await queue.add('task', { value: 1 }, {
        jobId: 'unique-job-123',
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });
      const originalId = String(job1.id);

      // Verify dedup works before restart
      const dedupBefore = await queue.getDeduplicationJobId('unique-job-123');
      expect(dedupBefore).toBe(originalId);

      // Simulate restart
      queue.close();
      shutdownManager();
      await Bun.sleep(100);

      // Phase 2: After restart - ensure DATA_PATH is still set (parallel test isolation)
      Bun.env.DATA_PATH = DB_PATH;
      queue = new Queue(QUEUE, { embedded: true });

      const dedupAfter = await queue.getDeduplicationJobId('unique-job-123');
      expect(dedupAfter).toBe(originalId);

      queue.obliterate();
      queue.close();
    });

    test('should return existing job when adding duplicate jobId after restart', async () => {
      const QUEUE = 'test-customid-2';

      // Phase 1: Create job
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      const job1 = await queue.add('task', { value: 1 }, {
        jobId: 'dedup-test-456',
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });
      const originalId = String(job1.id);

      // Simulate restart
      queue.close();
      shutdownManager();
      await Bun.sleep(100);

      // Phase 2: After restart - try to add duplicate
      queue = new Queue(QUEUE, { embedded: true });

      const job2 = await queue.add('task', { value: 2 }, {
        jobId: 'dedup-test-456',
        delay: 60000
      });

      expect(String(job2.id)).toBe(originalId);

      const count = await queue.count();
      expect(count).toBe(1);

      queue.obliterate();
      queue.close();
    });
  });

  describe('uniqueKey Recovery (TTL deduplication)', () => {
    test('should deduplicate using uniqueKey after restart', async () => {
      const QUEUE = 'test-uniquekey-1';

      // Phase 1: Create job with deduplication
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      const job1 = await queue.add('task', { value: 1 }, {
        deduplication: { id: 'dedup-key-789', ttl: 300000 },
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });
      const originalId = String(job1.id);

      // Simulate restart
      queue.close();
      shutdownManager();
      await Bun.sleep(100);

      // Phase 2: After restart - try to add duplicate
      queue = new Queue(QUEUE, { embedded: true });

      const job2 = await queue.add('task', { value: 2 }, {
        deduplication: { id: 'dedup-key-789', ttl: 300000 },
        delay: 60000
      });

      expect(String(job2.id)).toBe(originalId);

      const count = await queue.count();
      expect(count).toBe(1);

      queue.obliterate();
      queue.close();
    });

    test('should allow different uniqueKeys after restart', async () => {
      const QUEUE = 'test-uniquekey-2';

      // Phase 1: Create jobs with different uniqueKeys
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      const job1 = await queue.add('task', { value: 1 }, {
        deduplication: { id: 'key-a', ttl: 300000 },
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });
      const job2 = await queue.add('task', { value: 2 }, {
        deduplication: { id: 'key-b', ttl: 300000 },
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });

      // Simulate restart
      queue.close();
      shutdownManager();
      await Bun.sleep(100);

      // Phase 2: After restart - ensure DATA_PATH is still set (parallel test isolation)
      Bun.env.DATA_PATH = DB_PATH;
      queue = new Queue(QUEUE, { embedded: true });

      const count = await queue.count();
      expect(count).toBe(2);

      queue.obliterate();
      queue.close();
    });
  });

  describe('Dependency Recovery', () => {
    test('should put job with completed dependency in main queue after restart', async () => {
      const QUEUE = 'test-deps-1';

      // Phase 1: Create and complete parent job
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      const parentJob = await queue.add('parent', { value: 'parent' });

      // Process parent job to completion
      const worker = new Worker(
        QUEUE,
        async () => ({ result: 'done' }),
        { embedded: true, autorun: false }
      );

      worker.run();
      await Bun.sleep(200);
      await worker.close();

      // Create child job (delayed so it won't be processed)
      const childJob = await queue.add('child', { value: 'child' }, {
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });
      const childId = String(childJob.id);

      // Simulate restart
      queue.close();
      shutdownManager();
      await Bun.sleep(100);

      // Phase 2: After restart - ensure DATA_PATH is still set (parallel test isolation)
      Bun.env.DATA_PATH = DB_PATH;
      queue = new Queue(QUEUE, { embedded: true });

      // Child should be in queue
      const jobs = await queue.getJobs(['waiting', 'delayed']);
      const childFound = jobs.some(j => String(j.id) === childId);

      expect(childFound).toBe(true);

      queue.obliterate();
      queue.close();
    });
  });

  describe('Delayed Jobs Recovery', () => {
    test('should keep delayed job as delayed after restart', async () => {
      const QUEUE = 'test-delayed-1';

      // Phase 1: Create delayed job
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      await queue.add('task', { value: 1 }, { delay: 3600000, durable: true }); // 1 hour, immediate disk write

      // Simulate restart
      queue.close();
      shutdownManager();
      await Bun.sleep(100);

      // Phase 2: After restart - ensure DATA_PATH is still set (parallel test isolation)
      Bun.env.DATA_PATH = DB_PATH;
      queue = new Queue(QUEUE, { embedded: true });

      const delayedJobs = await queue.getJobs(['delayed']);
      expect(delayedJobs.length).toBe(1);

      queue.obliterate();
      queue.close();
    });
  });

  describe('Multiple Restarts', () => {
    test('should not corrupt data after multiple restarts', async () => {
      const QUEUE = 'test-multi-restart';

      // Phase 1: Create job
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      const job = await queue.add('task', { value: 1 }, {
        jobId: 'persist-id',
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });
      const originalId = String(job.id);

      // Multiple restarts
      for (let i = 0; i < 3; i++) {
        queue.close();
        shutdownManager();
        await Bun.sleep(50);
        // Ensure DATA_PATH is still set (parallel test isolation)
        Bun.env.DATA_PATH = DB_PATH;
        queue = new Queue(QUEUE, { embedded: true });
      }

      // Verify job is still there and dedup works
      const dedupId = await queue.getDeduplicationJobId('persist-id');
      expect(dedupId).toBe(originalId);

      const count = await queue.count();
      expect(count).toBe(1);

      queue.obliterate();
      queue.close();
    });
  });

  describe('Combined Features', () => {
    test('should recover all features together correctly', async () => {
      const QUEUE = 'test-combined';

      // Phase 1: Create various jobs
      let queue = new Queue(QUEUE, { embedded: true });
      queue.obliterate();

      // Job A: Simple job with jobId
      const jobA = await queue.add('taskA', { type: 'A' }, {
        jobId: 'job-a',
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });

      // Job B: Job with uniqueKey deduplication
      const jobB = await queue.add('taskB', { type: 'B' }, {
        deduplication: { id: 'key-b', ttl: 300000 },
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });

      // Job C: Job with both jobId and deduplication
      const jobC = await queue.add('taskC', { type: 'C' }, {
        jobId: 'job-c',
        deduplication: { id: 'key-c', ttl: 300000 },
        delay: 60000,
        durable: true  // Ensure immediate disk write for recovery test
      });

      const countBefore = await queue.count();
      expect(countBefore).toBe(3);

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
      expect(String(jobA2.id)).toBe(String(jobA.id));
      expect(String(jobB2.id)).toBe(String(jobB.id));
      expect(String(jobC2.id)).toBe(String(jobC.id));

      const countAfter = await queue.count();
      expect(countAfter).toBe(3);

      queue.obliterate();
      queue.close();
    });
  });
});
