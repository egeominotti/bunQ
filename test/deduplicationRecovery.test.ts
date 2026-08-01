/**
 * Deduplication Recovery Tests
 * Tests that jobId deduplication works correctly after server restart
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('Deduplication Recovery', () => {
  const QUEUE_NAME = 'dedup-recovery-test';
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue(QUEUE_NAME, { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('jobId deduplication', () => {
    test('should return existing job when adding with same jobId', async () => {
      // Add first job
      const job1 = await queue.add('test', { value: 1 }, { jobId: 'unique-123' });
      expect(job1.id).toBeDefined();

      // Add second job with same jobId - should return existing job
      const job2 = await queue.add('test', { value: 2 }, { jobId: 'unique-123' });
      expect(job2.id).toBe(job1.id);
    });

    test('custom job IDs and deduplication IDs use separate lookups', async () => {
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          jobId: 'my-custom-id',
          deduplication: { id: 'my-deduplication-id', ttl: 60_000 },
        }
      );

      expect(await queue.getDeduplicationJobId('my-deduplication-id')).toBe(String(job.id));
      expect(await queue.getDeduplicationJobId('my-custom-id')).toBeNull();
      expect((await queue.getJob('my-custom-id'))?.id).toBe(job.id);
    });

    test('should return null for a non-existent deduplication ID', async () => {
      const foundId = await queue.getDeduplicationJobId('non-existent-id');
      expect(foundId).toBeNull();
    });

    test('customIdMap should be populated after adding job', async () => {
      const customId = 'test-custom-id-456';

      // Add job
      await queue.add('test', { value: 1 }, { jobId: customId });

      // Verify customIdMap has the entry
      const manager = getSharedManager();
      const job = manager.getJobByCustomId(customId);
      expect(job).not.toBeNull();
      expect(job?.customId).toBe(customId);
    });
  });

  describe('recovery simulation', () => {
    test('customIdMap should remain queryable after adding a job', async () => {
      const customId = 'recovery-test-id';

      // Add job with custom ID
      const originalJob = await queue.add('test', { value: 1 }, { jobId: customId });
      expect(originalJob.id).toBeDefined();

      // The custom-ID index is distinct from the deduplication-key index.
      const manager = getSharedManager();
      const jobFromManager = manager.getJobByCustomId(customId);
      expect(jobFromManager).not.toBeNull();
      expect(jobFromManager?.id).toBe(originalJob.id);
      expect(await queue.getDeduplicationJobId(customId)).toBeNull();
    });

    test('should not create duplicate when jobId exists in queue', async () => {
      const customId = 'no-duplicate-test';

      // Add first job
      const job1 = await queue.add('test', { value: 1 }, { jobId: customId });

      // Try to add again - should return same job
      const job2 = await queue.add('test', { value: 2 }, { jobId: customId });

      // Should be the same job
      expect(job2.id).toBe(job1.id);

      // Verify only one job exists in queue (not duplicates)
      const jobs = await queue.getJobs(['waiting', 'delayed']);
      const matchingJobs = jobs.filter((j) => j.id === job1.id);
      expect(matchingJobs.length).toBe(1);

      // Should only have 1 job total in queue
      const count = await queue.count();
      expect(count).toBe(1);
    });

    test('multiple jobs with different jobIds should all be tracked', async () => {
      // Add multiple jobs with different custom IDs
      const job1 = await queue.add('test', { value: 1 }, { jobId: 'id-1' });
      const job2 = await queue.add('test', { value: 2 }, { jobId: 'id-2' });
      const job3 = await queue.add('test', { value: 3 }, { jobId: 'id-3' });

      // All should be findable through the public job-ID lookup.
      expect((await queue.getJob('id-1'))?.id).toBe(job1.id);
      expect((await queue.getJob('id-2'))?.id).toBe(job2.id);
      expect((await queue.getJob('id-3'))?.id).toBe(job3.id);
    });
  });

  describe('deduplication after job completion', () => {
    test('should return same job if jobId exists (BullMQ idempotency)', async () => {
      const customId = 'complete-then-reuse';

      // Add and process job
      const job1 = await queue.add('test', { value: 1 }, { jobId: customId });

      const worker = new Worker(QUEUE_NAME, async () => ({ done: true }), {
        embedded: true,
        autorun: false,
      });

      // Process the job
      worker.run();
      await Bun.sleep(100);
      await worker.close();

      // BullMQ behavior: jobId IS the job ID, so adding same jobId returns same job
      const job2 = await queue.add('test', { value: 2 }, { jobId: customId });

      // Should be the SAME job (BullMQ idempotency - jobId is the actual ID)
      expect(job2.id).toBe(job1.id);
      expect(job2.id).toBe(customId);
    });

    test('should allow reusing jobId after removeOnComplete', async () => {
      const customId = 'remove-and-reuse';

      // Add job with removeOnComplete
      const job1 = await queue.add(
        'test',
        { value: 1 },
        { jobId: customId, removeOnComplete: true }
      );
      expect(job1.id).toBe(customId);

      const worker = new Worker(QUEUE_NAME, async () => ({ done: true }), {
        embedded: true,
        autorun: false,
      });

      // Process the job (should be removed after completion)
      worker.run();
      await Bun.sleep(200);
      await worker.close();

      // Now we should be able to add a new job with the same ID
      const job2 = await queue.add('test', { value: 2 }, { jobId: customId });

      // Should be the same ID (reused after removal)
      expect(job2.id).toBe(customId);
    });
  });

  describe('uniqueKey deduplication', () => {
    test('should deduplicate using uniqueKey with TTL', async () => {
      // Add job with deduplication TTL
      const job1 = await queue.add(
        'test',
        { value: 1 },
        {
          deduplication: { id: 'unique-key-1', ttl: 60000 },
        }
      );
      expect(job1.id).toBeDefined();

      // Try to add duplicate - should return existing job
      const job2 = await queue.add(
        'test',
        { value: 2 },
        {
          deduplication: { id: 'unique-key-1', ttl: 60000 },
        }
      );
      expect(job2.id).toBe(job1.id);
    });

    test('should allow different uniqueKeys', async () => {
      const job1 = await queue.add(
        'test',
        { value: 1 },
        {
          deduplication: { id: 'key-a', ttl: 60000 },
        }
      );
      const job2 = await queue.add(
        'test',
        { value: 2 },
        {
          deduplication: { id: 'key-b', ttl: 60000 },
        }
      );

      expect(job1.id).not.toBe(job2.id);
    });
  });

  describe('dependency tracking', () => {
    test('job without dependencies should be in main queue', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(job.id).toBeDefined();

      const count = await queue.count();
      expect(count).toBe(1);
    });

    test('jobs with all options should be tracked correctly', async () => {
      // Add job with both jobId and deduplication
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          jobId: 'combined-id',
          deduplication: { id: 'combined-dedup', ttl: 30000 },
          delay: 1000,
        }
      );

      expect(job.id).toBeDefined();

      expect((await queue.getJob('combined-id'))?.id).toBe(job.id);
      expect(await queue.getDeduplicationJobId('combined-dedup')).toBe(String(job.id));
    });
  });
});
