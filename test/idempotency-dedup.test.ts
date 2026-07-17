/**
 * Idempotency & Deduplication Tests (Embedded Mode)
 *
 * Tests for custom jobId deduplication, concurrent duplicate pushes,
 * data preservation on dedup, cross-queue isolation, jobId reuse after
 * completion, bulk dedup, and retry compatibility.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Idempotency & Deduplication - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. custom jobId deduplication — push same job twice, only 1 exists', async () => {
    const queue = new Queue<{ value: number }>('idemp-dedup-1', { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('task', { value: 1 }, { jobId: 'dedup-single' });
    const job2 = await queue.add('task', { value: 2 }, { jobId: 'dedup-single' });

    // Second add should return the same job (dedup)
    expect(job2.id).toBe(job1.id);

    // Only 1 job should exist in the queue
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(1);

    queue.close();
  }, 30000);

  test('2. concurrent duplicate push — 10 jobs with same jobId, only 1 processed', async () => {
    const queue = new Queue<{ index: number }>('idemp-dedup-2', { embedded: true });
    queue.obliterate();

    // Push 10 jobs concurrently with the same jobId
    const promises = Array.from({ length: 10 }, (_, i) =>
      queue.add('task', { index: i }, { jobId: 'concurrent-dedup' })
    );
    const jobs = await Promise.all(promises);

    // All should return the same job ID
    const uniqueIds = new Set(jobs.map((j) => j.id));
    expect(uniqueIds.size).toBe(1);

    // Only 1 job in the queue
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(1);

    // Process it and verify only 1 execution
    let processedCount = 0;
    const worker = new Worker(
      'idemp-dedup-2',
      async () => {
        processedCount++;
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    for (let i = 0; i < 100; i++) {
      if (processedCount >= 1) break;
      await Bun.sleep(50);
    }
    await worker.close();

    expect(processedCount).toBe(1);

    queue.close();
  }, 30000);

  test('3. different data same jobId — first data wins', async () => {
    const queue = new Queue<{ msg: string }>('idemp-dedup-3', { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('task', { msg: 'first' }, { jobId: 'same-id-diff-data' });
    const job2 = await queue.add('task', { msg: 'second' }, { jobId: 'same-id-diff-data' });

    // Should return the same job
    expect(job2.id).toBe(job1.id);

    // Fetch the job and verify the first data is preserved
    const fetched = await queue.getJob(job1.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data.msg).toBe('first');

    queue.close();
  }, 30000);

  test('4. a live custom jobId is globally idempotent across queues', async () => {
    const queueA = new Queue<{ source: string }>('idemp-dedup-4a', { embedded: true });
    const queueB = new Queue<{ source: string }>('idemp-dedup-4b', { embedded: true });
    queueA.obliterate();
    queueB.obliterate();

    const sharedJobId = 'cross-queue-id';

    const jobA = await queueA.add('task', { source: 'A' }, { jobId: sharedJobId });
    const jobB = await queueB.add('task', { source: 'B' }, { jobId: sharedJobId });

    // jobs.id is the custom ID and is globally unique in the broker database:
    // the second queue cannot create another live generation.
    expect(jobB.id).toBe(jobA.id);
    expect(jobB.data).toEqual({ source: 'A' });

    const countsA = queueA.getJobCounts();
    const countsB = queueB.getJobCounts();
    expect(countsA.waiting).toBe(1);
    expect(countsB.waiting).toBe(0);
    expect((await queueA.getJob(jobA.id))?.data).toEqual({ source: 'A' });

    queueA.close();
    queueB.close();
  }, 30000);

  test('5. reuse jobId after completion — second job is created with removeOnComplete', async () => {
    const queue = new Queue<{ attempt: number }>('idemp-dedup-5', { embedded: true });
    queue.obliterate();

    const customId = 'reuse-after-complete';

    // Add first job with removeOnComplete so the dedup key is freed
    const job1 = await queue.add(
      'task',
      { attempt: 1 },
      {
        jobId: customId,
        removeOnComplete: true,
      }
    );
    expect(job1.id).toBeDefined();

    // Process and complete the first job
    const worker1 = new Worker('idemp-dedup-5', async () => ({ done: true }), {
      embedded: true,
      concurrency: 1,
    });

    // Wait for completion
    for (let i = 0; i < 100; i++) {
      const state = await queue.getJobState(job1.id);
      if (state === 'completed' || state === 'unknown') break;
      await Bun.sleep(50);
    }
    await worker1.close();

    // Now add another job with the same jobId — should succeed
    const job2 = await queue.add(
      'task',
      { attempt: 2 },
      {
        jobId: customId,
      }
    );
    expect(job2.id).toBeDefined();
    // The job should use the same custom ID
    expect(job2.id).toBe(customId);

    queue.close();
  }, 30000);

  test('6. bulk push with duplicates — addBulk deduplicates correctly', async () => {
    const queue = new Queue<{ index: number }>('idemp-dedup-6', { embedded: true });
    queue.obliterate();

    // 10 jobs where indices 0,3,6 share jobId 'dup-a', indices 1,4,7 share 'dup-b',
    // indices 2,5,8 share 'dup-c', and index 9 has unique 'unique-9'
    const bulkJobs = Array.from({ length: 10 }, (_, i) => {
      let jobId: string;
      if (i === 9) {
        jobId = 'unique-9';
      } else {
        const group = ['dup-a', 'dup-b', 'dup-c'][i % 3];
        jobId = group;
      }
      return {
        name: `job-${i}`,
        data: { index: i },
        opts: { jobId },
      };
    });

    const results = await queue.addBulk(bulkJobs);

    // Should return 10 results (one per input)
    expect(results.length).toBe(10);

    // Duplicate jobs should return the same IDs
    // dup-a: indices 0, 3, 6 — all should have same id
    expect(results[3].id).toBe(results[0].id);
    expect(results[6].id).toBe(results[0].id);

    // dup-b: indices 1, 4, 7
    expect(results[4].id).toBe(results[1].id);
    expect(results[7].id).toBe(results[1].id);

    // dup-c: indices 2, 5, 8
    expect(results[5].id).toBe(results[2].id);
    expect(results[8].id).toBe(results[2].id);

    // unique-9 should be distinct
    const allIds = new Set(results.map((r) => r.id));
    // 4 unique jobs: dup-a, dup-b, dup-c, unique-9
    expect(allIds.size).toBe(4);

    // Queue should have exactly 4 waiting jobs
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(4);

    queue.close();
  }, 30000);

  test('7. custom jobId with retry — dedup does not interfere with retries', async () => {
    const queue = new Queue<{ value: number }>('idemp-dedup-7', { embedded: true });
    queue.obliterate();

    const customId = 'retry-dedup-test';
    let attemptCount = 0;

    // Add a job that will fail first 2 times and succeed on the 3rd
    const job = await queue.add(
      'task',
      { value: 42 },
      {
        jobId: customId,
        attempts: 3,
        backoff: 100,
      }
    );
    expect(job.id).toBeDefined();

    const worker = new Worker(
      'idemp-dedup-7',
      async () => {
        attemptCount++;
        if (attemptCount < 3) {
          throw new Error(`Deliberate failure attempt ${attemptCount}`);
        }
        return { success: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for the job to eventually complete
    for (let i = 0; i < 200; i++) {
      const state = await queue.getJobState(job.id);
      if (state === 'completed') break;
      await Bun.sleep(50);
    }

    await worker.close();

    // Should have been attempted 3 times
    expect(attemptCount).toBe(3);

    // Job should be completed now
    const finalState = await queue.getJobState(job.id);
    expect(finalState).toBe('completed');

    // Verify the dedup key still works — adding same jobId should return same job
    const job2 = await queue.add('task', { value: 99 }, { jobId: customId });
    expect(job2.id).toBe(job.id);

    queue.close();
  }, 30000);
});
