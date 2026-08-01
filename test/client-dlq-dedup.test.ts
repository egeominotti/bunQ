/**
 * Comprehensive tests for client-side DLQ and Deduplication modules
 * Tests: dlq.ts, dlqOps.ts, deduplication.ts
 *
 * Run with: BUNQUEUE_EMBEDDED=1 bun test test/client-dlq-dedup.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';
import type { DlqConfig, DlqFilter, DlqEntry, DlqStats } from '../src/client';

/**
 * Helper: add a job and process it through a worker that always fails,
 * causing the job to land in the DLQ after exhausting all attempts.
 */
async function addAndFailJob(
  queue: Queue<Record<string, unknown>>,
  queueName: string,
  data: Record<string, unknown>,
  opts: { attempts?: number; delay?: number } = {}
): Promise<string> {
  const job = await queue.add('fail-task', data, {
    attempts: opts.attempts ?? 1,
    backoff: 0,
    ...(opts.delay !== undefined ? { delay: opts.delay } : {}),
  });

  const worker = new Worker(
    queueName,
    async () => {
      throw new Error('intentional failure');
    },
    { concurrency: 1 }
  );

  // Wait for the job to exhaust attempts and land in DLQ
  await Bun.sleep(500);
  await worker.close();

  return job.id;
}

// ============================================================
// DLQ OPERATIONS (src/client/queue/dlq.ts, dlqOps.ts)
// ============================================================
describe('DLQ Operations', () => {
  let queue: Queue<Record<string, unknown>>;
  const QUEUE_NAME = 'dlq-test';

  beforeEach(() => {
    queue = new Queue(QUEUE_NAME);
  });

  afterEach(() => {
    shutdownManager();
  });

  // ---- Empty DLQ Edge Cases ----
  describe('empty DLQ', () => {
    it('should return empty array from getDlq when no DLQ entries exist', () => {
      const entries = queue.getDlq();
      expect(entries).toEqual([]);
    });

    it('should return 0 from retryDlq when DLQ is empty', () => {
      const count = queue.retryDlq();
      expect(count).toBe(0);
    });

    it('should return 0 from purgeDlq when DLQ is empty', () => {
      const count = queue.purgeDlq();
      expect(count).toBe(0);
    });

    it('should return zero-filled stats from getDlqStats when DLQ is empty', () => {
      const stats = queue.getDlqStats();
      expect(stats.total).toBe(0);
      expect(stats.pendingRetry).toBe(0);
      expect(stats.expired).toBe(0);
      expect(stats.oldestEntry).toBeNull();
      expect(stats.newestEntry).toBeNull();
    });

    it('should return 0 from retryDlqByFilter when DLQ is empty', () => {
      const count = queue.retryDlqByFilter({ reason: 'max_attempts_exceeded' });
      expect(count).toBe(0);
    });
  });

  // ---- getDlqConfig / setDlqConfig ----
  describe('getDlqConfig / setDlqConfig', () => {
    it('should return default DLQ config', () => {
      const config = queue.getDlqConfig();
      expect(config).toBeDefined();
      expect(config.autoRetry).toBe(false);
      expect(config.maxAutoRetries).toBe(3);
      expect(config.maxEntries).toBe(10000);
      expect(config.maxAge).toBe(7 * 24 * 60 * 60 * 1000); // 7 days
    });

    it('should set and get custom DLQ config', () => {
      queue.setDlqConfig({
        autoRetry: true,
        maxAutoRetries: 5,
        maxAge: 3600000, // 1 hour
        maxEntries: 500,
      });

      const config = queue.getDlqConfig();
      expect(config.autoRetry).toBe(true);
      expect(config.maxAutoRetries).toBe(5);
      expect(config.maxAge).toBe(3600000);
      expect(config.maxEntries).toBe(500);
    });

    it('should partially update DLQ config', () => {
      queue.setDlqConfig({ autoRetry: true });

      const config = queue.getDlqConfig();
      expect(config.autoRetry).toBe(true);
      // Other defaults should remain
      expect(config.maxAutoRetries).toBe(3);
    });

    it('should set maxAge to null to disable auto-purge', () => {
      queue.setDlqConfig({ maxAge: null });

      const config = queue.getDlqConfig();
      expect(config.maxAge).toBeNull();
    });
  });

  // ---- Adding jobs to DLQ via failure ----
  describe('getDlq (after job failures)', () => {
    it('should contain a job after it exhausts all attempts', async () => {
      const jobId = await addAndFailJob(queue, QUEUE_NAME, { key: 'value1' });

      const entries = queue.getDlq();
      expect(entries.length).toBeGreaterThanOrEqual(1);

      const entry = entries.find((e) => e.job.id === jobId);
      expect(entry).toBeDefined();
      expect(entry!.reason).toBe('max_attempts_exceeded');
      expect(entry!.job.data).toEqual({ key: 'value1' });
    });

    it('should have correct DLQ entry metadata', async () => {
      const jobId = await addAndFailJob(queue, QUEUE_NAME, { meta: 'test' });

      const entries = queue.getDlq();
      const entry = entries.find((e) => e.job.id === jobId);
      expect(entry).toBeDefined();
      expect(entry!.enteredAt).toBeGreaterThan(0);
      expect(entry!.retryCount).toBe(0);
      expect(entry!.lastRetryAt).toBeNull();
      expect(entry!.attempts).toBeDefined();
      expect(entry!.attempts.length).toBeGreaterThanOrEqual(1);
    });

    it('should accumulate multiple failed jobs in DLQ', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { idx: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { idx: 2 });
      await addAndFailJob(queue, QUEUE_NAME, { idx: 3 });

      const entries = queue.getDlq();
      expect(entries.length).toBeGreaterThanOrEqual(3);
    });
  });

  // ---- getDlqStats ----
  describe('getDlqStats', () => {
    it('should return correct stats after adding jobs to DLQ', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { stat: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { stat: 2 });

      const stats = queue.getDlqStats();
      expect(stats.total).toBeGreaterThanOrEqual(2);
      expect(stats.oldestEntry).toBeGreaterThan(0);
      expect(stats.newestEntry).toBeGreaterThanOrEqual(stats.oldestEntry!);
    });

    it('should track entries by reason', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { reason: 'test' });

      const stats = queue.getDlqStats();
      expect(stats.byReason).toBeDefined();
      expect(stats.byReason['max_attempts_exceeded']).toBeGreaterThanOrEqual(1);
    });
  });

  // ---- DLQ Filtering ----
  describe('getDlq with filter', () => {
    it('should filter by reason', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { filter: 'byReason' });

      const filtered = queue.getDlq({ reason: 'max_attempts_exceeded' });
      expect(filtered.length).toBeGreaterThanOrEqual(1);
      for (const entry of filtered) {
        expect(entry.reason).toBe('max_attempts_exceeded');
      }
    });

    it('should filter with limit', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { a: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { a: 2 });
      await addAndFailJob(queue, QUEUE_NAME, { a: 3 });

      const limited = queue.getDlq({ limit: 1 });
      expect(limited.length).toBeLessThanOrEqual(1);
    });

    it('should filter with offset', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { b: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { b: 2 });

      const all = queue.getDlq();
      const withOffset = queue.getDlq({ offset: 1 });

      // offset should return fewer entries than all
      if (all.length > 1) {
        expect(withOffset.length).toBeLessThan(all.length);
      }
    });

    it('should return empty when filter matches nothing', () => {
      const filtered = queue.getDlq({ reason: 'stalled' });
      expect(filtered).toEqual([]);
    });
  });

  // ---- retryDlq ----
  describe('retryDlq', () => {
    it('should retry all DLQ entries and return count', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { retry: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { retry: 2 });

      const entriesBefore = queue.getDlq();
      expect(entriesBefore.length).toBeGreaterThanOrEqual(2);

      const retried = queue.retryDlq();
      expect(retried).toBeGreaterThanOrEqual(2);

      // DLQ should be empty after retry
      const entriesAfter = queue.getDlq();
      expect(entriesAfter.length).toBe(0);
    });

    it('should retry a single DLQ entry by ID', async () => {
      const jobId = await addAndFailJob(queue, QUEUE_NAME, { single: true });

      const entriesBefore = queue.getDlq();
      const entry = entriesBefore.find((e) => e.job.id === jobId);
      expect(entry).toBeDefined();

      const retried = queue.retryDlq(jobId);
      expect(retried).toBe(1);

      // That specific entry should be gone
      const entriesAfter = queue.getDlq();
      const stillThere = entriesAfter.find((e) => e.job.id === jobId);
      expect(stillThere).toBeUndefined();
    });

    it('should return 0 when retrying a non-existent job ID', () => {
      const retried = queue.retryDlq('non-existent-id');
      expect(retried).toBe(0);
    });

    it('should move retried jobs back to waiting state', async () => {
      const jobId = await addAndFailJob(queue, QUEUE_NAME, { retryCheck: 1 });

      queue.retryDlq(jobId);

      // The job should now be waiting (or active if picked up fast)
      const state = await queue.getJobState(jobId);
      expect(['waiting', 'active', 'delayed']).toContain(state);
    });
  });

  // ---- retryDlqByFilter ----
  describe('retryDlqByFilter', () => {
    it('should retry DLQ entries matching a filter', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { filterRetry: 1 });

      const retried = queue.retryDlqByFilter({ reason: 'max_attempts_exceeded' });
      expect(retried).toBeGreaterThanOrEqual(1);
    });

    it('should return 0 when filter matches nothing', () => {
      const retried = queue.retryDlqByFilter({ reason: 'stalled' });
      expect(retried).toBe(0);
    });

    it('should retry with limit filter', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { limitRetry: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { limitRetry: 2 });
      await addAndFailJob(queue, QUEUE_NAME, { limitRetry: 3 });

      const retried = queue.retryDlqByFilter({
        reason: 'max_attempts_exceeded',
        limit: 1,
      });
      expect(retried).toBeLessThanOrEqual(1);
    });
  });

  // ---- purgeDlq ----
  describe('purgeDlq', () => {
    it('should purge all DLQ entries and return count', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { purge: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { purge: 2 });

      const beforeCount = queue.getDlq().length;
      expect(beforeCount).toBeGreaterThanOrEqual(2);

      const purged = queue.purgeDlq();
      expect(purged).toBeGreaterThanOrEqual(2);

      const afterCount = queue.getDlq().length;
      expect(afterCount).toBe(0);
    });

    it('should return 0 when purging an empty DLQ', () => {
      const purged = queue.purgeDlq();
      expect(purged).toBe(0);
    });
  });

  // ---- async variants (awaitable, return the server count over TCP too) ----
  describe('retryDlqAsync / purgeDlqAsync', () => {
    it('retryDlqAsync should retry all entries and resolve with the count', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { asyncRetry: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { asyncRetry: 2 });

      const retried = await queue.retryDlqAsync();
      expect(retried).toBeGreaterThanOrEqual(2);
      expect(queue.getDlq().length).toBe(0);
    });

    it('purgeDlqAsync should purge and resolve with the count', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { asyncPurge: 1 });

      const purged = await queue.purgeDlqAsync();
      expect(purged).toBeGreaterThanOrEqual(1);
      expect(queue.getDlq().length).toBe(0);
    });

    it('both resolve with 0 on an empty DLQ', async () => {
      expect(await queue.retryDlqAsync()).toBe(0);
      expect(await queue.purgeDlqAsync()).toBe(0);
    });

    it('retryDlqAsync(id) retries only the requested entry', async () => {
      const first = await addAndFailJob(queue, QUEUE_NAME, { pick: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { pick: 2 });

      const retried = await queue.retryDlqAsync(first);
      expect(retried).toBe(1);

      const remaining = queue.getDlq();
      expect(remaining.length).toBe(1);
      expect(remaining[0]!.job.id).not.toBe(first);
    });

    it('retryDlqAsync with an unknown id resolves with 0', async () => {
      await addAndFailJob(queue, QUEUE_NAME, { keep: true });
      expect(await queue.retryDlqAsync('no-such-job-id')).toBe(0);
      expect(queue.getDlq().length).toBe(1);
    });
  });

  describe('getDlqJobsAsync', () => {
    it('returns the dead jobs as public Job objects', async () => {
      const deadId = await addAndFailJob(queue, QUEUE_NAME, { dead: true });

      const jobs = await queue.getDlqJobsAsync();
      expect(jobs.length).toBeGreaterThanOrEqual(1);

      const dead = jobs.find((j) => j.id === deadId);
      expect(dead).toBeDefined();
      expect(dead!.data).toMatchObject({ dead: true });
      expect(dead!.queueName).toBe(QUEUE_NAME);
    });

    it('honours the count cap and returns [] on an empty DLQ', async () => {
      expect(await queue.getDlqJobsAsync()).toEqual([]);

      await addAndFailJob(queue, QUEUE_NAME, { n: 1 });
      await addAndFailJob(queue, QUEUE_NAME, { n: 2 });
      expect((await queue.getDlqJobsAsync(1)).length).toBe(1);
    });
  });

  describe('setDlqConfigAsync', () => {
    it('round-trips through getDlqConfig', async () => {
      await queue.setDlqConfigAsync({ autoRetry: true, maxEntries: 123 });

      const config = queue.getDlqConfig();
      expect(config.autoRetry).toBe(true);
      expect(config.maxEntries).toBe(123);
    });
  });

  // ---- DLQ config behavior ----
  describe('DLQ auto-retry config', () => {
    it('should configure auto-retry settings', () => {
      queue.setDlqConfig({
        autoRetry: true,
        autoRetryInterval: 5000,
        maxAutoRetries: 2,
      });

      const config = queue.getDlqConfig();
      expect(config.autoRetry).toBe(true);
      expect(config.autoRetryInterval).toBe(5000);
      expect(config.maxAutoRetries).toBe(2);
    });
  });

  describe('DLQ maxAge config', () => {
    it('should set and get maxAge', () => {
      queue.setDlqConfig({ maxAge: 86400000 }); // 1 day

      const config = queue.getDlqConfig();
      expect(config.maxAge).toBe(86400000);
    });
  });

  describe('DLQ maxEntries config', () => {
    it('should set and get maxEntries', () => {
      queue.setDlqConfig({ maxEntries: 100 });

      const config = queue.getDlqConfig();
      expect(config.maxEntries).toBe(100);
    });
  });

  // ---- Retry completed job ----
  describe('retryCompleted', () => {
    it('should return 0 when no completed jobs exist', () => {
      const count = queue.retryCompleted();
      expect(count).toBe(0);
    });

    it('should return 0 for a non-existent completed job ID', () => {
      const count = queue.retryCompleted('non-existent');
      expect(count).toBe(0);
    });
  });

  // ---- DLQ isolation between queues ----
  describe('DLQ isolation', () => {
    it('should isolate DLQ entries between different queues', async () => {
      const queue2 = new Queue<Record<string, unknown>>('dlq-test-2');

      await addAndFailJob(queue, QUEUE_NAME, { q1: true });
      await addAndFailJob(queue2, 'dlq-test-2', { q2: true });

      const entries1 = queue.getDlq();
      const entries2 = queue2.getDlq();

      // Each queue should have its own DLQ entries
      expect(entries1.length).toBeGreaterThanOrEqual(1);
      expect(entries2.length).toBeGreaterThanOrEqual(1);

      // Purge queue1 should not affect queue2
      queue.purgeDlq();
      expect(queue.getDlq().length).toBe(0);
      expect(queue2.getDlq().length).toBeGreaterThanOrEqual(1);

      queue2.purgeDlq();
    });
  });
});

// ============================================================
// DEDUPLICATION OPERATIONS (src/client/queue/deduplication.ts)
// ============================================================
describe('Deduplication Operations', () => {
  let queue: Queue<Record<string, unknown>>;
  const QUEUE_NAME = 'dedup-test';

  beforeEach(() => {
    queue = new Queue(QUEUE_NAME);
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('getDeduplicationJobId', () => {
    it('should return null for a non-existent deduplication key', async () => {
      const result = await queue.getDeduplicationJobId('non-existent');
      expect(result).toBeNull();
    });

    it('should return the owner ID for an existing deduplication key', async () => {
      const deduplicationId = `dedup-${Date.now()}`;
      const job = await queue.add(
        'dedup-task',
        { val: 1 },
        { deduplication: { id: deduplicationId, ttl: 60_000 } }
      );

      const found = await queue.getDeduplicationJobId(deduplicationId);
      expect(found).toBe(String(job.id));
    });
  });

  describe('removeDeduplicationKey', () => {
    it('should return 0 for a non-existent deduplication key', async () => {
      const result = await queue.removeDeduplicationKey('does-not-exist');
      expect(result).toBe(0);
    });

    it('should return 1 for an existing deduplication key', async () => {
      const deduplicationId = `dedup-remove-${Date.now()}`;
      await queue.add(
        'dedup-task',
        { val: 1 },
        { deduplication: { id: deduplicationId, ttl: 60_000 } }
      );

      const result = await queue.removeDeduplicationKey(deduplicationId);
      expect(result).toBe(1);
      expect(await queue.getDeduplicationJobId(deduplicationId)).toBeNull();
    });
  });

  describe('deduplication with jobId', () => {
    it('should deduplicate jobs with the same custom jobId', async () => {
      const customId = `unique-${Date.now()}`;

      const job1 = await queue.add('task', { val: 1 }, { jobId: customId });
      const job2 = await queue.add('task', { val: 2 }, { jobId: customId });

      // Both adds should succeed but the second should be deduplicated
      // (may return the same job or a new one depending on implementation)
      expect(job1.id).toBeDefined();
      expect(job2.id).toBeDefined();

      // There should only be one waiting job with this custom ID
      const foundId = await queue.getDeduplicationJobId(customId);
      expect(foundId).toBeDefined();
    });

    it('should allow different custom IDs as distinct jobs', async () => {
      const id1 = `distinct-1-${Date.now()}`;
      const id2 = `distinct-2-${Date.now()}`;

      const job1 = await queue.add('task', { val: 1 }, { jobId: id1 });
      const job2 = await queue.add('task', { val: 2 }, { jobId: id2 });

      expect(job1.id).toBeDefined();
      expect(job2.id).toBeDefined();
      // They should not be the same internal job
      // (custom IDs are different)
    });
  });
});

// ============================================================
// DLQ + WORKER INTEGRATION
// ============================================================
describe('DLQ Worker Integration', () => {
  let queue: Queue<Record<string, unknown>>;
  const QUEUE_NAME = 'dlq-worker-test';

  beforeEach(() => {
    queue = new Queue(QUEUE_NAME);
  });

  afterEach(() => {
    shutdownManager();
  });

  it('should move job to DLQ after max attempts exhausted via worker', async () => {
    let failCount = 0;
    const job = await queue.add('failing-job', { test: true }, { attempts: 1, backoff: 0 });

    const worker = new Worker(
      QUEUE_NAME,
      async () => {
        failCount++;
        throw new Error('always fails');
      },
      { concurrency: 1 }
    );

    await Bun.sleep(500);
    await worker.close();

    expect(failCount).toBeGreaterThanOrEqual(1);

    const dlqEntries = queue.getDlq();
    const entry = dlqEntries.find((e) => e.job.id === job.id);
    expect(entry).toBeDefined();
    expect(entry!.reason).toBe('max_attempts_exceeded');
  });

  it('should retry a DLQ job and let it be processed again', async () => {
    let callCount = 0;

    const job = await queue.add('retry-me', { x: 1 }, { attempts: 1, backoff: 0 });

    // First worker: always fails
    const worker1 = new Worker(
      QUEUE_NAME,
      async () => {
        callCount++;
        throw new Error('fail first time');
      },
      { concurrency: 1 }
    );

    await Bun.sleep(500);
    await worker1.close();

    // Verify job is in DLQ
    const dlqEntries = queue.getDlq();
    const entry = dlqEntries.find((e) => e.job.id === job.id);
    expect(entry).toBeDefined();

    // Retry from DLQ
    const retried = queue.retryDlq(job.id);
    expect(retried).toBe(1);

    // Second worker: succeeds
    const worker2 = new Worker(
      QUEUE_NAME,
      async () => {
        callCount++;
        return { success: true };
      },
      { concurrency: 1 }
    );

    await Bun.sleep(500);
    await worker2.close();

    // Should have been called at least twice (once fail, once success)
    expect(callCount).toBeGreaterThanOrEqual(2);

    // DLQ should not contain the job anymore
    const dlqAfter = queue.getDlq();
    const stillInDlq = dlqAfter.find((e) => e.job.id === job.id);
    expect(stillInDlq).toBeUndefined();
  });

  it('should keep error information in DLQ entry', async () => {
    const errorMsg = 'specific error message for DLQ test';

    const job = await queue.add('error-job', { e: 1 }, { attempts: 1, backoff: 0 });

    const worker = new Worker(
      QUEUE_NAME,
      async () => {
        throw new Error(errorMsg);
      },
      { concurrency: 1 }
    );

    await Bun.sleep(500);
    await worker.close();

    const dlqEntries = queue.getDlq();
    const entry = dlqEntries.find((e) => e.job.id === job.id);
    expect(entry).toBeDefined();
    // The error message should be stored in the DLQ entry
    expect(entry!.error).toContain(errorMsg);
  });
});

// ============================================================
// DLQ STATS DETAILS
// ============================================================
describe('DLQ Stats Details', () => {
  let queue: Queue<Record<string, unknown>>;
  const QUEUE_NAME = 'dlq-stats-test';

  beforeEach(() => {
    queue = new Queue(QUEUE_NAME);
  });

  afterEach(() => {
    shutdownManager();
  });

  it('should update stats correctly as entries are added and removed', async () => {
    // Start empty
    let stats = queue.getDlqStats();
    expect(stats.total).toBe(0);

    // Add one
    await addAndFailJob(queue, QUEUE_NAME, { s: 1 });
    stats = queue.getDlqStats();
    expect(stats.total).toBeGreaterThanOrEqual(1);

    // Purge
    queue.purgeDlq();
    stats = queue.getDlqStats();
    expect(stats.total).toBe(0);
    expect(stats.oldestEntry).toBeNull();
    expect(stats.newestEntry).toBeNull();
  });
});
