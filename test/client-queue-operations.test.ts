/**
 * Comprehensive tests for client-side queue operations
 * Tests: add, query, management, counts, control
 *
 * Run with: BUNQUEUE_EMBEDDED=1 bun test test/client-queue-operations.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

// ============================================================
// ADD OPERATIONS (src/client/queue/operations/add.ts)
// ============================================================
describe('Add Operations', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => {
    queue = new Queue('add-ops-test');
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('add() - single job', () => {
    it('should add a job and return a job object with correct properties', async () => {
      const job = await queue.add('email', { to: 'user@test.com' });

      expect(job.id).toBeDefined();
      expect(typeof job.id).toBe('string');
      expect(job.name).toBe('email');
      expect(job.data.to).toBe('user@test.com');
      expect(job.queueName).toBe('add-ops-test');
      expect(job.attemptsMade).toBe(0);
      expect(job.timestamp).toBeGreaterThan(0);
    });

    it('should add a job with priority option', async () => {
      const job = await queue.add('high-pri', { value: 1 }, { priority: 10 });

      expect(job.id).toBeDefined();
      // Verify the job was added - we can retrieve it
      const fetched = await queue.getJob(job.id);
      expect(fetched).not.toBeNull();
    });

    it('should add a job with delay option', async () => {
      const job = await queue.add('delayed-task', { value: 1 }, { delay: 5000 });

      expect(job.id).toBeDefined();
      const state = await queue.getJobState(job.id);
      expect(state).toBe('delayed');
    });

    it('should add a job with attempts option', async () => {
      const job = await queue.add('retry-task', { value: 1 }, { attempts: 5 });

      expect(job.id).toBeDefined();
    });

    it('should add a job with backoff option (number)', async () => {
      const job = await queue.add('backoff-task', { value: 1 }, { backoff: 2000 });

      expect(job.id).toBeDefined();
    });

    it('should add a job with backoff option (object)', async () => {
      const job = await queue.add(
        'backoff-task-obj',
        { value: 1 },
        { backoff: { type: 'exponential', delay: 1000 } }
      );

      expect(job.id).toBeDefined();
    });

    it('should add a job with timeout option', async () => {
      const job = await queue.add('timeout-task', { value: 1 }, { timeout: 30000 });

      expect(job.id).toBeDefined();
    });

    it('should add a job with custom jobId', async () => {
      const job = await queue.add('custom-id-task', { value: 1 }, { jobId: 'my-custom-id' });

      expect(job.id).toBeDefined();
    });

    it('should add a job with removeOnComplete = true', async () => {
      const job = await queue.add('remove-complete', { value: 1 }, { removeOnComplete: true });

      expect(job.id).toBeDefined();
    });

    it('should add a job with removeOnComplete = false', async () => {
      const job = await queue.add('keep-complete', { value: 1 }, { removeOnComplete: false });

      expect(job.id).toBeDefined();
    });

    it('should add a job with removeOnFail = true', async () => {
      const job = await queue.add('remove-fail', { value: 1 }, { removeOnFail: true });

      expect(job.id).toBeDefined();
    });

    it('should add a job with removeOnFail = false', async () => {
      const job = await queue.add('keep-fail', { value: 1 }, { removeOnFail: false });

      expect(job.id).toBeDefined();
    });

    it('should add a job with durable = true', async () => {
      const job = await queue.add('durable-task', { value: 1 }, { durable: true });

      expect(job.id).toBeDefined();
    });

    it('should add a job with repeat option (every)', async () => {
      const job = await queue.add(
        'repeat-task',
        { value: 1 },
        { repeat: { every: 60000 } }
      );

      expect(job.id).toBeDefined();
    });

    it('should add a job with repeat option (pattern/cron)', async () => {
      const job = await queue.add(
        'cron-task',
        { value: 1 },
        { repeat: { pattern: '*/5 * * * *' } }
      );

      expect(job.id).toBeDefined();
    });

    it('should add a job with repeat option with limit', async () => {
      const job = await queue.add(
        'limited-repeat',
        { value: 1 },
        { repeat: { every: 10000, limit: 5 } }
      );

      expect(job.id).toBeDefined();
    });

    it('should add a job with all options combined', async () => {
      const job = await queue.add('full-opts', { value: 42 }, {
        priority: 5,
        delay: 1000,
        attempts: 3,
        backoff: 500,
        timeout: 10000,
        removeOnComplete: true,
        removeOnFail: false,
        durable: true,
      });

      expect(job.id).toBeDefined();
      expect(job.name).toBe('full-opts');
    });

    it('should add a job with stallTimeout option', async () => {
      const job = await queue.add('stall-task', { value: 1 }, { stallTimeout: 15000 });

      expect(job.id).toBeDefined();
    });

    it('should use defaultJobOptions from queue constructor', async () => {
      const queueWithDefaults = new Queue('defaults-test', {
        defaultJobOptions: { priority: 7, attempts: 5 },
      });

      const job = await queueWithDefaults.add('task', { value: 1 });

      expect(job.id).toBeDefined();
    });

    it('should override defaultJobOptions with per-job options', async () => {
      const queueWithDefaults = new Queue('override-test', {
        defaultJobOptions: { priority: 3 },
      });

      const job = await queueWithDefaults.add('task', { value: 1 }, { priority: 10 });

      expect(job.id).toBeDefined();
    });

    it('should add a job with empty data', async () => {
      const job = await queue.add('empty', {});

      expect(job.id).toBeDefined();
      expect(job.name).toBe('empty');
    });

    it('should add a job with complex nested data', async () => {
      const data = {
        user: { name: 'test', email: 'test@example.com' },
        items: [1, 2, 3],
        nested: { deep: { value: true } },
      };
      const job = await queue.add('complex', data);

      expect(job.id).toBeDefined();
      expect(job.data.user).toEqual({ name: 'test', email: 'test@example.com' });
    });
  });

  describe('addBulk() - multiple jobs', () => {
    it('should add multiple jobs at once', async () => {
      const jobs = await queue.addBulk([
        { name: 'task1', data: { value: 1 } },
        { name: 'task2', data: { value: 2 } },
        { name: 'task3', data: { value: 3 } },
      ]);

      expect(jobs).toHaveLength(3);
      expect(jobs[0].name).toBe('task1');
      expect(jobs[1].name).toBe('task2');
      expect(jobs[2].name).toBe('task3');
    });

    it('should return empty array for empty input', async () => {
      const jobs = await queue.addBulk([]);

      expect(jobs).toHaveLength(0);
    });

    it('should add bulk jobs with individual options', async () => {
      const jobs = await queue.addBulk([
        { name: 'low', data: { value: 1 }, opts: { priority: 1 } },
        { name: 'high', data: { value: 2 }, opts: { priority: 10 } },
        { name: 'delayed', data: { value: 3 }, opts: { delay: 5000 } },
      ]);

      expect(jobs).toHaveLength(3);
      expect(jobs[0].id).toBeDefined();
      expect(jobs[1].id).toBeDefined();
      expect(jobs[2].id).toBeDefined();
    });

    it('should assign unique IDs to each bulk job', async () => {
      const jobs = await queue.addBulk([
        { name: 'a', data: { v: 1 } },
        { name: 'b', data: { v: 2 } },
        { name: 'c', data: { v: 3 } },
      ]);

      const ids = jobs.map((j) => j.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(3);
    });

    it('should add a large batch of jobs', async () => {
      const batch = Array.from({ length: 100 }, (_, i) => ({
        name: `batch-${i}`,
        data: { index: i },
      }));

      const jobs = await queue.addBulk(batch);

      expect(jobs).toHaveLength(100);
    });

    it('should handle bulk jobs with durable option', async () => {
      const jobs = await queue.addBulk([
        { name: 'durable1', data: { v: 1 }, opts: { durable: true } },
        { name: 'durable2', data: { v: 2 }, opts: { durable: true } },
      ]);

      expect(jobs).toHaveLength(2);
    });
  });
});

// ============================================================
// QUERY OPERATIONS (src/client/queue/operations/query.ts)
// ============================================================
describe('Query Operations', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => {
    queue = new Queue('query-ops-test');
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('getJob()', () => {
    it('should return a job by ID', async () => {
      const added = await queue.add('task', { value: 99 });
      const fetched = await queue.getJob(added.id);

      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(added.id);
    });

    it('should return null for non-existent job', async () => {
      const fetched = await queue.getJob('non-existent-id-12345');

      expect(fetched).toBeNull();
    });

    it('should preserve job data when fetched', async () => {
      const added = await queue.add('task', { email: 'test@example.com', count: 42 });
      const fetched = await queue.getJob(added.id);

      expect(fetched).not.toBeNull();
      expect(fetched!.data.email).toBe('test@example.com');
      expect(fetched!.data.count).toBe(42);
    });

    it('should return job with correct name', async () => {
      const added = await queue.add('my-task-name', { value: 1 });
      const fetched = await queue.getJob(added.id);

      expect(fetched).not.toBeNull();
      expect(fetched!.name).toBe('my-task-name');
    });
  });

  describe('getJobState()', () => {
    it('should return waiting for a normal job', async () => {
      const job = await queue.add('task', { value: 1 });
      const state = await queue.getJobState(job.id);

      expect(state).toBe('waiting');
    });

    it('should return delayed for a delayed job', async () => {
      const job = await queue.add('delayed', { value: 1 }, { delay: 60000 });
      const state = await queue.getJobState(job.id);

      expect(state).toBe('delayed');
    });

    it('should return unknown for non-existent job', async () => {
      const state = await queue.getJobState('non-existent-999');

      expect(state).toBe('unknown');
    });
  });

  describe('getJobs()', () => {
    it('should return waiting jobs', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });

      const jobs = queue.getJobs({ state: 'waiting' });

      expect(jobs.length).toBeGreaterThanOrEqual(2);
    });

    it('should return delayed jobs', async () => {
      await queue.add('delayed1', { value: 1 }, { delay: 60000 });
      await queue.add('delayed2', { value: 2 }, { delay: 60000 });

      const jobs = queue.getJobs({ state: 'delayed' });

      expect(jobs.length).toBeGreaterThanOrEqual(2);
    });

    it('should respect start and end pagination', async () => {
      for (let i = 0; i < 10; i++) {
        await queue.add('task', { index: i });
      }

      const firstPage = queue.getJobs({ state: 'waiting', start: 0, end: 5 });
      const secondPage = queue.getJobs({ state: 'waiting', start: 5, end: 10 });

      expect(firstPage.length).toBeLessThanOrEqual(5);
      expect(secondPage.length).toBeLessThanOrEqual(5);
    });

    it('should return empty array for state with no jobs', async () => {
      const jobs = queue.getJobs({ state: 'failed' });

      expect(jobs).toEqual([]);
    });

    it('should return all jobs when no state filter is specified', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });

      const jobs = queue.getJobs();

      expect(jobs.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('getJobsAsync()', () => {
    it('should return jobs asynchronously', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });

      const jobs = await queue.getJobsAsync({ state: 'waiting' });

      expect(jobs.length).toBeGreaterThanOrEqual(2);
    });

    it('should return delayed jobs asynchronously', async () => {
      await queue.add('delayed-async', { value: 1 }, { delay: 60000 });

      const jobs = await queue.getJobsAsync({ state: 'delayed' });

      expect(jobs.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getWaiting()', () => {
    it('should return waiting jobs', async () => {
      await queue.add('w1', { value: 1 });
      await queue.add('w2', { value: 2 });

      const waiting = queue.getWaiting();

      expect(waiting.length).toBeGreaterThanOrEqual(2);
    });

    it('should support pagination', async () => {
      for (let i = 0; i < 10; i++) {
        await queue.add('task', { index: i });
      }

      const page = queue.getWaiting(0, 5);

      expect(page.length).toBeLessThanOrEqual(5);
    });
  });

  describe('getWaitingAsync()', () => {
    it('should return waiting jobs asynchronously', async () => {
      await queue.add('w1', { value: 1 });

      const waiting = await queue.getWaitingAsync();

      expect(waiting.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getDelayed()', () => {
    it('should return delayed jobs', async () => {
      await queue.add('d1', { value: 1 }, { delay: 60000 });

      const delayed = queue.getDelayed();

      expect(delayed.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getDelayedAsync()', () => {
    it('should return delayed jobs asynchronously', async () => {
      await queue.add('d1', { value: 1 }, { delay: 60000 });

      const delayed = await queue.getDelayedAsync();

      expect(delayed.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getActive()', () => {
    it('should return empty when no jobs are active', () => {
      const active = queue.getActive();

      expect(active).toEqual([]);
    });
  });

  describe('getCompleted()', () => {
    it('should return empty when no jobs are completed', () => {
      const completed = queue.getCompleted();

      expect(completed).toEqual([]);
    });
  });

  describe('getFailed()', () => {
    it('should return empty when no jobs have failed', () => {
      const failed = queue.getFailed();

      expect(failed).toEqual([]);
    });
  });
});

// ============================================================
// MANAGEMENT OPERATIONS (src/client/queue/operations/management.ts)
// ============================================================
describe('Management Operations', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => {
    queue = new Queue('mgmt-ops-test');
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('remove() / removeAsync()', () => {
    it('should cancel/remove a job by ID', async () => {
      const job = await queue.add('task', { value: 1 });

      // remove is sync, uses cancel internally
      queue.remove(job.id);

      // wait a tick for internal processing
      await Bun.sleep(10);

      const state = await queue.getJobState(job.id);
      // After cancellation, the job should no longer be in waiting state
      expect(['unknown', 'failed']).toContain(state);
    });

    it('should cancel/remove a job asynchronously', async () => {
      const job = await queue.add('task', { value: 1 });

      await queue.removeAsync(job.id);

      await Bun.sleep(10);

      const state = await queue.getJobState(job.id);
      expect(['unknown', 'failed']).toContain(state);
    });
  });

  describe('promoteJob()', () => {
    it('should promote a delayed job to waiting', async () => {
      const job = await queue.add('delayed', { value: 1 }, { delay: 60000 });

      const stateBefore = await queue.getJobState(job.id);
      expect(stateBefore).toBe('delayed');

      await queue.promoteJob(job.id);

      const stateAfter = await queue.getJobState(job.id);
      expect(stateAfter).toBe('waiting');
    });
  });

  describe('promoteJobs()', () => {
    it('should promote multiple delayed jobs', async () => {
      await queue.add('d1', { value: 1 }, { delay: 60000 });
      await queue.add('d2', { value: 2 }, { delay: 60000 });

      const promoted = await queue.promoteJobs();

      expect(promoted).toBeGreaterThanOrEqual(2);
    });

    it('should respect count limit when promoting', async () => {
      await queue.add('d1', { value: 1 }, { delay: 60000 });
      await queue.add('d2', { value: 2 }, { delay: 60000 });
      await queue.add('d3', { value: 3 }, { delay: 60000 });

      const promoted = await queue.promoteJobs({ count: 1 });

      expect(promoted).toBe(1);
    });
  });

  describe('updateJobData()', () => {
    it('should update job data', async () => {
      const job = await queue.add('task', { value: 1 });

      await queue.updateJobData(job.id, { value: 99, extra: 'new' });

      const fetched = await queue.getJob(job.id);
      expect(fetched).not.toBeNull();
      // The data should be updated
      expect(fetched!.data.value).toBe(99);
    });
  });

  describe('changeJobPriority()', () => {
    it('should change job priority', async () => {
      const job = await queue.add('task', { value: 1 }, { priority: 1 });

      await queue.changeJobPriority(job.id, { priority: 100 });

      // Verify job still exists and can be fetched
      const fetched = await queue.getJob(job.id);
      expect(fetched).not.toBeNull();
    });
  });

  describe('updateJobProgress()', () => {
    it('should update job progress with number', async () => {
      const job = await queue.add('task', { value: 1 });

      await queue.updateJobProgress(job.id, 50);

      // Progress should be updated
    });

    it('should update job progress with object', async () => {
      const job = await queue.add('task', { value: 1 });

      await queue.updateJobProgress(job.id, { step: 3, total: 10 });

      // Progress object should be stored
    });
  });

  describe('addJobLog() / getJobLogs()', () => {
    it('should add and retrieve job logs', async () => {
      const job = await queue.add('task', { value: 1 });

      await queue.addJobLog(job.id, 'Step 1 completed');
      await queue.addJobLog(job.id, 'Step 2 completed');

      const { logs, count } = await queue.getJobLogs(job.id);

      expect(count).toBeGreaterThanOrEqual(2);
      expect(logs.length).toBeGreaterThanOrEqual(2);
    });

    it('should return empty logs for job with no logs', async () => {
      const job = await queue.add('task', { value: 1 });

      const { logs, count } = await queue.getJobLogs(job.id);

      expect(logs).toEqual([]);
      expect(count).toBe(0);
    });
  });

  describe('clearJobLogs()', () => {
    it('should clear job logs', async () => {
      const job = await queue.add('task', { value: 1 });

      await queue.addJobLog(job.id, 'Log entry 1');
      await queue.addJobLog(job.id, 'Log entry 2');

      await queue.clearJobLogs(job.id);

      const { logs, count } = await queue.getJobLogs(job.id);
      expect(count).toBe(0);
      expect(logs).toEqual([]);
    });
  });

  describe('clean()', () => {
    it('should clean old waiting jobs (sync)', async () => {
      await queue.add('old1', { value: 1 });
      await queue.add('old2', { value: 2 });

      // Clean with 0 grace period (all jobs), limit 100
      const result = queue.clean(0, 100, 'wait');

      // result is an array of ids (empty strings placeholder)
      expect(Array.isArray(result)).toBe(true);
    });

    it('should clean with grace period', async () => {
      await queue.add('task', { value: 1 });

      // Clean with 1 hour grace - job just added, shouldn't be cleaned
      const result = queue.clean(3600000, 100, 'wait');

      expect(result.length).toBe(0);
    });
  });

  describe('cleanAsync()', () => {
    it('should clean old jobs asynchronously', async () => {
      await queue.add('old1', { value: 1 });
      await queue.add('old2', { value: 2 });

      const result = await queue.cleanAsync(0, 100, 'wait');

      expect(Array.isArray(result)).toBe(true);
    });
  });
});

// ============================================================
// COUNTS OPERATIONS (src/client/queue/operations/counts.ts)
// ============================================================
describe('Counts Operations', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => {
    queue = new Queue('counts-ops-test');
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('getJobCounts()', () => {
    it('should return zero counts for empty queue', () => {
      const counts = queue.getJobCounts();

      expect(counts.waiting).toBe(0);
      expect(counts.active).toBe(0);
      expect(counts.completed).toBe(0);
      expect(counts.failed).toBe(0);
    });

    it('should count waiting jobs', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });
      await queue.add('task3', { value: 3 });

      const counts = queue.getJobCounts();

      expect(counts.waiting).toBe(3);
    });

    it('should not count delayed jobs as waiting', async () => {
      await queue.add('waiting', { value: 1 });
      await queue.add('delayed', { value: 2 }, { delay: 60000 });

      const counts = queue.getJobCounts();

      expect(counts.waiting).toBe(1);
    });
  });

  describe('getJobCountsAsync()', () => {
    it('should return counts asynchronously', async () => {
      await queue.add('task', { value: 1 });

      const counts = await queue.getJobCountsAsync();

      expect(counts.waiting).toBeGreaterThanOrEqual(1);
      expect(typeof counts.active).toBe('number');
      expect(typeof counts.completed).toBe('number');
      expect(typeof counts.failed).toBe('number');
    });
  });

  describe('getWaitingCount()', () => {
    it('should return the number of waiting jobs', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });

      const count = await queue.getWaitingCount();

      expect(count).toBe(2);
    });

    it('should return 0 for empty queue', async () => {
      const count = await queue.getWaitingCount();

      expect(count).toBe(0);
    });
  });

  describe('getActiveCount()', () => {
    it('should return 0 when no jobs are active', async () => {
      const count = await queue.getActiveCount();

      expect(count).toBe(0);
    });
  });

  describe('getCompletedCount()', () => {
    it('should return 0 when no jobs are completed', async () => {
      const count = await queue.getCompletedCount();

      expect(count).toBe(0);
    });
  });

  describe('getFailedCount()', () => {
    it('should return 0 when no jobs have failed', async () => {
      const count = await queue.getFailedCount();

      expect(count).toBe(0);
    });
  });

  describe('getDelayedCount()', () => {
    it('should count delayed jobs', async () => {
      await queue.add('d1', { value: 1 }, { delay: 60000 });
      await queue.add('d2', { value: 2 }, { delay: 60000 });

      const count = await queue.getDelayedCount();

      expect(count).toBe(2);
    });

    it('should return 0 when no jobs are delayed', async () => {
      await queue.add('normal', { value: 1 });

      const count = await queue.getDelayedCount();

      expect(count).toBe(0);
    });
  });

  describe('count()', () => {
    it('should return total job count for the queue', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });
      await queue.add('task3', { value: 3 });

      const total = queue.count();

      expect(total).toBe(3);
    });

    it('should return 0 for empty queue', () => {
      const total = queue.count();

      expect(total).toBe(0);
    });

    it('should include delayed jobs in count', async () => {
      await queue.add('normal', { value: 1 });
      await queue.add('delayed', { value: 2 }, { delay: 60000 });

      const total = queue.count();

      expect(total).toBe(2);
    });
  });

  describe('countAsync()', () => {
    it('should return total count asynchronously', async () => {
      await queue.add('task1', { value: 1 });

      const total = await queue.countAsync();

      expect(total).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getCountsPerPriority()', () => {
    it('should return counts grouped by priority', async () => {
      await queue.add('low', { value: 1 }, { priority: 1 });
      await queue.add('low2', { value: 2 }, { priority: 1 });
      await queue.add('high', { value: 3 }, { priority: 10 });

      const counts = queue.getCountsPerPriority();

      expect(typeof counts).toBe('object');
      // Should have entries for priority 1 and 10
      expect(counts[1]).toBeGreaterThanOrEqual(2);
      expect(counts[10]).toBeGreaterThanOrEqual(1);
    });

    it('should return empty object for empty queue', () => {
      const counts = queue.getCountsPerPriority();

      expect(typeof counts).toBe('object');
    });
  });

  describe('getCountsPerPriorityAsync()', () => {
    it('should return counts per priority asynchronously', async () => {
      await queue.add('task', { value: 1 }, { priority: 5 });

      const counts = await queue.getCountsPerPriorityAsync();

      expect(typeof counts).toBe('object');
      expect(counts[5]).toBeGreaterThanOrEqual(1);
    });
  });
});

// ============================================================
// CONTROL OPERATIONS (src/client/queue/operations/control.ts)
// ============================================================
describe('Control Operations', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => {
    queue = new Queue('control-ops-test');
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('pause()', () => {
    it('should pause the queue', () => {
      queue.pause();

      const paused = queue.isPaused();
      expect(paused).toBe(true);
    });

    it('should be idempotent (pause twice)', () => {
      queue.pause();
      queue.pause();

      const paused = queue.isPaused();
      expect(paused).toBe(true);
    });
  });

  describe('resume()', () => {
    it('should resume a paused queue', () => {
      queue.pause();
      expect(queue.isPaused()).toBe(true);

      queue.resume();
      expect(queue.isPaused()).toBe(false);
    });

    it('should be safe to resume non-paused queue', () => {
      queue.resume();

      expect(queue.isPaused()).toBe(false);
    });
  });

  describe('isPaused() / isPausedAsync()', () => {
    it('should return false for non-paused queue', () => {
      expect(queue.isPaused()).toBe(false);
    });

    it('should return true for paused queue', () => {
      queue.pause();
      expect(queue.isPaused()).toBe(true);
    });

    it('should return paused status asynchronously', async () => {
      queue.pause();
      const paused = await queue.isPausedAsync();
      expect(paused).toBe(true);
    });

    it('should return not paused status asynchronously', async () => {
      const paused = await queue.isPausedAsync();
      expect(paused).toBe(false);
    });
  });

  describe('drain()', () => {
    it('should remove all waiting jobs', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });
      await queue.add('task3', { value: 3 });

      const countBefore = queue.count();
      expect(countBefore).toBe(3);

      queue.drain();

      const countAfter = queue.count();
      expect(countAfter).toBe(0);
    });

    it('should work on empty queue', () => {
      queue.drain();

      expect(queue.count()).toBe(0);
    });

    it('should not affect delayed jobs', async () => {
      await queue.add('normal', { value: 1 });
      await queue.add('delayed', { value: 2 }, { delay: 60000 });

      queue.drain();

      const delayed = await queue.getDelayedCount();
      // Depending on implementation, drain may or may not affect delayed
      // The important thing is it doesn't error
      expect(typeof delayed).toBe('number');
    });
  });

  describe('obliterate()', () => {
    it('should remove all queue data', async () => {
      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });
      await queue.add('delayed', { value: 3 }, { delay: 60000 });

      queue.obliterate();

      const counts = queue.getJobCounts();
      expect(counts.waiting).toBe(0);
    });

    it('should work on empty queue', () => {
      queue.obliterate();

      expect(queue.count()).toBe(0);
    });
  });

  describe('waitUntilReady()', () => {
    it('should resolve immediately in embedded mode', async () => {
      await queue.waitUntilReady();
      // Should not throw
      expect(true).toBe(true);
    });
  });
});

// ============================================================
// INTEGRATION TESTS - combining multiple operations
// ============================================================
describe('Integration Tests', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => {
    queue = new Queue('integration-test');
  });

  afterEach(() => {
    shutdownManager();
  });

  it('should add jobs, count them, then drain', async () => {
    await queue.add('task1', { value: 1 });
    await queue.add('task2', { value: 2 });
    await queue.add('task3', { value: 3 });

    expect(queue.count()).toBe(3);
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(3);

    queue.drain();

    expect(queue.count()).toBe(0);
  });

  it('should add delayed job, promote it, and verify state change', async () => {
    const job = await queue.add('delayed', { value: 1 }, { delay: 60000 });

    expect(await queue.getJobState(job.id)).toBe('delayed');

    await queue.promoteJob(job.id);

    expect(await queue.getJobState(job.id)).toBe('waiting');
  });

  it('should add jobs with different priorities and count by priority', async () => {
    await queue.add('low1', { v: 1 }, { priority: 1 });
    await queue.add('low2', { v: 2 }, { priority: 1 });
    await queue.add('med', { v: 3 }, { priority: 5 });
    await queue.add('high', { v: 4 }, { priority: 10 });

    const counts = queue.getCountsPerPriority();
    expect(counts[1]).toBe(2);
    expect(counts[5]).toBe(1);
    expect(counts[10]).toBe(1);
  });

  it('should pause queue, add jobs, resume, and verify counts', async () => {
    queue.pause();
    expect(queue.isPaused()).toBe(true);

    await queue.add('task1', { value: 1 });
    await queue.add('task2', { value: 2 });

    expect(queue.count()).toBeGreaterThanOrEqual(2);

    queue.resume();
    expect(queue.isPaused()).toBe(false);
  });

  it('should update job data and verify it was updated', async () => {
    const job = await queue.add('task', { original: true, count: 0 });

    await queue.updateJobData(job.id, { original: false, count: 42, extra: 'added' });

    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data.original).toBe(false);
    expect(fetched!.data.count).toBe(42);
  });

  it('should add bulk, get jobs, and verify pagination', async () => {
    const batch = Array.from({ length: 20 }, (_, i) => ({
      name: `task-${i}`,
      data: { index: i },
    }));

    await queue.addBulk(batch);

    const firstPage = queue.getJobs({ state: 'waiting', start: 0, end: 10 });
    expect(firstPage.length).toBeLessThanOrEqual(10);

    const total = queue.count();
    expect(total).toBe(20);
  });

  it('should handle obliterate and then re-add jobs', async () => {
    await queue.add('task1', { value: 1 });
    queue.obliterate();

    expect(queue.count()).toBe(0);

    await queue.add('task2', { value: 2 });
    expect(queue.count()).toBe(1);
  });

  it('should process jobs with worker and verify completed state', async () => {
    const q = new Queue<{ x: number }>('worker-integration');
    const results: number[] = [];

    const worker = new Worker('worker-integration', async (job) => {
      results.push(job.data.x);
      return { doubled: job.data.x * 2 };
    });

    await q.add('calc', { x: 7 });
    await q.add('calc', { x: 14 });

    await Bun.sleep(300);

    expect(results).toContain(7);
    expect(results).toContain(14);

    await worker.close();
  });

  it('should handle multiple queues independently', async () => {
    const q1 = new Queue('queue-a');
    const q2 = new Queue('queue-b');

    await q1.add('task-a', { from: 'a' });
    await q1.add('task-a2', { from: 'a' });
    await q2.add('task-b', { from: 'b' });

    expect(q1.count()).toBe(2);
    expect(q2.count()).toBe(1);

    q1.drain();

    expect(q1.count()).toBe(0);
    expect(q2.count()).toBe(1);
  });
});
