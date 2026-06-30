/**
 * Comprehensive tests for:
 * - jobManagement.ts (cancel, promote, update, changePriority, discard)
 * - queryOperations.ts (getJob, getJobState, getJobResult, getJobs, getJobCounts, count)
 * - queueControl.ts (pause, resume, drain, obliterate)
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { JobId } from '../src/domain/types/job';

describe('Job Management Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============================================================
  // jobManagement.ts
  // ============================================================

  describe('cancelJob', () => {
    test('should cancel a waiting job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'cancel-me' } });
      const result = await qm.cancel(job.id);

      expect(result).toBe(true);

      // Job should no longer be retrievable
      const found = await qm.getJob(job.id);
      expect(found).toBeNull();
    });

    test('should return false for non-existent job', async () => {
      const fakeId = 'non-existent-id' as JobId;
      const result = await qm.cancel(fakeId);
      expect(result).toBe(false);
    });

    test('should not cancel an active job (returns false)', async () => {
      await qm.push('test-queue', { data: { msg: 'active-job' } });
      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();

      // Active jobs have location type 'processing', cancelJob only handles 'queue'
      const result = await qm.cancel(pulled!.id);
      expect(result).toBe(false);
    });

    test('should cancel a delayed job', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'delayed-cancel' },
        delay: 60000,
      });

      const result = await qm.cancel(job.id);
      expect(result).toBe(true);

      const found = await qm.getJob(job.id);
      expect(found).toBeNull();
    });

    test('should remove cancelled job from queue count', async () => {
      await qm.push('test-queue', { data: { msg: 'job1' } });
      const job2 = await qm.push('test-queue', { data: { msg: 'job2' } });
      await qm.push('test-queue', { data: { msg: 'job3' } });

      expect(qm.count('test-queue')).toBe(3);

      await qm.cancel(job2.id);

      expect(qm.count('test-queue')).toBe(2);
    });

    test('should release unique key on cancel', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'unique-job' },
        uniqueKey: 'my-unique-key',
      });

      await qm.cancel(job.id);

      // Should be able to push with same unique key now
      const newJob = await qm.push('test-queue', {
        data: { msg: 'new-unique-job' },
        uniqueKey: 'my-unique-key',
      });

      expect(newJob.id).not.toBe(job.id);
    });
  });

  describe('promoteJob', () => {
    test('should promote a delayed job to immediate', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'promote-me' },
        delay: 60000,
      });

      // Verify job is delayed
      const stateBefore = await qm.getJobState(job.id);
      expect(stateBefore).toBe('delayed');

      const result = await qm.promote(job.id);
      expect(result).toBe(true);

      // Now the job should be pullable (waiting)
      const pulled = await qm.pull('test-queue', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.id).toBe(job.id);
    });

    test('should return false for non-delayed job (waiting)', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'already-waiting' } });

      const result = await qm.promote(job.id);
      expect(result).toBe(false);
    });

    test('should return false for non-existent job', async () => {
      const result = await qm.promote('non-existent' as JobId);
      expect(result).toBe(false);
    });

    test('should return false for active job', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });
      const pulled = await qm.pull('test-queue');

      const result = await qm.promote(pulled!.id);
      expect(result).toBe(false);
    });
  });

  describe('updateJobData', () => {
    test('should update data of a waiting job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'original' } });

      const result = await qm.updateJobData(job.id, { msg: 'updated' });
      expect(result).toBe(true);

      const found = await qm.getJob(job.id);
      expect(found).not.toBeNull();
      expect(found!.data).toEqual({ msg: 'updated' });
    });

    test('should update data of an active job', async () => {
      await qm.push('test-queue', { data: { msg: 'original' } });
      const pulled = await qm.pull('test-queue');

      const result = await qm.updateJobData(pulled!.id, { msg: 'updated-active' });
      expect(result).toBe(true);

      const found = await qm.getJob(pulled!.id);
      expect(found).not.toBeNull();
      expect(found!.data).toEqual({ msg: 'updated-active' });
    });

    test('should return false for non-existent job', async () => {
      const result = await qm.updateJobData('non-existent' as JobId, { msg: 'nope' });
      expect(result).toBe(false);
    });
  });

  describe('changeJobPriority', () => {
    test('should change priority of a waiting job', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'change-priority' },
        priority: 5,
      });

      const result = await qm.changePriority(job.id, 100);
      expect(result).toBe(true);

      const found = await qm.getJob(job.id);
      expect(found).not.toBeNull();
      expect(found!.priority).toBe(100);
    });

    test('should affect pull order after priority change', async () => {
      const lowJob = await qm.push('test-queue', {
        data: { id: 'was-low' },
        priority: 1,
      });
      await qm.push('test-queue', {
        data: { id: 'high' },
        priority: 10,
      });

      // Raise the low priority job above the high one
      await qm.changePriority(lowJob.id, 100);

      const first = await qm.pull('test-queue');
      expect((first?.data as { id: string }).id).toBe('was-low');
    });

    test('should return false for non-existent job', async () => {
      const result = await qm.changePriority('non-existent' as JobId, 10);
      expect(result).toBe(false);
    });

    test('should return false for active job', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });
      const pulled = await qm.pull('test-queue');

      const result = await qm.changePriority(pulled!.id, 50);
      expect(result).toBe(false);
    });
  });

  describe('discardJob', () => {
    test('should discard a waiting job to DLQ', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'discard-me' } });

      const result = await qm.discard(job.id);
      expect(result).toBe(true);

      // Should appear in DLQ
      const dlqJobs = qm.getDlq('test-queue');
      expect(dlqJobs.length).toBe(1);
      expect(dlqJobs[0].id).toBe(job.id);
    });

    test('should discard an active job to DLQ', async () => {
      await qm.push('test-queue', { data: { msg: 'discard-active' } });
      const pulled = await qm.pull('test-queue');

      const result = await qm.discard(pulled!.id);
      expect(result).toBe(true);

      const dlqJobs = qm.getDlq('test-queue');
      expect(dlqJobs.length).toBe(1);
      expect(dlqJobs[0].id).toBe(pulled!.id);
    });

    test('should return false for non-existent job', async () => {
      const result = await qm.discard('non-existent' as JobId);
      expect(result).toBe(false);
    });

    test('should remove discarded job from queue count', async () => {
      await qm.push('test-queue', { data: { msg: 'keep' } });
      const toDiscard = await qm.push('test-queue', { data: { msg: 'discard' } });

      expect(qm.count('test-queue')).toBe(2);

      await qm.discard(toDiscard.id);

      expect(qm.count('test-queue')).toBe(1);
    });
  });

  describe('updateJobProgress', () => {
    test('should update progress of an active job', async () => {
      await qm.push('test-queue', { data: { msg: 'progress' } });
      const pulled = await qm.pull('test-queue');

      const result = await qm.updateProgress(pulled!.id, 50, 'halfway done');
      expect(result).toBe(true);

      const progress = qm.getProgress(pulled!.id);
      expect(progress).not.toBeNull();
      expect(progress!.progress).toBe(50);
      expect(progress!.message).toBe('halfway done');
    });

    test('should clamp progress to 0-100', async () => {
      await qm.push('test-queue', { data: { msg: 'clamp' } });
      const pulled = await qm.pull('test-queue');

      await qm.updateProgress(pulled!.id, 150);
      let progress = qm.getProgress(pulled!.id);
      expect(progress!.progress).toBe(100);

      await qm.updateProgress(pulled!.id, -10);
      progress = qm.getProgress(pulled!.id);
      expect(progress!.progress).toBe(0);
    });

    test('should return false for waiting job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'waiting' } });
      const result = await qm.updateProgress(job.id, 50);
      expect(result).toBe(false);
    });
  });

  describe('moveJobToDelayed', () => {
    test('should move an active job back to delayed', async () => {
      await qm.push('test-queue', { data: { msg: 'delay-me' } });
      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();

      const result = await qm.moveToDelayed(pulled!.id, 60000);
      expect(result).toBe(true);

      // Job should now be delayed
      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('delayed');
    });

    test('should move a waiting job to delayed', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'delay-waiting' } });
      // In-queue (waiting/prioritized/delayed) jobs are routed through changeDelay
      // so they become delayed rather than a silent no-op (matches embedded/TCP).
      const result = await qm.moveToDelayed(job.id, 5000);
      expect(result).toBe(true);
      expect(await qm.getJobState(job.id)).toBe('delayed');
    });

    test('should return false for non-existent job', async () => {
      const result = await qm.moveToDelayed('non-existent' as JobId, 5000);
      expect(result).toBe(false);
    });
  });

  // ============================================================
  // queryOperations.ts
  // ============================================================

  describe('getJob', () => {
    test('should get a waiting job by ID', async () => {
      const pushed = await qm.push('test-queue', { data: { msg: 'find-me' } });

      const found = await qm.getJob(pushed.id);
      expect(found).not.toBeNull();
      expect(found!.id).toBe(pushed.id);
      expect(found!.data).toEqual({ msg: 'find-me' });
      expect(found!.queue).toBe('test-queue');
    });

    test('should get an active job by ID', async () => {
      await qm.push('test-queue', { data: { msg: 'active-find' } });
      const pulled = await qm.pull('test-queue');

      const found = await qm.getJob(pulled!.id);
      expect(found).not.toBeNull();
      expect(found!.id).toBe(pulled!.id);
      expect(found!.startedAt).not.toBeNull();
    });

    test('should return null for non-existent job', async () => {
      const found = await qm.getJob('non-existent' as JobId);
      expect(found).toBeNull();
    });

    test('should get a delayed job by ID', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'delayed-find' },
        delay: 60000,
      });

      const found = await qm.getJob(job.id);
      expect(found).not.toBeNull();
      expect(found!.id).toBe(job.id);
      expect(found!.runAt).toBeGreaterThan(Date.now());
    });
  });

  describe('getJobState', () => {
    test('should return "waiting" for a waiting job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'waiting' } });
      const state = await qm.getJobState(job.id);
      expect(state).toBe('waiting');
    });

    test('should return "delayed" for a delayed job', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'delayed' },
        delay: 60000,
      });
      const state = await qm.getJobState(job.id);
      expect(state).toBe('delayed');
    });

    test('should return "active" for an active job', async () => {
      await qm.push('test-queue', { data: { msg: 'active' } });
      const pulled = await qm.pull('test-queue');
      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('active');
    });

    test('should return "completed" for a completed job', async () => {
      await qm.push('test-queue', { data: { msg: 'complete-me' } });
      const pulled = await qm.pull('test-queue');
      await qm.ack(pulled!.id, { done: true });

      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('completed');
    });

    test('should return "unknown" for non-existent job', async () => {
      const state = await qm.getJobState('non-existent' as JobId);
      expect(state).toBe('unknown');
    });
  });

  describe('getJobResult', () => {
    test('should get result of a completed job', async () => {
      await qm.push('test-queue', { data: { msg: 'result-test' } });
      const pulled = await qm.pull('test-queue');
      await qm.ack(pulled!.id, { result: 'success', value: 42 });

      const result = qm.getResult(pulled!.id);
      expect(result).toEqual({ result: 'success', value: 42 });
    });

    test('should return undefined for incomplete job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'no-result' } });
      const result = qm.getResult(job.id);
      expect(result).toBeUndefined();
    });

    test('should return undefined for non-existent job', async () => {
      const result = qm.getResult('non-existent' as JobId);
      expect(result).toBeUndefined();
    });
  });

  describe('getJobs (with filters and pagination)', () => {
    test('should get all waiting jobs from a queue', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      const jobs = qm.getJobs('test-queue', { state: 'waiting' });
      expect(jobs.length).toBe(3);
    });

    test('should get delayed jobs only', async () => {
      await qm.push('test-queue', { data: { id: 'immediate' } });
      await qm.push('test-queue', { data: { id: 'delayed1' }, delay: 60000 });
      await qm.push('test-queue', { data: { id: 'delayed2' }, delay: 60000 });

      const jobs = qm.getJobs('test-queue', { state: 'delayed' });
      expect(jobs.length).toBe(2);
    });

    test('should get active jobs only', async () => {
      await qm.push('test-queue', { data: { id: 'active1' } });
      await qm.push('test-queue', { data: { id: 'active2' } });
      await qm.push('test-queue', { data: { id: 'waiting' } });

      await qm.pull('test-queue');
      await qm.pull('test-queue');

      const jobs = qm.getJobs('test-queue', { state: 'active' });
      expect(jobs.length).toBe(2);
    });

    test('should support pagination with start and end', async () => {
      for (let i = 0; i < 10; i++) {
        await qm.push('test-queue', { data: { id: i } });
      }

      const page = qm.getJobs('test-queue', { state: 'waiting', start: 2, end: 5 });
      expect(page.length).toBe(3);
    });

    test('should support ascending sort order (default)', async () => {
      const job1 = await qm.push('test-queue', { data: { id: 'first' } });
      const job2 = await qm.push('test-queue', { data: { id: 'second' } });

      const jobs = qm.getJobs('test-queue', { state: 'waiting', asc: true });
      expect(jobs.length).toBe(2);
      // Ascending: older first
      expect(jobs[0].id).toBe(job1.id);
      expect(jobs[1].id).toBe(job2.id);
    });

    test('should support descending sort order', async () => {
      // Use explicit timestamps to guarantee distinct createdAt values
      const now = Date.now();
      const job1 = await qm.push('test-queue', { data: { id: 'first' }, timestamp: now - 1000 });
      const job2 = await qm.push('test-queue', { data: { id: 'second' }, timestamp: now });

      const jobsDesc = qm.getJobs('test-queue', { state: 'waiting', asc: false });
      expect(jobsDesc.length).toBe(2);
      // Descending: newer first
      expect(jobsDesc[0].id).toBe(job2.id);
      expect(jobsDesc[1].id).toBe(job1.id);
    });

    test('should return empty array for empty queue', async () => {
      const jobs = qm.getJobs('empty-queue', { state: 'waiting' });
      expect(jobs.length).toBe(0);
    });

    test('should return all states when no state filter provided', async () => {
      await qm.push('test-queue', { data: { id: 'waiting' } });
      await qm.push('test-queue', { data: { id: 'delayed' }, delay: 60000 });
      await qm.push('test-queue', { data: { id: 'to-active' } });
      await qm.pull('test-queue');

      const jobs = qm.getJobs('test-queue');
      // Should include waiting (1) + delayed (1) + active (1)
      expect(jobs.length).toBe(3);
    });
  });

  describe('getQueueJobCounts', () => {
    test('should return counts per state', async () => {
      // Add 3 waiting
      await qm.push('test-queue', { data: { id: 'w1' } });
      await qm.push('test-queue', { data: { id: 'w2' } });
      await qm.push('test-queue', { data: { id: 'w3' } });
      // Add 1 delayed
      await qm.push('test-queue', { data: { id: 'd1' }, delay: 60000 });

      // Pull one to make it active (FIFO, so w1 gets pulled)
      await qm.pull('test-queue');

      const counts = qm.getQueueJobCounts('test-queue');
      expect(counts.waiting).toBe(2); // w2 and w3 remain waiting
      expect(counts.delayed).toBe(1);
      expect(counts.active).toBe(1);
    });

    test('should return zero counts for empty queue', async () => {
      const counts = qm.getQueueJobCounts('empty-queue');
      expect(counts.waiting).toBe(0);
      expect(counts.delayed).toBe(0);
      expect(counts.active).toBe(0);
      expect(counts.failed).toBe(0);
    });

    test('should count failed jobs (DLQ)', async () => {
      await qm.push('test-queue', { data: { msg: 'fail-me' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-queue');
      await qm.fail(pulled!.id, 'intentional failure');

      const counts = qm.getQueueJobCounts('test-queue');
      expect(counts.failed).toBe(1);
    });
  });

  describe('count', () => {
    test('should count jobs in a queue', async () => {
      expect(qm.count('test-queue')).toBe(0);

      await qm.push('test-queue', { data: { id: 1 } });
      expect(qm.count('test-queue')).toBe(1);

      await qm.push('test-queue', { data: { id: 2 } });
      expect(qm.count('test-queue')).toBe(2);
    });

    test('should decrease count after pull', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      expect(qm.count('test-queue')).toBe(2);
      await qm.pull('test-queue');
      expect(qm.count('test-queue')).toBe(1);
    });

    test('should include delayed jobs in count', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 }, delay: 60000 });

      expect(qm.count('test-queue')).toBe(2);
    });

    test('should return 0 for non-existent queue', async () => {
      expect(qm.count('non-existent')).toBe(0);
    });
  });

  describe('getJobProgress', () => {
    test('should return null for non-processing job', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'nope' } });
      const progress = qm.getProgress(job.id);
      expect(progress).toBeNull();
    });

    test('should return default progress for active job', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });
      const pulled = await qm.pull('test-queue');

      const progress = qm.getProgress(pulled!.id);
      expect(progress).not.toBeNull();
      expect(progress!.progress).toBe(0);
      expect(progress!.message).toBeNull();
    });
  });

  describe('getJobByCustomId', () => {
    test('should find job by custom ID', async () => {
      const job = await qm.push('test-queue', {
        data: { msg: 'custom' },
        customId: 'my-custom-id',
      });

      const found = qm.getJobByCustomId('my-custom-id');
      expect(found).not.toBeNull();
      expect(found!.id).toBe(job.id);
    });

    test('should return null for non-existent custom ID', async () => {
      const found = qm.getJobByCustomId('does-not-exist');
      expect(found).toBeNull();
    });
  });

  // ============================================================
  // queueControl.ts
  // ============================================================

  describe('pauseQueue', () => {
    test('should pause a queue', async () => {
      qm.pause('test-queue');

      expect(qm.isPaused('test-queue')).toBe(true);
    });

    test('should prevent pulls on paused queue', async () => {
      await qm.push('test-queue', { data: { msg: 'paused-pull' } });

      qm.pause('test-queue');

      const pulled = await qm.pull('test-queue', 0);
      expect(pulled).toBeNull();
    });

    test('should still allow push to paused queue', async () => {
      qm.pause('test-queue');

      const job = await qm.push('test-queue', { data: { msg: 'pushed-while-paused' } });
      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
      expect(qm.count('test-queue')).toBe(1);
    });
  });

  describe('resumeQueue', () => {
    test('should resume a paused queue', async () => {
      qm.pause('test-queue');
      expect(qm.isPaused('test-queue')).toBe(true);

      qm.resume('test-queue');
      expect(qm.isPaused('test-queue')).toBe(false);
    });

    test('should allow pulls after resume', async () => {
      await qm.push('test-queue', { data: { msg: 'resume-pull' } });

      qm.pause('test-queue');
      const nullPull = await qm.pull('test-queue', 0);
      expect(nullPull).toBeNull();

      qm.resume('test-queue');
      const pulled = await qm.pull('test-queue', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.data).toEqual({ msg: 'resume-pull' });
    });

    test('should be no-op on non-paused queue', async () => {
      expect(qm.isPaused('test-queue')).toBe(false);
      qm.resume('test-queue');
      expect(qm.isPaused('test-queue')).toBe(false);
    });
  });

  describe('drainQueue', () => {
    test('should drain all waiting jobs from queue', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      expect(qm.count('test-queue')).toBe(3);

      const drained = qm.drain('test-queue');
      expect(drained).toBe(3);
      expect(qm.count('test-queue')).toBe(0);
    });

    test('should return 0 for empty queue', async () => {
      const drained = qm.drain('empty-queue');
      expect(drained).toBe(0);
    });

    test('should not affect active jobs', async () => {
      await qm.push('test-queue', { data: { id: 'active' } });
      await qm.push('test-queue', { data: { id: 'waiting1' } });
      await qm.push('test-queue', { data: { id: 'waiting2' } });

      // Pull one job to make it active
      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();

      // Drain remaining
      const drained = qm.drain('test-queue');
      expect(drained).toBe(2);

      // Active job should still be retrievable
      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('active');
    });

    test('should drain delayed jobs too', async () => {
      await qm.push('test-queue', { data: { id: 'immediate' } });
      await qm.push('test-queue', { data: { id: 'delayed' }, delay: 60000 });

      const drained = qm.drain('test-queue');
      expect(drained).toBe(2);
      expect(qm.count('test-queue')).toBe(0);
    });

    test('should clean up jobIndex for drained jobs', async () => {
      const job1 = await qm.push('test-queue', { data: { id: 1 } });
      const job2 = await qm.push('test-queue', { data: { id: 2 } });

      qm.drain('test-queue');

      // Drained jobs should not be findable
      const found1 = await qm.getJob(job1.id);
      const found2 = await qm.getJob(job2.id);
      expect(found1).toBeNull();
      expect(found2).toBeNull();
    });
  });

  describe('obliterateQueue', () => {
    test('should remove all queue data', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      qm.obliterate('test-queue');

      expect(qm.count('test-queue')).toBe(0);

      const jobs = qm.getJobs('test-queue');
      expect(jobs.length).toBe(0);
    });

    test('should remove DLQ entries too', async () => {
      await qm.push('test-queue', { data: { msg: 'fail-me' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-queue');
      await qm.fail(pulled!.id, 'intentional');

      const dlqBefore = qm.getDlq('test-queue');
      expect(dlqBefore.length).toBe(1);

      qm.obliterate('test-queue');

      const dlqAfter = qm.getDlq('test-queue');
      expect(dlqAfter.length).toBe(0);
    });

    test('should remove queue from listed queues', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      expect(qm.listQueues()).toContain('test-queue');

      qm.obliterate('test-queue');
      expect(qm.listQueues()).not.toContain('test-queue');
    });

    test('should handle obliterate on non-existent queue', async () => {
      // Should not throw
      qm.obliterate('non-existent-queue');
    });
  });

  describe('cleanQueue', () => {
    test('should clean old waiting jobs beyond grace period', async () => {
      // Push jobs - these will have createdAt = Date.now()
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      // With a grace of 0ms, all jobs are "old"
      const cleaned = qm.clean('test-queue', 0);
      expect(cleaned.length).toBe(2);
      expect(qm.count('test-queue')).toBe(0);
    });

    test('should not clean jobs within grace period', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      // Very large grace period - no jobs should be cleaned
      const cleaned = qm.clean('test-queue', 999999999);
      expect(cleaned).toEqual([]);
      expect(qm.count('test-queue')).toBe(2);
    });

    test('should respect limit parameter', async () => {
      for (let i = 0; i < 5; i++) {
        await qm.push('test-queue', { data: { id: i } });
      }

      // Clean at most 2 jobs
      const cleaned = qm.clean('test-queue', 0, undefined, 2);
      expect(cleaned.length).toBeLessThanOrEqual(2);
    });
  });

  describe('listAllQueues', () => {
    test('should list all known queues', async () => {
      await qm.push('queue-a', { data: { id: 1 } });
      await qm.push('queue-b', { data: { id: 2 } });
      await qm.push('queue-c', { data: { id: 3 } });

      const queues = qm.listQueues();
      expect(queues).toContain('queue-a');
      expect(queues).toContain('queue-b');
      expect(queues).toContain('queue-c');
    });

    test('should return empty array when no queues exist', async () => {
      const queues = qm.listQueues();
      expect(queues.length).toBe(0);
    });
  });

  describe('getCountsPerPriority', () => {
    test('should return counts grouped by priority', async () => {
      await qm.push('test-queue', { data: { id: 1 }, priority: 1 });
      await qm.push('test-queue', { data: { id: 2 }, priority: 1 });
      await qm.push('test-queue', { data: { id: 3 }, priority: 5 });
      await qm.push('test-queue', { data: { id: 4 }, priority: 10 });

      const counts = qm.getCountsPerPriority('test-queue');
      expect(counts[1]).toBe(2);
      expect(counts[5]).toBe(1);
      expect(counts[10]).toBe(1);
    });
  });

  // ============================================================
  // Integration tests across operations
  // ============================================================

  describe('Cross-operation integration', () => {
    test('cancel then re-push same data works', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'reusable' } });
      await qm.cancel(job.id);

      const newJob = await qm.push('test-queue', { data: { msg: 'reusable' } });
      expect(newJob.id).not.toBe(job.id);
      expect(qm.count('test-queue')).toBe(1);
    });

    test('drain then push new jobs works', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      qm.drain('test-queue');
      expect(qm.count('test-queue')).toBe(0);

      await qm.push('test-queue', { data: { id: 3 } });
      expect(qm.count('test-queue')).toBe(1);

      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();
      expect((pulled!.data as { id: number }).id).toBe(3);
    });

    test('pause, push, resume, pull flow', async () => {
      qm.pause('test-queue');
      await qm.push('test-queue', { data: { msg: 'queued-while-paused' } });

      // Cannot pull while paused
      const nullPull = await qm.pull('test-queue', 0);
      expect(nullPull).toBeNull();

      qm.resume('test-queue');

      // Now can pull
      const pulled = await qm.pull('test-queue', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.data).toEqual({ msg: 'queued-while-paused' });
    });

    test('promote then pull immediately', async () => {
      const delayed = await qm.push('test-queue', {
        data: { msg: 'promote-and-pull' },
        delay: 60000,
      });

      // Cannot pull delayed
      let pulled = await qm.pull('test-queue', 0);
      expect(pulled).toBeNull();

      // Promote
      await qm.promote(delayed.id);

      // Now can pull
      pulled = await qm.pull('test-queue', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.id).toBe(delayed.id);
    });

    test('change priority affects pull order among many jobs', async () => {
      const jobs = [];
      for (let i = 0; i < 5; i++) {
        jobs.push(await qm.push('test-queue', { data: { id: i }, priority: i }));
      }

      // Job 0 has lowest priority, boost it to highest
      await qm.changePriority(jobs[0].id, 100);

      const pulled = await qm.pull('test-queue');
      expect((pulled!.data as { id: number }).id).toBe(0);
    });

    test('full lifecycle: push, pull, update progress, ack, get result', async () => {
      const job = await qm.push('test-queue', { data: { msg: 'lifecycle' } });

      // State: waiting
      expect(await qm.getJobState(job.id)).toBe('waiting');

      const pulled = await qm.pull('test-queue');

      // State: active
      expect(await qm.getJobState(pulled!.id)).toBe('active');

      // Update progress
      await qm.updateProgress(pulled!.id, 50, 'processing');
      const progress = qm.getProgress(pulled!.id);
      expect(progress!.progress).toBe(50);

      // Ack
      await qm.ack(pulled!.id, { status: 'done' });

      // State: completed
      expect(await qm.getJobState(pulled!.id)).toBe('completed');

      // Get result
      const result = qm.getResult(pulled!.id);
      expect(result).toEqual({ status: 'done' });
    });

    test('discard from active then retry from DLQ', async () => {
      await qm.push('test-queue', { data: { msg: 'discard-retry' } });
      const pulled = await qm.pull('test-queue');

      // Discard to DLQ
      await qm.discard(pulled!.id);
      expect(qm.getDlq('test-queue').length).toBe(1);

      // Retry from DLQ
      const retried = qm.retryDlq('test-queue');
      expect(retried).toBe(1);

      // Job should be back in queue
      expect(qm.count('test-queue')).toBe(1);

      // Should be pullable again
      const rePulled = await qm.pull('test-queue');
      expect(rePulled).not.toBeNull();
      expect(rePulled!.data).toEqual({ msg: 'discard-retry' });
    });
  });
});
