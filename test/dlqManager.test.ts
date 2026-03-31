/**
 * DLQ Manager Tests
 * Dead Letter Queue operations
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('DLQ Manager', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('getDlq', () => {
    test('should return empty array when DLQ is empty', () => {
      const jobs = qm.getDlq('test-queue');
      expect(jobs).toEqual([]);
    });

    test('should return jobs in DLQ after max attempts', async () => {
      await qm.push('test-queue', {
        data: { msg: 'will fail' },
        maxAttempts: 1,
      });

      const pulled = await qm.pull('test-queue');
      await qm.fail(pulled!.id, 'Test error');

      const dlqJobs = qm.getDlq('test-queue');
      expect(dlqJobs.length).toBe(1);
      expect(dlqJobs[0].data).toEqual({ msg: 'will fail' });
    });

    test('should limit returned jobs with count parameter', async () => {
      // Push and fail 3 jobs
      for (let i = 0; i < 3; i++) {
        await qm.push('test-queue', { data: { id: i }, maxAttempts: 1 });
        const job = await qm.pull('test-queue');
        await qm.fail(job!.id, 'Error');
      }

      const dlqJobs = qm.getDlq('test-queue', 2);
      expect(dlqJobs.length).toBe(2);
    });
  });

  describe('retryDlq', () => {
    test('should retry single job from DLQ', async () => {
      await qm.push('test-queue', {
        data: { msg: 'retry me' },
        maxAttempts: 1,
      });

      const pulled = await qm.pull('test-queue');
      const jobId = pulled!.id;
      await qm.fail(jobId, 'Error');

      // Verify in DLQ
      expect(qm.getDlq('test-queue').length).toBe(1);

      // Retry
      const count = qm.retryDlq('test-queue', jobId);
      expect(count).toBe(1);

      // Should be back in queue
      expect(qm.getDlq('test-queue').length).toBe(0);
      const retried = await qm.pull('test-queue');
      expect(retried).not.toBeNull();
      expect(retried!.attempts).toBe(0); // Reset attempts
    });

    test('should retry all jobs from DLQ', async () => {
      // Push and fail 3 jobs
      for (let i = 0; i < 3; i++) {
        await qm.push('test-queue', { data: { id: i }, maxAttempts: 1 });
        const job = await qm.pull('test-queue');
        await qm.fail(job!.id, 'Error');
      }

      expect(qm.getDlq('test-queue').length).toBe(3);

      // Retry all
      const count = qm.retryDlq('test-queue');
      expect(count).toBe(3);

      // DLQ should be empty
      expect(qm.getDlq('test-queue').length).toBe(0);

      // All jobs should be back in queue
      const stats = qm.getStats();
      expect(stats.waiting).toBe(3);
    });

    test('should return 0 for non-existent job', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const count = qm.retryDlq('test-queue', jobId('non-existent'));
      expect(count).toBe(0);
    });
  });

  describe('purgeDlq', () => {
    test('should remove all jobs from DLQ', async () => {
      // Push and fail 3 jobs
      for (let i = 0; i < 3; i++) {
        await qm.push('test-queue', { data: { id: i }, maxAttempts: 1 });
        const job = await qm.pull('test-queue');
        await qm.fail(job!.id, 'Error');
      }

      expect(qm.getDlq('test-queue').length).toBe(3);

      const purged = qm.purgeDlq('test-queue');
      expect(purged).toBe(3);
      expect(qm.getDlq('test-queue').length).toBe(0);
    });

    test('should return 0 for empty DLQ', () => {
      const purged = qm.purgeDlq('test-queue');
      expect(purged).toBe(0);
    });
  });

  describe('DLQ isolation', () => {
    test('should isolate DLQ per queue', async () => {
      // Fail job in queue1
      await qm.push('queue1', { data: { q: 1 }, maxAttempts: 1 });
      const job1 = await qm.pull('queue1');
      await qm.fail(job1!.id, 'Error');

      // Fail job in queue2
      await qm.push('queue2', { data: { q: 2 }, maxAttempts: 1 });
      const job2 = await qm.pull('queue2');
      await qm.fail(job2!.id, 'Error');

      expect(qm.getDlq('queue1').length).toBe(1);
      expect(qm.getDlq('queue2').length).toBe(1);

      // Purge only queue1
      qm.purgeDlq('queue1');

      expect(qm.getDlq('queue1').length).toBe(0);
      expect(qm.getDlq('queue2').length).toBe(1);
    });
  });
});
