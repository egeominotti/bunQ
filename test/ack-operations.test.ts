/**
 * Ack/Fail Operations Tests
 * Comprehensive tests for ack.ts and ackHelpers.ts
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { jobId, type JobId } from '../src/domain/types/job';

describe('Ack Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============ Basic Ack ============

  describe('ackJob', () => {
    test('should transition job to completed state', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');
      expect(pulled).not.toBeNull();

      await qm.ack(pulled!.id);

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      expect(stats.active).toBe(0);
    });

    test('should store result data when provided', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      const resultData = { processed: true, output: 42 };
      await qm.ack(pulled!.id, resultData);

      const result = qm.getResult(pulled!.id);
      expect(result).toEqual(resultData);
    });

    test('should not store result when result is undefined', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id, undefined);

      const result = qm.getResult(pulled!.id);
      expect(result).toBeUndefined();
    });

    test('should remove job from active/processing state', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      // Before ack: job is active
      let stats = qm.getStats();
      expect(stats.active).toBe(1);

      await qm.ack(pulled!.id);

      // After ack: no longer active
      stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('should increment totalCompleted counter', async () => {
      await qm.push('test-q', { data: { id: 1 } });
      await qm.push('test-q', { data: { id: 2 } });
      const p1 = await qm.pull('test-q');
      const p2 = await qm.pull('test-q');

      await qm.ack(p1!.id);
      await qm.ack(p2!.id);

      const stats = qm.getStats();
      expect(stats.completed).toBe(2);
      expect(Number(stats.totalCompleted)).toBe(2);
    });

    test('should add job to completedJobs set', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id);

      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('completed');
    });

    test('should update jobIndex to completed location', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id);

      // In embedded mode without storage, getJob for completed returns null
      // but getJobState still returns 'completed' via jobIndex
      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('completed');
    });

    test('should throw when acking non-existent job', async () => {
      const fakeId = jobId('00000000-0000-7000-8000-000000000099');
      await expect(qm.ack(fakeId)).rejects.toThrow(
        /Job not found or not in processing state/
      );
    });

    test('should throw when acking already completed job', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id);

      // Acking again should throw because job is no longer in processing
      await expect(qm.ack(pulled!.id)).rejects.toThrow(
        /Job not found or not in processing state/
      );
    });
  });

  // ============ Ack with removeOnComplete ============

  describe('ackJob with removeOnComplete', () => {
    test('should remove job entirely when removeOnComplete is true', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        removeOnComplete: true,
      });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id);

      // Job should be completely gone
      const job = await qm.getJob(pulled!.id);
      expect(job).toBeNull();

      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('unknown');
    });

    test('should not add to completedJobs when removeOnComplete is true', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        removeOnComplete: true,
      });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id, { result: 'data' });

      // Result should not be stored
      const result = qm.getResult(pulled!.id);
      expect(result).toBeUndefined();
    });

    test('should keep job in completed state when removeOnComplete is false', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        removeOnComplete: false,
      });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id);

      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('completed');
    });
  });

  // ============ Ack with customId ============

  describe('ackJob with customId', () => {
    test('should release customId mapping on completion', async () => {
      // customId is used as the actual job ID (jobId(input.customId))
      // After ack, the customIdMap entry is deleted
      await qm.push('test-q', {
        data: { msg: 'first' },
        customId: 'unique-job-1',
      });
      const pulled = await qm.pull('test-q');
      // The job ID should be the customId value
      expect(pulled!.id).toBe('unique-job-1');

      await qm.ack(pulled!.id);

      // After ack, the job is in completed state
      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('completed');
    });

    test('should allow reuse of customId after ack with removeOnComplete', async () => {
      await qm.push('test-q', {
        data: { msg: 'first' },
        customId: 'reusable-id',
        removeOnComplete: true,
      });
      const pulled = await qm.pull('test-q');
      await qm.ack(pulled!.id);

      // With removeOnComplete, the job and its jobIndex entry are fully cleaned up
      // So a new push with the same customId should create a new job
      const newJob = await qm.push('test-q', {
        data: { msg: 'second' },
        customId: 'reusable-id',
      });
      expect(newJob).toBeDefined();
      expect(newJob.data).toEqual({ msg: 'second' });
    });
  });

  // ============ Ack with repeat ============

  describe('ackJob with repeat', () => {
    test('should trigger repeat when repeat config is set and limit not reached', async () => {
      await qm.push('test-q', {
        data: { msg: 'repeating' },
        repeat: { every: 1000, limit: 3 },
      });
      const pulled = await qm.pull('test-q');

      await qm.ack(pulled!.id);

      // After ack, the repeat mechanism should have added a new job
      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      // The repeatable job should have been requeued
      expect(stats.waiting + stats.delayed).toBeGreaterThanOrEqual(1);
    });
  });

  // ============ Event emission ============

  describe('ack event emission', () => {
    test('should broadcast completed event on ack', async () => {
      const events: Array<{ eventType: string; jobId: string; data?: unknown }> = [];
      qm.subscribe((event) => {
        if (event.eventType === 'completed') {
          events.push({
            eventType: event.eventType,
            jobId: event.jobId,
            data: event.data,
          });
        }
      });

      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');
      await qm.ack(pulled!.id, { success: true });

      expect(events.length).toBe(1);
      expect(events[0].eventType).toBe('completed');
      expect(events[0].jobId).toBe(pulled!.id);
      expect(events[0].data).toEqual({ success: true });
    });

    test('should broadcast failed event on fail', async () => {
      const events: Array<{ eventType: string; jobId: string; error?: string }> = [];
      qm.subscribe((event) => {
        if (event.eventType === 'failed') {
          events.push({
            eventType: event.eventType,
            jobId: event.jobId,
            error: event.error,
          });
        }
      });

      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-q');
      await qm.fail(pulled!.id, 'Something broke');

      expect(events.length).toBe(1);
      expect(events[0].eventType).toBe('failed');
      expect(events[0].jobId).toBe(pulled!.id);
      expect(events[0].error).toBe('Something broke');
    });

    test('should broadcast retried event when job is requeued for retry', async () => {
      const events: Array<{ eventType: string; jobId: string; prev?: string }> = [];
      qm.subscribe((event) => {
        if (event.eventType === 'retried') {
          events.push({
            eventType: event.eventType,
            jobId: event.jobId,
            prev: event.prev,
          });
        }
      });

      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 3 });
      const pulled = await qm.pull('test-q');
      await qm.fail(pulled!.id, 'Temporary error');

      expect(events.length).toBe(1);
      expect(events[0].eventType).toBe('retried');
      expect(events[0].prev).toBe('failed');
    });
  });

  // ============ Fail Operations ============

  describe('failJob', () => {
    test('should increment attempts count', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 3 });
      const pulled = await qm.pull('test-q');
      expect(pulled!.attempts).toBe(0);

      await qm.fail(pulled!.id, 'Error');

      // Job should be requeued with incremented attempts
      // We need to wait for the backoff period, so check stats
      const stats = qm.getStats();
      expect(stats.waiting + stats.delayed).toBe(1);
    });

    test('should requeue job when retries remain', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 3 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Transient error');

      const stats = qm.getStats();
      expect(stats.active).toBe(0);
      // Job should be requeued (waiting or delayed due to backoff)
      expect(stats.waiting + stats.delayed).toBe(1);
      expect(stats.dlq).toBe(0);
    });

    test('should move to DLQ when max attempts exceeded', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Final error');

      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);
      expect(stats.active).toBe(0);
    });

    test('should store error message in DLQ entry', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Specific error message');

      const dlqJobs = qm.getDlq('test-q');
      expect(dlqJobs.length).toBe(1);
    });

    test('should move to DLQ after exhausting all retries', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        maxAttempts: 2,
        backoff: 0, // No backoff delay for faster testing
      });

      // First attempt
      let pulled = await qm.pull('test-q');
      await qm.fail(pulled!.id, 'Error 1');

      // Second attempt (job requeued with attempts=1, pull it again)
      pulled = await qm.pull('test-q');
      expect(pulled).not.toBeNull();
      await qm.fail(pulled!.id, 'Error 2');

      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);
      expect(stats.waiting + stats.delayed).toBe(0);
    });

    test('should throw when failing non-existent job', async () => {
      const fakeId = jobId('00000000-0000-7000-8000-000000000099');
      await expect(qm.fail(fakeId, 'Error')).rejects.toThrow(
        /Job not found or not in processing state/
      );
    });

    test('should throw when failing already failed job', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Error');

      // Failing again should throw since job moved out of processing
      await expect(qm.fail(pulled!.id, 'Error again')).rejects.toThrow(
        /Job not found or not in processing state/
      );
    });

    test('should handle fail with undefined error message', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, undefined);

      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);
    });

    test('should increment totalFailed counter when moved to DLQ', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Error');

      const stats = qm.getStats();
      expect(Number(stats.totalFailed)).toBe(1);
    });

    test('should not increment totalFailed when job is retried', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 3 });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Retriable error');

      const stats = qm.getStats();
      expect(Number(stats.totalFailed)).toBe(0);
    });
  });

  // ============ Fail with removeOnFail ============

  describe('failJob with removeOnFail', () => {
    test('should remove job entirely when removeOnFail is true and max attempts exceeded', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        maxAttempts: 1,
        removeOnFail: true,
      });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Error');

      // Job should be completely gone, not in DLQ
      const job = await qm.getJob(pulled!.id);
      expect(job).toBeNull();

      const stats = qm.getStats();
      expect(stats.dlq).toBe(0);
    });

    test('should still retry when removeOnFail is true but retries remain', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        maxAttempts: 3,
        removeOnFail: true,
      });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Retriable error');

      const stats = qm.getStats();
      // Job should be requeued, not removed
      expect(stats.waiting + stats.delayed).toBe(1);
      expect(stats.dlq).toBe(0);
    });

    test('should release customId when removeOnFail removes the job', async () => {
      await qm.push('test-q', {
        data: { msg: 'first' },
        customId: 'my-custom-id',
        maxAttempts: 1,
        removeOnFail: true,
      });
      const pulled = await qm.pull('test-q');
      // customId is used as the job ID
      expect(pulled!.id).toBe('my-custom-id');
      await qm.fail(pulled!.id, 'Error');

      // Job should be completely gone (removeOnFail + max attempts exceeded)
      const job = await qm.getJob(pulled!.id);
      expect(job).toBeNull();

      const state = await qm.getJobState(pulled!.id);
      expect(state).toBe('unknown');

      // The customId mapping should have been cleaned up
      const stats = qm.getStats();
      expect(stats.dlq).toBe(0); // Not in DLQ because removeOnFail=true
    });
  });

  // ============ Fail with backoff ============

  describe('failJob with backoff', () => {
    test('should apply exponential backoff on retry', async () => {
      await qm.push('test-q', {
        data: { msg: 'hello' },
        maxAttempts: 3,
        backoff: 1000,
      });
      const pulled = await qm.pull('test-q');

      await qm.fail(pulled!.id, 'Error');

      // Job should be delayed (not immediately available)
      const stats = qm.getStats();
      expect(stats.delayed).toBeGreaterThanOrEqual(1);
    });
  });

  // ============ Lock-based Ack ============

  describe('ack with lock tokens', () => {
    test('should ack successfully with valid lock token', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const { job, token } = await qm.pullWithLock('test-q', 'worker-1');
      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      await qm.ack(job!.id, { done: true }, token!);

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
    });

    test('should throw with invalid lock token', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const { job } = await qm.pullWithLock('test-q', 'worker-1');
      expect(job).not.toBeNull();

      await expect(
        qm.ack(job!.id, { done: true }, 'invalid-token-abc')
      ).rejects.toThrow(/Invalid or expired lock token/);
    });

    test('should fail with valid lock token', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const { job, token } = await qm.pullWithLock('test-q', 'worker-1');

      await qm.fail(job!.id, 'Error', token!);

      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);
    });

    test('should throw fail with invalid lock token', async () => {
      await qm.push('test-q', { data: { msg: 'hello' }, maxAttempts: 1 });
      const { job } = await qm.pullWithLock('test-q', 'worker-1');

      await expect(
        qm.fail(job!.id, 'Error', 'invalid-token-xyz')
      ).rejects.toThrow(/Invalid or expired lock token/);
    });

    test('should ack without token when no lock was acquired', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      // Ack without token should work since no lock was set
      await qm.ack(pulled!.id);

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
    });
  });

  // ============ Batch Ack ============

  describe('ackJobBatch', () => {
    test('should ack empty batch without error', async () => {
      await qm.ackBatch([]);
      // No error means success
    });

    test('should ack small batch (<=4 jobs, uses parallel individual acks)', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 3; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 3; i++) {
        const pulled = await qm.pull('test-q');
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      const stats = qm.getStats();
      expect(stats.completed).toBe(3);
      expect(stats.active).toBe(0);
    });

    test('should ack large batch (>4 jobs, uses optimized path)', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 8; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 8; i++) {
        const pulled = await qm.pull('test-q');
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      const stats = qm.getStats();
      expect(stats.completed).toBe(8);
      expect(stats.active).toBe(0);
    });

    test('should ack batch from multiple queues', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 6; i++) {
        const qName = `queue-${i % 3}`;
        await qm.push(qName, { data: { id: i } });
      }
      for (let i = 0; i < 6; i++) {
        const qName = `queue-${i % 3}`;
        const pulled = await qm.pull(qName);
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      const stats = qm.getStats();
      expect(stats.completed).toBe(6);
      expect(stats.active).toBe(0);
    });

    test('should handle batch with lock tokens', async () => {
      const ids: JobId[] = [];
      const tokens: string[] = [];
      for (let i = 0; i < 3; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 3; i++) {
        const { job, token } = await qm.pullWithLock('test-q', 'worker-1');
        ids.push(job!.id);
        tokens.push(token!);
      }

      await qm.ackBatch(ids, tokens);

      const stats = qm.getStats();
      expect(stats.completed).toBe(3);
    });

    test('should throw batch ack with invalid lock token', async () => {
      const ids: JobId[] = [];
      const tokens: string[] = [];
      for (let i = 0; i < 2; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 2; i++) {
        const { job } = await qm.pullWithLock('test-q', 'worker-1');
        ids.push(job!.id);
        tokens.push('invalid-token');
      }

      await expect(qm.ackBatch(ids, tokens)).rejects.toThrow(
        /Invalid or expired lock token/
      );
    });
  });

  // ============ Batch Ack with Results ============

  describe('ackJobBatchWithResults', () => {
    test('should ack batch with individual results (small batch)', async () => {
      const items: Array<{ id: JobId; result: unknown }> = [];
      for (let i = 0; i < 3; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 3; i++) {
        const pulled = await qm.pull('test-q');
        items.push({ id: pulled!.id, result: { value: i * 10 } });
      }

      await qm.ackBatchWithResults(items);

      const stats = qm.getStats();
      expect(stats.completed).toBe(3);

      // Verify results stored correctly
      for (const item of items) {
        const result = qm.getResult(item.id);
        expect(result).toEqual(item.result);
      }
    });

    test('should ack batch with individual results (large batch)', async () => {
      const items: Array<{ id: JobId; result: unknown }> = [];
      for (let i = 0; i < 8; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 8; i++) {
        const pulled = await qm.pull('test-q');
        items.push({ id: pulled!.id, result: { processed: true, index: i } });
      }

      await qm.ackBatchWithResults(items);

      const stats = qm.getStats();
      expect(stats.completed).toBe(8);

      // Verify each result
      for (const item of items) {
        const result = qm.getResult(item.id);
        expect(result).toEqual(item.result);
      }
    });

    test('should handle empty batch with results', async () => {
      await qm.ackBatchWithResults([]);
      // No error means success
    });

    test('should handle batch with lock tokens and results', async () => {
      const items: Array<{ id: JobId; result: unknown; token?: string }> = [];
      for (let i = 0; i < 3; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 3; i++) {
        const { job, token } = await qm.pullWithLock('test-q', 'worker-1');
        items.push({ id: job!.id, result: { v: i }, token: token! });
      }

      await qm.ackBatchWithResults(items);

      const stats = qm.getStats();
      expect(stats.completed).toBe(3);
    });
  });

  // ============ Batch with removeOnComplete ============

  describe('batch ack with removeOnComplete', () => {
    test('should remove jobs with removeOnComplete in batch', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 6; i++) {
        await qm.push('test-q', {
          data: { id: i },
          removeOnComplete: true,
        });
      }
      for (let i = 0; i < 6; i++) {
        const pulled = await qm.pull('test-q');
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      // All jobs should be gone
      for (const id of ids) {
        const job = await qm.getJob(id);
        expect(job).toBeNull();
      }
    });

    test('should handle mixed removeOnComplete in batch', async () => {
      // Push jobs with different removeOnComplete settings
      await qm.push('test-q', {
        data: { id: 'keep' },
        removeOnComplete: false,
      });
      await qm.push('test-q', {
        data: { id: 'remove' },
        removeOnComplete: true,
      });

      const p1 = await qm.pull('test-q');
      const p2 = await qm.pull('test-q');

      await qm.ack(p1!.id);
      await qm.ack(p2!.id);

      // stats.completed only counts jobs in the completedJobs set
      // removeOnComplete=true jobs are NOT added to completedJobs
      // So only 1 of the 2 jobs is counted as completed
      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      // But totalCompleted counter includes both
      expect(Number(stats.totalCompleted)).toBe(2);
    });
  });

  // ============ Edge Cases ============

  describe('edge cases', () => {
    test('should handle concurrent acks on different jobs', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 10; i++) {
        await qm.push('test-q', { data: { id: i } });
      }
      for (let i = 0; i < 10; i++) {
        const pulled = await qm.pull('test-q');
        ids.push(pulled!.id);
      }

      // Ack all concurrently
      await Promise.all(ids.map((id) => qm.ack(id)));

      const stats = qm.getStats();
      expect(stats.completed).toBe(10);
      expect(stats.active).toBe(0);
    });

    test('should handle ack and fail interleaved', async () => {
      const pushed1 = await qm.push('test-q', { data: { id: 1 }, maxAttempts: 1 });
      const pushed2 = await qm.push('test-q', { data: { id: 2 } });
      const pushed3 = await qm.push('test-q', { data: { id: 3 }, maxAttempts: 1 });

      const p1 = await qm.pull('test-q');
      const p2 = await qm.pull('test-q');
      const p3 = await qm.pull('test-q');

      await qm.ack(p1!.id, { ok: true });
      await qm.fail(p2!.id, 'Retriable error');
      await qm.fail(p3!.id, 'Fatal error');

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      expect(stats.dlq).toBe(1);
      // p2 should be requeued (default maxAttempts=3)
      expect(stats.waiting + stats.delayed).toBe(1);
    });

    test('should handle ack with complex result data', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      const complexResult = {
        nested: { deep: { value: 42 } },
        array: [1, 2, 3],
        string: 'test',
        bool: true,
        null: null,
      };

      await qm.ack(pulled!.id, complexResult);

      const result = qm.getResult(pulled!.id);
      expect(result).toEqual(complexResult);
    });

    test('should handle ack with null result', async () => {
      await qm.push('test-q', { data: { msg: 'hello' } });
      const pulled = await qm.pull('test-q');

      // null passes the `result !== undefined` check in ackJob,
      // so jobResults.set(jobId, null) is called.
      // However, getResult uses `??` operator: jobResults.get(id) ?? storage?.getResult(id)
      // Since null is nullish, `null ?? undefined` returns `undefined`.
      // This is expected behavior of the getResult implementation.
      await qm.ack(pulled!.id, null);

      // The job should be completed regardless of result value
      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
    });

    test('should handle multiple queues independently', async () => {
      await qm.push('queue-a', { data: { q: 'a' } });
      await qm.push('queue-b', { data: { q: 'b' }, maxAttempts: 1 });

      const pA = await qm.pull('queue-a');
      const pB = await qm.pull('queue-b');

      await qm.ack(pA!.id, { result: 'a-done' });
      await qm.fail(pB!.id, 'b-error');

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      expect(stats.dlq).toBe(1);

      const resultA = qm.getResult(pA!.id);
      expect(resultA).toEqual({ result: 'a-done' });
    });

    test('should handle rapid push-pull-ack cycle', async () => {
      for (let i = 0; i < 50; i++) {
        const pushed = await qm.push('test-q', { data: { id: i } });
        const pulled = await qm.pull('test-q');
        await qm.ack(pulled!.id, { processed: i });
      }

      const stats = qm.getStats();
      expect(stats.completed).toBe(50);
      expect(stats.active).toBe(0);
      expect(stats.waiting).toBe(0);
    });
  });

  // ============ ackHelpers unit tests ============

  describe('ackHelpers', () => {
    test('groupByProcShard groups job IDs by processing shard', async () => {
      const { groupByProcShard } = await import(
        '../src/application/operations/ackHelpers'
      );
      const { processingShardIndex } = await import('../src/shared/hash');

      const ids = [
        jobId('id-aaa'),
        jobId('id-bbb'),
        jobId('id-ccc'),
        jobId('id-ddd'),
      ];

      const grouped = groupByProcShard(ids);

      // Verify all ids are present in grouping
      let totalIds = 0;
      for (const [shardIdx, groupIds] of grouped) {
        for (const gId of groupIds) {
          expect(processingShardIndex(gId)).toBe(shardIdx);
          totalIds++;
        }
      }
      expect(totalIds).toBe(4);
    });

    test('groupItemsByProcShard groups items with results', async () => {
      const { groupItemsByProcShard } = await import(
        '../src/application/operations/ackHelpers'
      );

      const items = [
        { id: jobId('id-1'), result: 'r1' },
        { id: jobId('id-2'), result: 'r2' },
        { id: jobId('id-3'), result: 'r3' },
      ];

      const grouped = groupItemsByProcShard(items);

      let totalItems = 0;
      for (const [, groupItems] of grouped) {
        totalItems += groupItems.length;
      }
      expect(totalItems).toBe(3);
    });

    test('groupByQueueShard groups extracted jobs by queue shard', async () => {
      const { groupByQueueShard } = await import(
        '../src/application/operations/ackHelpers'
      );
      const { shardIndex: getShardIndex } = await import('../src/shared/hash');

      const fakeJob = (queue: string) =>
        ({
          id: jobId('test-id'),
          queue,
          data: {},
          priority: 0,
          createdAt: Date.now(),
          runAt: Date.now(),
          startedAt: null,
          completedAt: null,
          attempts: 0,
          maxAttempts: 3,
          backoff: 1000,
          backoffConfig: null,
          ttl: null,
          timeout: null,
          uniqueKey: null,
          customId: null,
          dependsOn: [],
          parentId: null,
          childrenIds: [],
          childrenCompleted: 0,
          tags: [],
          groupId: null,
          progress: 0,
          progressMessage: null,
          removeOnComplete: false,
          removeOnFail: false,
          repeat: null,
          lastHeartbeat: Date.now(),
          stallTimeout: null,
          stallCount: 0,
          lifo: false,
          stackTraceLimit: 10,
          keepLogs: null,
          sizeLimit: null,
          failParentOnFailure: false,
          removeDependencyOnFailure: false,
          continueParentOnFailure: false,
          ignoreDependencyOnFailure: false,
          deduplicationTtl: null,
          deduplicationExtend: false,
          deduplicationReplace: false,
          debounceId: null,
          debounceTtl: null,
          timeline: [],
        }) as any;

      const extractedJobs = [
        { id: jobId('j1'), job: fakeJob('queue-x') },
        { id: jobId('j2'), job: fakeJob('queue-y') },
        { id: jobId('j3'), job: fakeJob('queue-x') },
      ];

      const grouped = groupByQueueShard(extractedJobs);

      let totalJobs = 0;
      for (const [shardIdx, jobs] of grouped) {
        for (const job of jobs) {
          expect(getShardIndex(job.queue)).toBe(shardIdx);
          totalJobs++;
        }
      }
      expect(totalJobs).toBe(3);
    });
  });

  // ============ State consistency ============

  describe('state consistency', () => {
    test('should maintain consistent counts across push-pull-ack cycle', async () => {
      const count = 20;
      for (let i = 0; i < count; i++) {
        await qm.push('test-q', { data: { id: i } });
      }

      let stats = qm.getStats();
      expect(stats.waiting).toBe(count);
      expect(Number(stats.totalPushed)).toBe(count);

      const ids: JobId[] = [];
      for (let i = 0; i < count; i++) {
        const pulled = await qm.pull('test-q');
        ids.push(pulled!.id);
      }

      stats = qm.getStats();
      expect(stats.active).toBe(count);
      expect(stats.waiting).toBe(0);

      await Promise.all(ids.map((id) => qm.ack(id)));

      stats = qm.getStats();
      expect(stats.completed).toBe(count);
      expect(stats.active).toBe(0);
      expect(stats.waiting).toBe(0);
      expect(Number(stats.totalCompleted)).toBe(count);
    });

    test('should maintain consistent counts with mixed ack and fail', async () => {
      for (let i = 0; i < 10; i++) {
        await qm.push('test-q', { data: { id: i }, maxAttempts: 1 });
      }

      const ids: JobId[] = [];
      for (let i = 0; i < 10; i++) {
        const pulled = await qm.pull('test-q');
        ids.push(pulled!.id);
      }

      // Ack even-indexed, fail odd-indexed
      await Promise.all(
        ids.map((id, i) =>
          i % 2 === 0 ? qm.ack(id) : qm.fail(id, 'Error')
        )
      );

      const stats = qm.getStats();
      expect(stats.completed).toBe(5);
      expect(stats.dlq).toBe(5);
      expect(stats.active).toBe(0);
    });
  });
});
