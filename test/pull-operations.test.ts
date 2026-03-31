/**
 * Pull Operations Tests
 * Comprehensive tests for pull.ts: pullJob, pullJobBatch
 * Tests use embedded mode via QueueManager directly (no TCP).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { JobId } from '../src/domain/types/job';

describe('Pull Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============ Single Job Pull ============

  describe('single pull', () => {
    test('should pull a job from the queue', async () => {
      await qm.push('tasks', { data: { action: 'process' } });

      const job = await qm.pull('tasks');

      expect(job).not.toBeNull();
      expect(job!.queue).toBe('tasks');
      expect(job!.data).toEqual({ action: 'process' });
    });

    test('should return null when queue is empty', async () => {
      const job = await qm.pull('empty-queue', 0);

      expect(job).toBeNull();
    });

    test('should return null for non-existent queue', async () => {
      const job = await qm.pull('does-not-exist', 0);

      expect(job).toBeNull();
    });

    test('should set startedAt timestamp on pulled job', async () => {
      const before = Date.now();
      await qm.push('tasks', { data: { msg: 'test' } });

      const job = await qm.pull('tasks');

      expect(job).not.toBeNull();
      expect(job!.startedAt).not.toBeNull();
      expect(job!.startedAt!).toBeGreaterThanOrEqual(before);
      expect(job!.startedAt!).toBeLessThanOrEqual(Date.now());
    });

    test('should transition job state from waiting to active', async () => {
      const pushed = await qm.push('tasks', { data: { msg: 'test' } });

      // Before pull, job should be waiting
      const stateBefore = await qm.getJobState(pushed.id);
      expect(stateBefore).toBe('waiting');

      const pulled = await qm.pull('tasks');
      expect(pulled).not.toBeNull();

      // After pull, job should be active
      const stateAfter = await qm.getJobState(pulled!.id);
      expect(stateAfter).toBe('active');
    });

    test('should preserve job data through pull', async () => {
      const complexData = {
        user: { name: 'Alice', id: 42 },
        items: [1, 2, 3],
        nested: { deep: { value: true } },
      };
      await qm.push('tasks', { data: complexData });

      const job = await qm.pull('tasks');

      expect(job!.data).toEqual(complexData);
    });

    test('should remove job from waiting queue after pull', async () => {
      await qm.push('tasks', { data: { msg: 'only-one' } });

      const first = await qm.pull('tasks');
      expect(first).not.toBeNull();

      // Second pull should return null (queue empty)
      const second = await qm.pull('tasks', 0);
      expect(second).toBeNull();
    });
  });

  // ============ Priority Ordering ============

  describe('priority ordering', () => {
    test('should pull higher priority jobs first', async () => {
      await qm.push('tasks', { data: { id: 'low' }, priority: 1 });
      await qm.push('tasks', { data: { id: 'high' }, priority: 10 });
      await qm.push('tasks', { data: { id: 'medium' }, priority: 5 });

      const first = await qm.pull('tasks');
      const second = await qm.pull('tasks');
      const third = await qm.pull('tasks');

      expect((first!.data as { id: string }).id).toBe('high');
      expect((second!.data as { id: string }).id).toBe('medium');
      expect((third!.data as { id: string }).id).toBe('low');
    });

    test('should pull same-priority jobs in FIFO order', async () => {
      await qm.push('tasks', { data: { id: 'first' }, priority: 5 });
      await qm.push('tasks', { data: { id: 'second' }, priority: 5 });
      await qm.push('tasks', { data: { id: 'third' }, priority: 5 });

      const a = await qm.pull('tasks');
      const b = await qm.pull('tasks');
      const c = await qm.pull('tasks');

      expect((a!.data as { id: string }).id).toBe('first');
      expect((b!.data as { id: string }).id).toBe('second');
      expect((c!.data as { id: string }).id).toBe('third');
    });

    test('should pull default priority (0) jobs after higher ones', async () => {
      await qm.push('tasks', { data: { id: 'default' } }); // priority 0
      await qm.push('tasks', { data: { id: 'elevated' }, priority: 1 });

      const first = await qm.pull('tasks');
      const second = await qm.pull('tasks');

      expect((first!.data as { id: string }).id).toBe('elevated');
      expect((second!.data as { id: string }).id).toBe('default');
    });
  });

  // ============ LIFO Ordering ============

  describe('LIFO ordering', () => {
    test('should pull LIFO jobs in reverse order within same priority', async () => {
      await qm.push('tasks', { data: { id: 'first' }, lifo: true });
      await qm.push('tasks', { data: { id: 'second' }, lifo: true });
      await qm.push('tasks', { data: { id: 'third' }, lifo: true });

      const a = await qm.pull('tasks');
      const b = await qm.pull('tasks');
      const c = await qm.pull('tasks');

      // LIFO: last pushed should be pulled first
      expect((a!.data as { id: string }).id).toBe('third');
      expect((b!.data as { id: string }).id).toBe('second');
      expect((c!.data as { id: string }).id).toBe('first');
    });

    test('should still respect priority with LIFO', async () => {
      await qm.push('tasks', { data: { id: 'low-lifo' }, priority: 1, lifo: true });
      await qm.push('tasks', { data: { id: 'high-lifo' }, priority: 10, lifo: true });

      const first = await qm.pull('tasks');
      const second = await qm.pull('tasks');

      // Higher priority first, regardless of LIFO
      expect((first!.data as { id: string }).id).toBe('high-lifo');
      expect((second!.data as { id: string }).id).toBe('low-lifo');
    });
  });

  // ============ Batch Pull ============

  describe('pullBatch', () => {
    test('should pull multiple jobs at once', async () => {
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });
      await qm.push('tasks', { data: { id: 3 } });

      const jobs = await qm.pullBatch('tasks', 3);

      expect(jobs.length).toBe(3);
    });

    test('should pull batch respecting priority order', async () => {
      await qm.push('tasks', { data: { id: 'low' }, priority: 1 });
      await qm.push('tasks', { data: { id: 'high' }, priority: 10 });
      await qm.push('tasks', { data: { id: 'medium' }, priority: 5 });

      const jobs = await qm.pullBatch('tasks', 3);

      expect(jobs.length).toBe(3);
      expect((jobs[0].data as { id: string }).id).toBe('high');
      expect((jobs[1].data as { id: string }).id).toBe('medium');
      expect((jobs[2].data as { id: string }).id).toBe('low');
    });

    test('should return fewer jobs if batch size exceeds available', async () => {
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });

      const jobs = await qm.pullBatch('tasks', 10);

      expect(jobs.length).toBe(2);
    });

    test('should return empty array when queue is empty', async () => {
      const jobs = await qm.pullBatch('empty-queue', 5, 0);

      expect(jobs).toEqual([]);
    });

    test('should return empty array for non-existent queue', async () => {
      const jobs = await qm.pullBatch('nope', 5, 0);

      expect(jobs).toEqual([]);
    });

    test('should pull batch of size 1', async () => {
      await qm.push('tasks', { data: { id: 'only' } });

      const jobs = await qm.pullBatch('tasks', 1);

      expect(jobs.length).toBe(1);
      expect((jobs[0].data as { id: string }).id).toBe('only');
    });

    test('should set startedAt on all batch-pulled jobs', async () => {
      const before = Date.now();
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });
      await qm.push('tasks', { data: { id: 3 } });

      const jobs = await qm.pullBatch('tasks', 3);

      for (const job of jobs) {
        expect(job.startedAt).not.toBeNull();
        expect(job.startedAt!).toBeGreaterThanOrEqual(before);
      }
    });

    test('should transition all batch-pulled jobs to active state', async () => {
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });

      const jobs = await qm.pullBatch('tasks', 2);

      for (const job of jobs) {
        const state = await qm.getJobState(job.id);
        expect(state).toBe('active');
      }
    });
  });

  // ============ Delayed Jobs ============

  describe('delayed jobs', () => {
    test('should not pull delayed jobs before runAt time', async () => {
      await qm.push('tasks', {
        data: { msg: 'delayed' },
        delay: 60000, // 60 seconds in the future
      });

      const job = await qm.pull('tasks', 0);

      expect(job).toBeNull();
    });

    test('should not pull delayed jobs in a batch', async () => {
      await qm.push('tasks', {
        data: { msg: 'delayed' },
        delay: 60000,
      });

      const jobs = await qm.pullBatch('tasks', 5, 0);

      expect(jobs).toEqual([]);
    });

    test('should pull ready jobs and skip delayed ones', async () => {
      await qm.push('tasks', { data: { id: 'ready' } });
      await qm.push('tasks', {
        data: { id: 'delayed' },
        delay: 60000,
      });

      const job = await qm.pull('tasks');

      expect(job).not.toBeNull();
      expect((job!.data as { id: string }).id).toBe('ready');

      // Second pull should return null (only delayed left)
      const job2 = await qm.pull('tasks', 0);
      expect(job2).toBeNull();
    });
  });

  // ============ Paused Queue ============

  describe('paused queue', () => {
    test('should return null when pulling from paused queue', async () => {
      await qm.push('tasks', { data: { msg: 'test' } });
      qm.pause('tasks');

      const job = await qm.pull('tasks', 0);

      expect(job).toBeNull();
    });

    test('should return empty array when batch pulling from paused queue', async () => {
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });
      qm.pause('tasks');

      const jobs = await qm.pullBatch('tasks', 5, 0);

      expect(jobs).toEqual([]);
    });

    test('should allow pulling after resume', async () => {
      await qm.push('tasks', { data: { msg: 'test' } });
      qm.pause('tasks');

      const jobBefore = await qm.pull('tasks', 0);
      expect(jobBefore).toBeNull();

      qm.resume('tasks');

      const jobAfter = await qm.pull('tasks');
      expect(jobAfter).not.toBeNull();
      expect(jobAfter!.data).toEqual({ msg: 'test' });
    });
  });

  // ============ Queue Isolation ============

  describe('queue isolation', () => {
    test('should pull from specific queue without affecting others', async () => {
      await qm.push('queue-a', { data: { from: 'a' } });
      await qm.push('queue-b', { data: { from: 'b' } });

      const jobA = await qm.pull('queue-a');

      expect(jobA).not.toBeNull();
      expect((jobA!.data as { from: string }).from).toBe('a');

      // queue-b should still have its job
      const jobB = await qm.pull('queue-b');
      expect(jobB).not.toBeNull();
      expect((jobB!.data as { from: string }).from).toBe('b');
    });

    test('should not cross-contaminate between queues', async () => {
      await qm.push('emails', { data: { type: 'email' } });

      // Pull from a different queue
      const job = await qm.pull('notifications', 0);

      expect(job).toBeNull();

      // Original queue should still have the job
      const emailJob = await qm.pull('emails');
      expect(emailJob).not.toBeNull();
      expect((emailJob!.data as { type: string }).type).toBe('email');
    });
  });

  // ============ Concurrent Pulls ============

  describe('concurrent pulls', () => {
    test('should not return the same job to concurrent pulls', async () => {
      // Push several jobs
      const count = 10;
      for (let i = 0; i < count; i++) {
        await qm.push('tasks', { data: { id: i } });
      }

      // Pull all concurrently
      const promises = Array.from({ length: count }, () => qm.pull('tasks', 0));
      const results = await Promise.all(promises);

      // Filter out nulls
      const jobs = results.filter((j) => j !== null);

      // All returned jobs should have unique IDs
      const ids = new Set(jobs.map((j) => j!.id));
      expect(ids.size).toBe(jobs.length);
    });

    test('should not duplicate jobs across concurrent batch pulls', async () => {
      const count = 20;
      for (let i = 0; i < count; i++) {
        await qm.push('tasks', { data: { id: i } });
      }

      // Pull in concurrent batches
      const promises = [
        qm.pullBatch('tasks', 10, 0),
        qm.pullBatch('tasks', 10, 0),
      ];
      const [batch1, batch2] = await Promise.all(promises);

      // All job IDs across batches should be unique
      const allIds = [...batch1.map((j) => j.id), ...batch2.map((j) => j.id)];
      const uniqueIds = new Set(allIds);
      expect(uniqueIds.size).toBe(allIds.length);

      // Total should not exceed what was pushed
      expect(allIds.length).toBeLessThanOrEqual(count);
    });
  });

  // ============ Pull After Push Ordering ============

  describe('pull after push ordering', () => {
    test('should pull jobs in push order with same priority (FIFO)', async () => {
      const pushOrder: number[] = [];
      for (let i = 0; i < 5; i++) {
        await qm.push('tasks', { data: { seq: i } });
        pushOrder.push(i);
      }

      const pullOrder: number[] = [];
      for (let i = 0; i < 5; i++) {
        const job = await qm.pull('tasks');
        expect(job).not.toBeNull();
        pullOrder.push((job!.data as { seq: number }).seq);
      }

      expect(pullOrder).toEqual(pushOrder);
    });

    test('should pull newly pushed jobs after existing ones at same priority', async () => {
      await qm.push('tasks', { data: { id: 'existing' } });
      await qm.push('tasks', { data: { id: 'new' } });

      const first = await qm.pull('tasks');
      const second = await qm.pull('tasks');

      expect((first!.data as { id: string }).id).toBe('existing');
      expect((second!.data as { id: string }).id).toBe('new');
    });
  });

  // ============ Pull with Lock ============

  describe('pullWithLock', () => {
    test('should return job and lock token', async () => {
      await qm.push('tasks', { data: { msg: 'locked' } });

      const result = await qm.pullWithLock('tasks', 'worker-1');

      expect(result.job).not.toBeNull();
      expect(result.token).not.toBeNull();
      expect(typeof result.token).toBe('string');
      expect(result.job!.data).toEqual({ msg: 'locked' });
    });

    test('should return null job and null token when queue is empty', async () => {
      const result = await qm.pullWithLock('empty-queue', 'worker-1', 0);

      expect(result.job).toBeNull();
      expect(result.token).toBeNull();
    });

    test('should allow ack with valid lock token', async () => {
      await qm.push('tasks', { data: { msg: 'locked' } });

      const { job, token } = await qm.pullWithLock('tasks', 'worker-1');
      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      // Ack with the lock token should succeed
      await qm.ack(job!.id, { done: true }, token!);

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
    });

    test('should reject ack with invalid lock token', async () => {
      await qm.push('tasks', { data: { msg: 'locked' } });

      const { job } = await qm.pullWithLock('tasks', 'worker-1');
      expect(job).not.toBeNull();

      // Ack with wrong token should throw
      await expect(
        qm.ack(job!.id, { done: true }, 'wrong-token')
      ).rejects.toThrow('Invalid or expired lock token');
    });
  });

  // ============ Pull Batch with Lock ============

  describe('pullBatchWithLock', () => {
    test('should return jobs and matching tokens', async () => {
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });
      await qm.push('tasks', { data: { id: 3 } });

      const result = await qm.pullBatchWithLock('tasks', 3, 'worker-1');

      expect(result.jobs.length).toBe(3);
      expect(result.tokens.length).toBe(3);

      for (const token of result.tokens) {
        expect(typeof token).toBe('string');
        expect(token.length).toBeGreaterThan(0);
      }
    });

    test('should return empty arrays when queue is empty', async () => {
      const result = await qm.pullBatchWithLock('empty', 5, 'worker-1', 0);

      expect(result.jobs).toEqual([]);
      expect(result.tokens).toEqual([]);
    });
  });

  // ============ Stats Integration ============

  describe('stats integration', () => {
    test('should update stats on pull', async () => {
      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });
      await qm.push('tasks', { data: { id: 3 } });

      await qm.pull('tasks');

      const stats = qm.getStats();
      expect(stats.active).toBe(1);
      expect(Number(stats.totalPulled)).toBe(1);
    });

    test('should update stats on batch pull', async () => {
      for (let i = 0; i < 5; i++) {
        await qm.push('tasks', { data: { id: i } });
      }

      const jobs = await qm.pullBatch('tasks', 3);

      const stats = qm.getStats();
      expect(stats.active).toBe(3);
      expect(Number(stats.totalPulled)).toBe(3);
      expect(jobs.length).toBe(3);
    });
  });

  // ============ Events Integration ============

  describe('events integration', () => {
    test('should emit pulled event on pull', async () => {
      const events: string[] = [];
      qm.subscribe((event) => {
        events.push(event.eventType);
      });

      await qm.push('tasks', { data: { msg: 'test' } });
      await qm.pull('tasks');

      expect(events).toContain('pulled');
    });

    test('should emit pulled events for batch pull', async () => {
      const pulledEvents: string[] = [];
      qm.subscribe((event) => {
        if (event.eventType === 'pulled') {
          pulledEvents.push(event.jobId);
        }
      });

      await qm.push('tasks', { data: { id: 1 } });
      await qm.push('tasks', { data: { id: 2 } });

      const jobs = await qm.pullBatch('tasks', 2);

      expect(pulledEvents.length).toBe(2);
      // Events should correspond to pulled jobs
      for (const job of jobs) {
        expect(pulledEvents).toContain(job.id);
      }
    });
  });

  // ============ Edge Cases ============

  describe('edge cases', () => {
    test('should handle pull with zero timeout', async () => {
      const job = await qm.pull('tasks', 0);
      expect(job).toBeNull();
    });

    test('should handle rapid push-pull cycles', async () => {
      for (let i = 0; i < 50; i++) {
        await qm.push('tasks', { data: { cycle: i } });
        const job = await qm.pull('tasks');
        expect(job).not.toBeNull();
        expect((job!.data as { cycle: number }).cycle).toBe(i);
        await qm.ack(job!.id);
      }
    });

    test('should handle large batch pull from small queue', async () => {
      await qm.push('tasks', { data: { id: 1 } });

      const jobs = await qm.pullBatch('tasks', 1000, 0);

      expect(jobs.length).toBe(1);
    });

    test('should handle pull with various data types', async () => {
      await qm.push('tasks', { data: null });
      const nullJob = await qm.pull('tasks');
      expect(nullJob!.data).toBeNull();

      await qm.push('tasks', { data: 42 });
      const numJob = await qm.pull('tasks');
      expect(numJob!.data).toBe(42);

      await qm.push('tasks', { data: 'hello' });
      const strJob = await qm.pull('tasks');
      expect(strJob!.data).toBe('hello');

      await qm.push('tasks', { data: [1, 2, 3] });
      const arrJob = await qm.pull('tasks');
      expect(arrJob!.data).toEqual([1, 2, 3]);
    });

    test('should handle pull after draining and re-pushing', async () => {
      await qm.push('tasks', { data: { id: 'before-drain' } });
      qm.drain('tasks');

      const nothingLeft = await qm.pull('tasks', 0);
      expect(nothingLeft).toBeNull();

      await qm.push('tasks', { data: { id: 'after-drain' } });
      const job = await qm.pull('tasks');
      expect(job).not.toBeNull();
      expect((job!.data as { id: string }).id).toBe('after-drain');
    });
  });
});
