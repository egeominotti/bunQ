/**
 * Batch Operations Tests
 * Tests pullBatch and ackBatch optimizations
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Batch Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('pullBatch', () => {
    test('should pull multiple jobs in single call', async () => {
      // Push 10 jobs
      for (let i = 0; i < 10; i++) {
        await qm.push('test-queue', { data: { index: i } });
      }

      // Pull batch of 5
      const jobs = await qm.pullBatch('test-queue', 5);
      expect(jobs.length).toBe(5);

      // Each job should have unique data
      const indices = jobs.map((j) => (j.data as { index: number }).index);
      expect(new Set(indices).size).toBe(5);
    });

    test('should return fewer jobs when queue has less than requested', async () => {
      // Push 3 jobs
      for (let i = 0; i < 3; i++) {
        await qm.push('test-queue', { data: { index: i } });
      }

      // Request 10 but only 3 available
      const jobs = await qm.pullBatch('test-queue', 10);
      expect(jobs.length).toBe(3);
    });

    test('should return empty array when queue is empty', async () => {
      const jobs = await qm.pullBatch('test-queue', 5);
      expect(jobs.length).toBe(0);
    });

    test('should respect priority order', async () => {
      // Push jobs with different priorities
      await qm.push('test-queue', { data: { id: 'low' }, priority: 1 });
      await qm.push('test-queue', { data: { id: 'high' }, priority: 10 });
      await qm.push('test-queue', { data: { id: 'medium' }, priority: 5 });

      const jobs = await qm.pullBatch('test-queue', 3);
      expect(jobs.length).toBe(3);
      expect((jobs[0].data as { id: string }).id).toBe('high');
      expect((jobs[1].data as { id: string }).id).toBe('medium');
      expect((jobs[2].data as { id: string }).id).toBe('low');
    });

    test('should move jobs to processing state', async () => {
      await qm.push('test-queue', { data: {} });
      await qm.push('test-queue', { data: {} });

      const jobs = await qm.pullBatch('test-queue', 2);
      expect(jobs.length).toBe(2);

      // Jobs should be in processing - stats should show active
      const stats = qm.getStats();
      expect(stats.active).toBe(2);
      expect(stats.waiting).toBe(0);
    });
  });

  describe('ackBatch', () => {
    test('should acknowledge multiple jobs', async () => {
      // Push and pull jobs
      for (let i = 0; i < 5; i++) {
        await qm.push('test-queue', { data: { index: i }, removeOnComplete: true });
      }
      const jobs = await qm.pullBatch('test-queue', 5);
      expect(jobs.length).toBe(5);

      // Ack all jobs
      await qm.ackBatch(jobs.map((j) => j.id));

      // Stats should show no active jobs
      const stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('should handle empty batch', async () => {
      // Should not throw
      await qm.ackBatch([]);
    });

    test('should handle small batch (fast path)', async () => {
      // Small batches (<= 4) use individual acks
      for (let i = 0; i < 3; i++) {
        await qm.push('test-queue', { data: { index: i }, removeOnComplete: true });
      }
      const jobs = await qm.pullBatch('test-queue', 3);
      await qm.ackBatch(jobs.map((j) => j.id));

      const stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('should handle large batch with shard grouping', async () => {
      // Large batches (> 4) use shard grouping optimization
      for (let i = 0; i < 20; i++) {
        await qm.push('test-queue', { data: { index: i }, removeOnComplete: true });
      }
      const jobs = await qm.pullBatch('test-queue', 20);
      expect(jobs.length).toBe(20);

      await qm.ackBatch(jobs.map((j) => j.id));

      const stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('should store completed jobs when removeOnComplete is false', async () => {
      for (let i = 0; i < 5; i++) {
        await qm.push('test-queue', { data: { index: i }, removeOnComplete: false });
      }
      const jobs = await qm.pullBatch('test-queue', 5);
      await qm.ackBatch(jobs.map((j) => j.id));

      const stats = qm.getStats();
      expect(stats.completed).toBe(5);
    });

    test('should handle jobs from multiple queues', async () => {
      // Push to different queues
      await qm.push('queue-1', { data: { q: 1 }, removeOnComplete: true });
      await qm.push('queue-2', { data: { q: 2 }, removeOnComplete: true });
      await qm.push('queue-3', { data: { q: 3 }, removeOnComplete: true });

      // Pull from each queue
      const job1 = await qm.pull('queue-1');
      const job2 = await qm.pull('queue-2');
      const job3 = await qm.pull('queue-3');

      // Ack all in one batch
      await qm.ackBatch([job1!.id, job2!.id, job3!.id]);

      const stats = qm.getStats();
      expect(stats.active).toBe(0);
    });

    test('should broadcast completion events', async () => {
      const events: string[] = [];
      qm.subscribe((event) => {
        events.push(event.eventType);
      });

      for (let i = 0; i < 5; i++) {
        await qm.push('test-queue', { data: { index: i }, removeOnComplete: true });
      }
      const jobs = await qm.pullBatch('test-queue', 5);
      await qm.ackBatch(jobs.map((j) => j.id));

      // Should have 5 push, 5 pull, 5 completed events
      const completedEvents = events.filter((e) => e === 'completed');
      expect(completedEvents.length).toBe(5);
    });
  });

  describe('combined batch operations', () => {
    test('should handle rapid push-pull-ack cycles', async () => {
      const iterations = 10;
      const batchSize = 100;

      for (let i = 0; i < iterations; i++) {
        // Push batch
        const inputs = Array.from({ length: batchSize }, (_, j) => ({
          data: { iteration: i, index: j },
          removeOnComplete: true,
        }));
        await qm.pushBatch('test-queue', inputs);

        // Pull batch
        const jobs = await qm.pullBatch('test-queue', batchSize);
        expect(jobs.length).toBe(batchSize);

        // Ack batch
        await qm.ackBatch(jobs.map((j) => j.id));
      }

      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
    });
  });
});
