/**
 * Queue Manager Tests
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('QueueManager', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('push', () => {
    test('should push a job', async () => {
      const job = await qm.push('emails', { data: { to: 'test@test.com' } });

      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
      expect(job.queue).toBe('emails');
      expect(job.data).toEqual({ to: 'test@test.com' });
    });

    test('should push with priority', async () => {
      const job = await qm.push('emails', {
        data: { msg: 'test' },
        priority: 10,
      });

      expect(job.priority).toBe(10);
    });

    test('should push with delay', async () => {
      const before = Date.now();
      const job = await qm.push('emails', {
        data: { msg: 'test' },
        delay: 5000,
      });

      expect(job.runAt).toBeGreaterThanOrEqual(before + 5000);
    });

    test('should push batch', async () => {
      const ids = await qm.pushBatch('emails', [
        { data: { id: 1 } },
        { data: { id: 2 } },
        { data: { id: 3 } },
      ]);

      expect(ids.length).toBe(3);
    });

    test('should return existing job for duplicate unique key', async () => {
      const job1 = await qm.push('emails', {
        data: { msg: 'first' },
        uniqueKey: 'unique1',
      });

      const job2 = await qm.push('emails', {
        data: { msg: 'second' },
        uniqueKey: 'unique1',
      });

      // BullMQ-style: returns existing job instead of throwing
      expect(job2.id).toBe(job1.id);
    });
  });

  describe('pull', () => {
    test('should pull a job', async () => {
      await qm.push('emails', { data: { msg: 'test' } });

      const job = await qm.pull('emails');

      expect(job).not.toBeNull();
      expect(job?.data).toEqual({ msg: 'test' });
      expect(job?.startedAt).not.toBeNull();
    });

    test('should return null for empty queue', async () => {
      const job = await qm.pull('emails', 0);
      expect(job).toBeNull();
    });

    test('should respect priority', async () => {
      await qm.push('emails', { data: { id: 'low' }, priority: 1 });
      await qm.push('emails', { data: { id: 'high' }, priority: 10 });

      const first = await qm.pull('emails');
      const second = await qm.pull('emails');

      expect((first?.data as { id: string }).id).toBe('high');
      expect((second?.data as { id: string }).id).toBe('low');
    });

    test('should not pull delayed jobs', async () => {
      await qm.push('emails', {
        data: { msg: 'delayed' },
        delay: 10000,
      });

      const job = await qm.pull('emails', 0);
      expect(job).toBeNull();
    });
  });

  describe('ack', () => {
    test('should acknowledge job', async () => {
      await qm.push('emails', { data: { msg: 'test' } });
      const pulled = await qm.pull('emails');

      await qm.ack(pulled!.id, { processed: true });

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      expect(stats.active).toBe(0);
    });

    test('should store result', async () => {
      await qm.push('emails', { data: { msg: 'test' } });
      const pulled = await qm.pull('emails');

      await qm.ack(pulled!.id, { result: 'success' });

      const result = qm.getResult(pulled!.id);
      expect(result).toEqual({ result: 'success' });
    });
  });

  describe('fail', () => {
    test('should retry job on fail', async () => {
      await qm.push('emails', {
        data: { msg: 'test' },
        maxAttempts: 3,
      });
      const pulled = await qm.pull('emails');

      await qm.fail(pulled!.id, 'Error occurred');

      const stats = qm.getStats();
      expect(stats.waiting + stats.delayed).toBe(1); // Job requeued
      expect(stats.dlq).toBe(0);
    });

    test('should move to DLQ after max attempts', async () => {
      await qm.push('emails', {
        data: { msg: 'test' },
        maxAttempts: 1,
      });

      const pulled = await qm.pull('emails');
      await qm.fail(pulled!.id, 'Error');

      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);
    });
  });

  describe('getJob', () => {
    test('should get job by id', async () => {
      const pushed = await qm.push('emails', { data: { msg: 'test' } });

      const job = await qm.getJob(pushed.id);

      expect(job).not.toBeNull();
      expect(job?.id).toBe(pushed.id);
    });

    test('should return null for unknown id', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const job = await qm.getJob(jobId('non-existent'));
      expect(job).toBeNull();
    });
  });

  describe('stats', () => {
    test('should return correct stats', async () => {
      await qm.push('emails', { data: { id: 1 } });
      await qm.push('emails', { data: { id: 2 } });
      await qm.push('emails', { data: { id: 3 }, delay: 10000 });

      await qm.pull('emails');

      const stats = qm.getStats();

      expect(stats.waiting).toBe(1);
      expect(stats.delayed).toBe(1);
      expect(stats.active).toBe(1);
      expect(Number(stats.totalPushed)).toBe(3);
      expect(Number(stats.totalPulled)).toBe(1);
    });
  });

  describe('events', () => {
    test('should emit events on push', async () => {
      const events: string[] = [];

      qm.subscribe((event) => {
        events.push(event.eventType);
      });

      await qm.push('emails', { data: { msg: 'test' } });

      expect(events).toContain('pushed');
    });

    test('should unsubscribe', async () => {
      const events: string[] = [];

      const unsubscribe = qm.subscribe((event) => {
        events.push(event.eventType);
      });

      await qm.push('emails', { data: { id: 1 } });
      unsubscribe();
      await qm.push('emails', { data: { id: 2 } });

      expect(events.length).toBe(1);
    });
  });
});
