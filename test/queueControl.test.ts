/**
 * Queue Control Tests
 * Pause, resume, drain, obliterate, clean, list queues
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Queue Control', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('pause', () => {
    test('should pause queue', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });

      qm.pause('test-queue');
      expect(qm.isPaused('test-queue')).toBe(true);

      // Should not be able to pull from paused queue
      const job = await qm.pull('test-queue', 0);
      expect(job).toBeNull();
    });

    test('should not affect other queues', async () => {
      await qm.push('queue1', { data: { msg: 'q1' } });
      await qm.push('queue2', { data: { msg: 'q2' } });

      qm.pause('queue1');

      expect(qm.isPaused('queue1')).toBe(true);
      expect(qm.isPaused('queue2')).toBe(false);

      // queue2 should still work
      const job = await qm.pull('queue2');
      expect(job).not.toBeNull();
    });
  });

  describe('resume', () => {
    test('should resume paused queue', async () => {
      await qm.push('test-queue', { data: { msg: 'test' } });

      qm.pause('test-queue');
      expect(qm.isPaused('test-queue')).toBe(true);

      qm.resume('test-queue');
      expect(qm.isPaused('test-queue')).toBe(false);

      // Should be able to pull now
      const job = await qm.pull('test-queue');
      expect(job).not.toBeNull();
    });

    test('should be idempotent', () => {
      qm.resume('test-queue');
      qm.resume('test-queue');
      expect(qm.isPaused('test-queue')).toBe(false);
    });
  });

  describe('isPaused', () => {
    test('should return false for non-existent queue', () => {
      expect(qm.isPaused('non-existent')).toBe(false);
    });

    test('should return correct state', () => {
      qm.pause('test-queue');
      expect(qm.isPaused('test-queue')).toBe(true);

      qm.resume('test-queue');
      expect(qm.isPaused('test-queue')).toBe(false);
    });
  });

  describe('drain', () => {
    test('should remove all waiting jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      const drained = qm.drain('test-queue');
      expect(drained).toBe(3);

      const job = await qm.pull('test-queue', 0);
      expect(job).toBeNull();
    });

    test('should not affect active jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      // Pull one job (make it active)
      await qm.pull('test-queue');

      const drained = qm.drain('test-queue');
      expect(drained).toBe(1); // Only waiting job drained

      const stats = qm.getStats();
      expect(stats.active).toBe(1);
    });

    test('should return 0 for empty queue', () => {
      const drained = qm.drain('empty-queue');
      expect(drained).toBe(0);
    });
  });

  describe('obliterate', () => {
    test('should remove all queue data', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.pull('test-queue'); // Make one active

      qm.obliterate('test-queue');

      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);
    });

    test('should clear paused state', async () => {
      qm.pause('test-queue');
      expect(qm.isPaused('test-queue')).toBe(true);

      qm.obliterate('test-queue');
      expect(qm.isPaused('test-queue')).toBe(false);
    });
  });

  describe('listQueues', () => {
    test('should return empty array initially', () => {
      const queues = qm.listQueues();
      expect(queues).toEqual([]);
    });

    test('should return all queue names', async () => {
      await qm.push('emails', { data: {} });
      await qm.push('notifications', { data: {} });
      await qm.push('tasks', { data: {} });

      const queues = qm.listQueues();
      expect(queues.length).toBe(3);
      expect(queues).toContain('emails');
      expect(queues).toContain('notifications');
      expect(queues).toContain('tasks');
    });

    test('should not duplicate queue names', async () => {
      await qm.push('emails', { data: { id: 1 } });
      await qm.push('emails', { data: { id: 2 } });
      await qm.push('emails', { data: { id: 3 } });

      const queues = qm.listQueues();
      expect(queues.filter((q) => q === 'emails').length).toBe(1);
    });
  });

  describe('clean', () => {
    test('should respect grace period', async () => {
      await qm.push('test-queue', { data: { id: 1 } });

      // Clean with 1 hour grace period (should not remove)
      const cleaned = qm.clean('test-queue', 3600000);
      expect(cleaned).toBe(0);

      const job = await qm.pull('test-queue');
      expect(job).not.toBeNull();
    });
  });

  describe('count', () => {
    test('should return 0 for empty queue', () => {
      const count = qm.count('empty-queue');
      expect(count).toBe(0);
    });

    test('should return correct count', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      const count = qm.count('test-queue');
      expect(count).toBe(3);
    });

    test('should decrease when jobs are pulled', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      expect(qm.count('test-queue')).toBe(2);

      await qm.pull('test-queue');
      expect(qm.count('test-queue')).toBe(1);
    });
  });
});
