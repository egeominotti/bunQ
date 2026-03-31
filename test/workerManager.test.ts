/**
 * Worker Manager Tests
 * Worker registration and tracking
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { WorkerManager } from '../src/application/workerManager';

describe('WorkerManager', () => {
  let manager: WorkerManager;

  beforeEach(() => {
    manager = new WorkerManager();
  });

  afterEach(() => {
    manager.stop();
  });

  describe('register', () => {
    test('should register a worker', () => {
      const worker = manager.register('worker-1', ['emails', 'notifications']);

      expect(worker.id).toBeDefined();
      expect(worker.name).toBe('worker-1');
      expect(worker.queues).toEqual(['emails', 'notifications']);
      expect(worker.activeJobs).toBe(0);
      expect(worker.processedJobs).toBe(0);
      expect(worker.failedJobs).toBe(0);
    });

    test('should generate unique IDs', () => {
      const w1 = manager.register('worker-1', ['q1']);
      const w2 = manager.register('worker-2', ['q2']);

      expect(w1.id).not.toBe(w2.id);
    });
  });

  describe('unregister', () => {
    test('should unregister a worker', () => {
      const worker = manager.register('worker-1', ['emails']);

      const removed = manager.unregister(worker.id);
      expect(removed).toBe(true);

      expect(manager.get(worker.id)).toBeUndefined();
    });

    test('should return false for non-existent worker', () => {
      const removed = manager.unregister('non-existent-id');
      expect(removed).toBe(false);
    });
  });

  describe('get', () => {
    test('should get worker by ID', () => {
      const worker = manager.register('worker-1', ['emails']);

      const found = manager.get(worker.id);
      expect(found?.name).toBe('worker-1');
    });

    test('should return undefined for non-existent ID', () => {
      expect(manager.get('non-existent')).toBeUndefined();
    });
  });

  describe('heartbeat', () => {
    test('should update lastSeen', async () => {
      const worker = manager.register('worker-1', ['emails']);
      const initialLastSeen = worker.lastSeen;

      await Bun.sleep(10);

      const updated = manager.heartbeat(worker.id);
      expect(updated).toBe(true);
      expect(worker.lastSeen).toBeGreaterThan(initialLastSeen);
    });

    test('should return false for non-existent worker', () => {
      const updated = manager.heartbeat('non-existent');
      expect(updated).toBe(false);
    });
  });

  describe('incrementActive', () => {
    test('should increment active jobs count', () => {
      const worker = manager.register('worker-1', ['emails']);

      manager.incrementActive(worker.id);
      expect(worker.activeJobs).toBe(1);

      manager.incrementActive(worker.id);
      expect(worker.activeJobs).toBe(2);
    });

    test('should update lastSeen', async () => {
      const worker = manager.register('worker-1', ['emails']);
      const initial = worker.lastSeen;

      await Bun.sleep(10);
      manager.incrementActive(worker.id);

      expect(worker.lastSeen).toBeGreaterThan(initial);
    });
  });

  describe('jobCompleted', () => {
    test('should decrement active and increment processed', () => {
      const worker = manager.register('worker-1', ['emails']);

      manager.incrementActive(worker.id);
      manager.incrementActive(worker.id);
      expect(worker.activeJobs).toBe(2);

      manager.jobCompleted(worker.id);
      expect(worker.activeJobs).toBe(1);
      expect(worker.processedJobs).toBe(1);
    });

    test('should not go below 0 active jobs', () => {
      const worker = manager.register('worker-1', ['emails']);

      manager.jobCompleted(worker.id);
      expect(worker.activeJobs).toBe(0);
    });
  });

  describe('jobFailed', () => {
    test('should decrement active and increment failed', () => {
      const worker = manager.register('worker-1', ['emails']);

      manager.incrementActive(worker.id);
      manager.jobFailed(worker.id);

      expect(worker.activeJobs).toBe(0);
      expect(worker.failedJobs).toBe(1);
    });
  });

  describe('list', () => {
    test('should return all workers', () => {
      manager.register('worker-1', ['q1']);
      manager.register('worker-2', ['q2']);
      manager.register('worker-3', ['q3']);

      const workers = manager.list();
      expect(workers.length).toBe(3);
    });

    test('should return empty array when no workers', () => {
      expect(manager.list()).toEqual([]);
    });
  });

  describe('listActive', () => {
    test('should return only recently active workers', async () => {
      const w1 = manager.register('worker-1', ['q1']);
      manager.register('worker-2', ['q2']);

      // Update w1's heartbeat
      manager.heartbeat(w1.id);

      const active = manager.listActive();
      // Both should be active since just registered
      expect(active.length).toBe(2);
    });
  });

  describe('getForQueue', () => {
    test('should return workers for specific queue', () => {
      manager.register('worker-1', ['emails', 'notifications']);
      manager.register('worker-2', ['emails']);
      manager.register('worker-3', ['tasks']);

      const emailWorkers = manager.getForQueue('emails');
      expect(emailWorkers.length).toBe(2);

      const taskWorkers = manager.getForQueue('tasks');
      expect(taskWorkers.length).toBe(1);

      const noWorkers = manager.getForQueue('non-existent');
      expect(noWorkers.length).toBe(0);
    });
  });

  describe('getStats', () => {
    test('should return correct stats', () => {
      const w1 = manager.register('worker-1', ['q1']);
      const w2 = manager.register('worker-2', ['q2']);

      manager.incrementActive(w1.id);
      manager.incrementActive(w1.id);
      manager.jobCompleted(w1.id);
      manager.jobFailed(w2.id);

      const stats = manager.getStats();
      expect(stats.total).toBe(2);
      expect(stats.active).toBe(2); // Both registered recently
      expect(stats.totalProcessed).toBe(1);
      expect(stats.totalFailed).toBe(1);
      expect(stats.activeJobs).toBe(1);
    });

    test('should return zero stats when no workers', () => {
      const stats = manager.getStats();
      expect(stats.total).toBe(0);
      expect(stats.active).toBe(0);
      expect(stats.totalProcessed).toBe(0);
      expect(stats.totalFailed).toBe(0);
      expect(stats.activeJobs).toBe(0);
    });
  });
});
