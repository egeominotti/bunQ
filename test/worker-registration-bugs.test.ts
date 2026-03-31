/**
 * Bug reproductions for worker registration and listing
 *
 * Bug 3: Workers missing queue/concurrency fields in response
 * Bug 4: GET /queues/:q/workers not implemented (tested via workerManager)
 * Bug 6: Worker data incomplete (concurrency, status)
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { WorkerManager } from '../src/application/workerManager';

describe('Bug: Worker registration and listing', () => {
  let wm: WorkerManager;

  beforeEach(() => {
    wm = new WorkerManager();
  });

  afterEach(() => {
    wm.stop();
  });

  // ============================================================
  // Bug 3: Workers should store and return concurrency
  // ============================================================
  describe('Bug 3: Worker should include concurrency field', () => {
    test('registered worker should have concurrency field', () => {
      const worker = wm.register('email-worker', ['email-notifications'], 3);
      expect(worker.concurrency).toBe(3);
    });

    test('listed workers should include concurrency', () => {
      wm.register('worker-1', ['queue-a'], 5);
      wm.register('worker-2', ['queue-b'], 10);

      const workers = wm.list();
      expect(workers.length).toBe(2);
      expect(workers[0].concurrency).toBe(5);
      expect(workers[1].concurrency).toBe(10);
    });

    test('concurrency should default to 1 when not specified', () => {
      const worker = wm.register('worker-default', ['queue-a']);
      expect(worker.concurrency).toBe(1);
    });
  });

  // ============================================================
  // Bug 4: getForQueue should work correctly
  // ============================================================
  describe('Bug 4: getForQueue should filter workers by queue', () => {
    test('should return only workers for the specified queue', () => {
      wm.register('worker-a', ['queue-a'], 3);
      wm.register('worker-b', ['queue-b'], 5);
      wm.register('worker-ab', ['queue-a', 'queue-b'], 2);

      const queueAWorkers = wm.getForQueue('queue-a');
      const queueBWorkers = wm.getForQueue('queue-b');
      const queueCWorkers = wm.getForQueue('queue-c');

      expect(queueAWorkers.length).toBe(2); // worker-a and worker-ab
      expect(queueBWorkers.length).toBe(2); // worker-b and worker-ab
      expect(queueCWorkers.length).toBe(0);
    });

    test('should not crash when some workers have undefined/null queues', () => {
      // Register a worker with queues
      wm.register('worker-a', ['queue-a'], 3);
      // Register a worker without queues (simulates TCP registration without queues field)
      wm.register('worker-no-queue', undefined as any);
      wm.register('worker-null-queue', null as any);

      // Should not throw
      const workers = wm.getForQueue('queue-a');
      expect(workers.length).toBe(1);
      expect(workers[0].name).toBe('worker-a');
    });
  });
});
