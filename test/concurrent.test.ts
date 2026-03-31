/**
 * Concurrent Operations Tests
 * Tests for race conditions, parallel operations, and thread safety
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Concurrent Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Concurrent Push', () => {
    test('should handle concurrent pushes', async () => {
      const pushes = Array.from({ length: 100 }, (_, i) =>
        qm.push('test', { data: { id: i } })
      );

      const jobs = await Promise.all(pushes);

      expect(jobs.length).toBe(100);
      const ids = new Set(jobs.map((j) => j.id));
      expect(ids.size).toBe(100); // All unique IDs
    });

    test('should handle concurrent pushes to different queues', async () => {
      const pushes = Array.from({ length: 50 }, (_, i) =>
        qm.push(`queue${i % 5}`, { data: { id: i } })
      );

      const jobs = await Promise.all(pushes);

      expect(jobs.length).toBe(50);
    });
  });

  describe('Concurrent Pull', () => {
    test('should not return same job to concurrent pulls', async () => {
      // Push 10 jobs
      for (let i = 0; i < 10; i++) {
        await qm.push('test', { data: { id: i } });
      }

      // Pull concurrently
      const pulls = Array.from({ length: 10 }, () => qm.pull('test', 0));
      const results = await Promise.all(pulls);

      const pulledJobs = results.filter((r) => r !== null);
      const pulledIds = pulledJobs.map((j) => j!.id);
      const uniqueIds = new Set(pulledIds);

      // Each job should be pulled only once
      expect(uniqueIds.size).toBe(pulledJobs.length);
    });

    test('should handle concurrent pulls from different queues', async () => {
      // Push jobs to different queues
      for (let i = 0; i < 5; i++) {
        await qm.push('queue1', { data: { queue: 1, id: i } });
        await qm.push('queue2', { data: { queue: 2, id: i } });
      }

      // Pull concurrently from both queues
      const pulls = [
        ...Array.from({ length: 5 }, () => qm.pull('queue1', 0)),
        ...Array.from({ length: 5 }, () => qm.pull('queue2', 0)),
      ];

      const results = await Promise.all(pulls);
      const pulledJobs = results.filter((r) => r !== null);

      expect(pulledJobs.length).toBe(10);
    });
  });

  describe('Concurrent Ack', () => {
    test('should handle concurrent acks for different jobs', async () => {
      // Push and pull multiple jobs
      const jobs = await Promise.all(
        Array.from({ length: 10 }, (_, i) => qm.push('test', { data: { id: i } }))
      );

      const pulled = await Promise.all(jobs.map(() => qm.pull('test', 0)));

      // Ack all concurrently - ack returns void, throws on error
      const acks = pulled.filter((j) => j !== null).map((j) => qm.ack(j!.id, { result: 'done' }));

      // All acks should succeed (no errors thrown)
      await expect(Promise.all(acks)).resolves.toBeDefined();
    });

    test('should reject double ack on same job', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });
      await qm.pull('test', 0);

      // First ack should succeed (returns void)
      await qm.ack(job.id, { result: 'first' });

      // Second ack should throw
      await expect(qm.ack(job.id, { result: 'second' })).rejects.toThrow();
    });
  });

  describe('Push + Pull Race', () => {
    test('should handle push and pull racing', async () => {
      // Start pulls before pushes
      const pullPromises = Array.from({ length: 5 }, () => qm.pull('test', 500));

      // Push while pulls are waiting
      await Bun.sleep(50);
      const pushPromises = Array.from({ length: 5 }, (_, i) =>
        qm.push('test', { data: { id: i } })
      );

      const [pulls, pushes] = await Promise.all([
        Promise.all(pullPromises),
        Promise.all(pushPromises),
      ]);

      const pulledJobs = pulls.filter((j) => j !== null);
      expect(pulledJobs.length).toBe(5);
    });
  });

  describe('Cancel + Pull Race', () => {
    test('should handle cancel during pull wait', async () => {
      const job = await qm.push('test', { data: { msg: 'test' } });

      // Start a long pull
      const pullPromise = qm.pull('test', 1000);

      // Cancel while pull might be in progress
      await Bun.sleep(50);
      const cancelResult = await qm.cancel(job.id);

      const pulled = await pullPromise;

      // Either pull got the job before cancel, or pull got null
      if (pulled) {
        expect(cancelResult).toBe(false); // Can't cancel active job
      } else {
        expect(cancelResult).toBe(true); // Job was cancelled
      }
    });
  });

  describe('Multiple Fail on Same Job', () => {
    test('should handle concurrent fails on same job', async () => {
      const job = await qm.push('test', {
        data: { msg: 'test' },
        maxAttempts: 1,
      });
      await qm.pull('test', 0);

      // Try to fail concurrently - catch errors
      const fails = Array.from({ length: 5 }, () =>
        qm.fail(job.id, 'error').then(
          () => 'success',
          () => 'error'
        )
      );
      const results = await Promise.all(fails);

      // Only one should succeed (others throw because job already processed)
      const successes = results.filter((r) => r === 'success');
      expect(successes.length).toBe(1);
    });
  });

  describe('High Concurrency Stress', () => {
    test('should handle high concurrency push/pull/ack cycle', async () => {
      const iterations = 50;
      const queues = ['q1', 'q2', 'q3'];

      // Push phase
      const pushPromises = Array.from({ length: iterations }, (_, i) =>
        qm.push(queues[i % queues.length], { data: { iteration: i } })
      );
      await Promise.all(pushPromises);

      // Pull and ack phase
      const processPromises = Array.from({ length: iterations }, async (_, i) => {
        const queue = queues[i % queues.length];
        const job = await qm.pull(queue, 100);
        if (job) {
          await qm.ack(job.id, { result: i });
          return true;
        }
        return false;
      });

      const results = await Promise.all(processPromises);
      const processed = results.filter((r) => r === true).length;

      expect(processed).toBe(iterations);
    });
  });

  describe('Unique Key Concurrent', () => {
    test('should return same job for duplicate unique keys in concurrent pushes', async () => {
      const pushes = Array.from({ length: 5 }, () =>
        qm.push('test', {
          data: { msg: 'test' },
          uniqueKey: 'same-key',
        })
      );

      const results = await Promise.all(pushes);

      // All should succeed and return the same job ID (BullMQ-style)
      const jobIds = results.map((r) => r.id);
      const uniqueIds = new Set(jobIds);

      expect(uniqueIds.size).toBe(1); // All return same job
      expect(results.length).toBe(5);
    });
  });

  describe('Stats Consistency', () => {
    test('should maintain consistent stats under concurrent operations', async () => {
      const operations: Promise<any>[] = [];

      // Mix of operations
      for (let i = 0; i < 20; i++) {
        operations.push(qm.push('test', { data: { id: i } }));
      }

      await Promise.all(operations);

      // Pull and ack some
      for (let i = 0; i < 10; i++) {
        const job = await qm.pull('test', 0);
        if (job) {
          await qm.ack(job.id, {});
        }
      }

      const stats = qm.getStats();

      // Stats should be consistent
      expect(stats.waiting + stats.active + stats.completed).toBeLessThanOrEqual(20);
    });
  });
});
