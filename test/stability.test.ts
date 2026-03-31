/**
 * Stability Tests for bunqueue
 * Comprehensive tests for production readiness
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

const TEST_DB = './test-stability.db';

async function cleanup() {
  for (const f of [TEST_DB, `${TEST_DB}-wal`, `${TEST_DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

describe('Stability Tests', () => {
  describe('1. Basic Operations Stability', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('push/pull/ack cycle works correctly', async () => {
      const job = await manager.push('test-queue', { data: { message: 'hello' } });
      expect(job).toBeDefined();
      expect(job.id).toBeDefined();

      const pulled = await manager.pull('test-queue', 1000);
      expect(pulled).toBeDefined();
      expect(pulled!.id).toBe(job.id);

      await manager.ack(pulled!.id, { success: true });

      const stats = manager.getStats();
      expect(stats.completed).toBe(1);
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
    });

    test('fail and retry works correctly', async () => {
      const job = await manager.push('test-queue', {
        data: { message: 'will fail' },
        maxAttempts: 3,
        backoff: 10,
      });

      const pulled = await manager.pull('test-queue', 1000);
      expect(pulled).toBeDefined();

      await manager.fail(pulled!.id, 'Test error');

      // Job should be back in queue for retry
      await Bun.sleep(50);
      const stats = manager.getStats();
      expect(stats.waiting + stats.delayed).toBeGreaterThanOrEqual(1);
    });

    test('handles empty queue gracefully', async () => {
      const pulled = await manager.pull('empty-queue', 100);
      expect(pulled).toBeNull();
    });

    test('handles large payloads', async () => {
      const largeData = { array: Array(10000).fill('x'.repeat(100)) };
      const job = await manager.push('large-queue', { data: largeData });
      expect(job).toBeDefined();

      const pulled = await manager.pull('large-queue', 1000);
      expect(pulled).toBeDefined();
      expect((pulled!.data as typeof largeData).array.length).toBe(10000);
    });
  });

  describe('2. Concurrent Operations', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('handles concurrent pushes', async () => {
      const promises = Array(100)
        .fill(null)
        .map((_, i) => manager.push('concurrent-queue', { data: { index: i } }));

      const jobs = await Promise.all(promises);
      expect(jobs.length).toBe(100);
      expect(new Set(jobs.map((j) => String(j.id))).size).toBe(100); // All unique IDs

      const stats = manager.getStats();
      expect(stats.waiting).toBe(100);
    });

    test('handles concurrent pulls', async () => {
      // Push 50 jobs
      for (let i = 0; i < 50; i++) {
        await manager.push('pull-queue', { data: { index: i } });
      }

      // Pull concurrently
      const pullPromises = Array(50)
        .fill(null)
        .map(() => manager.pull('pull-queue', 1000));

      const pulled = await Promise.all(pullPromises);
      const validPulls = pulled.filter((p) => p !== null);
      expect(validPulls.length).toBe(50);

      // All should be unique
      const ids = new Set(validPulls.map((j) => String(j!.id)));
      expect(ids.size).toBe(50);
    });

    test('handles concurrent acks', async () => {
      // Push and pull jobs
      const jobs = [];
      for (let i = 0; i < 50; i++) {
        await manager.push('ack-queue', { data: { index: i } });
      }
      for (let i = 0; i < 50; i++) {
        const job = await manager.pull('ack-queue', 1000);
        if (job) jobs.push(job);
      }

      // Ack concurrently
      const ackPromises = jobs.map((job) => manager.ack(job.id, { done: true }));
      await Promise.all(ackPromises);

      const stats = manager.getStats();
      expect(stats.completed).toBe(50);
      expect(stats.active).toBe(0);
    });
  });

  describe('3. Persistence & Recovery', () => {
    test('jobs survive restart', async () => {
      await cleanup();

      // Create manager and push jobs
      let manager = new QueueManager({ dataPath: TEST_DB });
      await manager.push('persist-queue', { data: { msg: 'job1' } });
      await manager.push('persist-queue', { data: { msg: 'job2' } });
      await manager.push('persist-queue', { data: { msg: 'job3' } });

      let stats = manager.getStats();
      expect(stats.waiting).toBe(3);

      // Shutdown
      manager.shutdown();

      // Restart
      manager = new QueueManager({ dataPath: TEST_DB });

      // Jobs should be recovered
      stats = manager.getStats();
      expect(stats.waiting).toBe(3);

      // Should be pullable
      const job = await manager.pull('persist-queue', 1000);
      expect(job).toBeDefined();

      manager.shutdown();
      await cleanup();
    });

    test('DLQ entries survive restart', async () => {
      await cleanup();

      let manager = new QueueManager({ dataPath: TEST_DB });

      // Push job that will fail
      const job = await manager.push('dlq-persist-queue', {
        data: { msg: 'will fail' },
        maxAttempts: 1,
      });

      // Pull and fail
      const pulled = await manager.pull('dlq-persist-queue', 1000);
      await manager.fail(pulled!.id, 'Intentional failure');

      let stats = manager.getStats();
      expect(stats.dlq).toBe(1);

      // Shutdown and restart
      manager.shutdown();
      manager = new QueueManager({ dataPath: TEST_DB });

      // DLQ should be recovered
      stats = manager.getStats();
      expect(stats.dlq).toBe(1);

      manager.shutdown();
      await cleanup();
    });

    test('completed jobs with results survive restart', async () => {
      await cleanup();

      let manager = new QueueManager({ dataPath: TEST_DB });

      const job = await manager.push('result-queue', { data: { msg: 'test' } });
      const pulled = await manager.pull('result-queue', 1000);
      await manager.ack(pulled!.id, { result: 'success', value: 42 });

      // Verify result stored
      let result = manager.getResult(job.id);
      expect(result).toEqual({ result: 'success', value: 42 });

      // Shutdown and restart
      manager.shutdown();
      manager = new QueueManager({ dataPath: TEST_DB });

      // Result should be recovered
      result = manager.getResult(job.id);
      expect(result).toEqual({ result: 'success', value: 42 });

      manager.shutdown();
      await cleanup();
    });
  });

  describe('4. DLQ Operations', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('job moves to DLQ after max attempts', async () => {
      await manager.push('dlq-queue', {
        data: { msg: 'fail me' },
        maxAttempts: 2,
        backoff: 1,
      });

      // Fail twice
      for (let i = 0; i < 2; i++) {
        await Bun.sleep(10);
        const job = await manager.pull('dlq-queue', 1000);
        if (job) {
          await manager.fail(job.id, `Failure ${i + 1}`);
        }
      }

      await Bun.sleep(50);
      const stats = manager.getStats();
      expect(stats.dlq).toBe(1);
    });

    test('retry from DLQ works', async () => {
      await manager.push('dlq-retry-queue', {
        data: { msg: 'retry me' },
        maxAttempts: 1,
      });

      const job = await manager.pull('dlq-retry-queue', 1000);
      await manager.fail(job!.id, 'Intentional failure');

      let stats = manager.getStats();
      expect(stats.dlq).toBe(1);

      // Retry
      const retried = manager.retryDlq('dlq-retry-queue');
      expect(retried).toBe(1);

      stats = manager.getStats();
      expect(stats.dlq).toBe(0);
      expect(stats.waiting).toBe(1);
    });
  });

  describe('5. Priority & Ordering', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('higher priority jobs are pulled first', async () => {
      await manager.push('priority-queue', { data: { name: 'low' }, priority: 1 });
      await manager.push('priority-queue', { data: { name: 'high' }, priority: 10 });
      await manager.push('priority-queue', { data: { name: 'medium' }, priority: 5 });

      const first = await manager.pull('priority-queue', 100);
      const second = await manager.pull('priority-queue', 100);
      const third = await manager.pull('priority-queue', 100);

      expect((first!.data as { name: string }).name).toBe('high');
      expect((second!.data as { name: string }).name).toBe('medium');
      expect((third!.data as { name: string }).name).toBe('low');
    });

    test('delayed jobs are not pulled early', async () => {
      await manager.push('delayed-queue', {
        data: { name: 'delayed' },
        delay: 500,
      });
      await manager.push('delayed-queue', { data: { name: 'immediate' } });

      const first = await manager.pull('delayed-queue', 100);
      expect((first!.data as { name: string }).name).toBe('immediate');

      const second = await manager.pull('delayed-queue', 100);
      expect(second).toBeNull(); // Delayed job not ready yet
    });
  });

  describe('6. Unique Keys & Deduplication', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('unique key returns existing job for duplicates', async () => {
      const job1 = await manager.push('unique-queue', {
        data: { msg: 'first' },
        uniqueKey: 'unique-123',
      });
      expect(job1).toBeDefined();

      // Second push with same key returns existing job (BullMQ-style)
      const job2 = await manager.push('unique-queue', {
        data: { msg: 'second' },
        uniqueKey: 'unique-123',
      });
      expect(job2.id).toBe(job1.id);

      const stats = manager.getStats();
      expect(stats.waiting).toBe(1);
    });

    test('unique key is released after completion', async () => {
      await manager.push('unique-release-queue', {
        data: { msg: 'first' },
        uniqueKey: 'release-key',
      });

      const job = await manager.pull('unique-release-queue', 100);
      await manager.ack(job!.id, {});

      // Now should be able to push again
      const job2 = await manager.push('unique-release-queue', {
        data: { msg: 'second' },
        uniqueKey: 'release-key',
      });
      expect(job2).toBeDefined();
    });
  });

  describe('7. Rate Limiting & Concurrency', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('concurrency limit is respected', async () => {
      manager.setConcurrency('conc-queue', 2);

      // Push 5 jobs
      for (let i = 0; i < 5; i++) {
        await manager.push('conc-queue', { data: { index: i } });
      }

      // Pull - should get max 2
      const job1 = await manager.pull('conc-queue', 100);
      const job2 = await manager.pull('conc-queue', 100);
      const job3 = await manager.pull('conc-queue', 100);

      expect(job1).toBeDefined();
      expect(job2).toBeDefined();
      expect(job3).toBeNull(); // Concurrency limit reached

      // Ack one, should be able to pull another
      await manager.ack(job1!.id, {});
      const job4 = await manager.pull('conc-queue', 100);
      expect(job4).toBeDefined();
    });
  });

  describe('8. Memory Stability', () => {
    test('handles moderate throughput without memory leak', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      const initialMemory = process.memoryUsage().heapUsed;

      // Push and process jobs in smaller batches with delays
      const BATCH_SIZE = 20;
      const ITERATIONS = 5;

      for (let iter = 0; iter < ITERATIONS; iter++) {
        // Push batch
        for (let i = 0; i < BATCH_SIZE; i++) {
          await manager.push('memory-queue', { data: { iter, i } });
        }

        // Small delay to let SQLite process
        await Bun.sleep(10);

        // Pull and ack all
        for (let i = 0; i < BATCH_SIZE; i++) {
          const job = await manager.pull('memory-queue', 100);
          if (job) await manager.ack(job.id, {});
        }

        // Small delay between iterations
        await Bun.sleep(10);
      }

      // Force GC if available
      if (global.gc) global.gc();

      const finalMemory = process.memoryUsage().heapUsed;
      const memoryGrowth = finalMemory - initialMemory;

      // Memory growth should be reasonable (less than 50MB for 100 jobs)
      expect(memoryGrowth).toBeLessThan(50 * 1024 * 1024);

      const stats = manager.getStats();
      expect(stats.completed).toBe(BATCH_SIZE * ITERATIONS);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('9. Error Recovery', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('ack non-existent job throws error', async () => {
      const fakeId = '999999999' as any;
      await expect(manager.ack(fakeId, {})).rejects.toThrow();
    });

    test('fail non-existent job throws error', async () => {
      const fakeId = '999999999' as any;
      await expect(manager.fail(fakeId, 'error')).rejects.toThrow();
    });

    test('double ack is handled', async () => {
      await manager.push('double-ack-queue', { data: {} });
      const job = await manager.pull('double-ack-queue', 100);

      await manager.ack(job!.id, {});

      // Second ack should throw
      await expect(manager.ack(job!.id, {})).rejects.toThrow();
    });
  });

  describe('10. Worker Integration', () => {
    test('worker processes jobs correctly', async () => {
      await cleanup();
      const processed: number[] = [];

      const queue = new Queue<{ index: number }>('worker-test');
      const worker = new Worker<{ index: number }, number>(
        'worker-test',
        async (job) => {
          processed.push(job.data.index);
          return job.data.index * 2;
        },
        { concurrency: 2 }
      );

      // Add jobs
      await queue.add('task', { index: 1 });
      await queue.add('task', { index: 2 });
      await queue.add('task', { index: 3 });

      // Wait for processing
      await Bun.sleep(500);

      expect(processed.length).toBe(3);
      expect(processed.sort()).toEqual([1, 2, 3]);

      await worker.close();
      await cleanup();
    });

    test('worker handles errors gracefully', async () => {
      await cleanup();
      let errorCount = 0;

      const queue = new Queue<{ shouldFail: boolean }>('error-worker-test');
      const worker = new Worker<{ shouldFail: boolean }, void>(
        'error-worker-test',
        async (job) => {
          if (job.data.shouldFail) {
            throw new Error('Intentional error');
          }
        },
        { concurrency: 1 }
      );

      worker.on('failed', () => {
        errorCount++;
      });

      await queue.add('task', { shouldFail: true });

      await Bun.sleep(300);

      expect(errorCount).toBe(1);

      await worker.close();
      await cleanup();
    });
  });
});
