/**
 * Stress Tests for bunqueue
 * Heavy load testing for production stability
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

const TEST_DB = './test-stress.db';

async function cleanup() {
  for (const f of [TEST_DB, `${TEST_DB}-wal`, `${TEST_DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

describe('Stress Tests', () => {
  describe('1. High Volume Push', () => {
    test('handles 1000 sequential pushes', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      const start = performance.now();
      for (let i = 0; i < 1000; i++) {
        await manager.push('stress-queue', { data: { index: i } });
      }
      const duration = performance.now() - start;

      const stats = manager.getStats();
      expect(stats.waiting).toBe(1000);
      console.log(`1000 sequential pushes: ${duration.toFixed(2)}ms (${(1000 / (duration / 1000)).toFixed(0)} ops/sec)`);

      manager.shutdown();
      await cleanup();
    });

    test('handles 500 parallel pushes', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      const start = performance.now();
      const promises = Array(500)
        .fill(null)
        .map((_, i) => manager.push('parallel-queue', { data: { index: i } }));
      await Promise.all(promises);
      const duration = performance.now() - start;

      const stats = manager.getStats();
      expect(stats.waiting).toBe(500);
      console.log(`500 parallel pushes: ${duration.toFixed(2)}ms (${(500 / (duration / 1000)).toFixed(0)} ops/sec)`);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('2. High Volume Processing', () => {
    test('processes 500 jobs end-to-end', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      // Push 500 jobs
      for (let i = 0; i < 500; i++) {
        await manager.push('process-queue', { data: { index: i } });
      }

      const start = performance.now();
      let processed = 0;

      // Process all jobs
      while (processed < 500) {
        const job = await manager.pull('process-queue', 100);
        if (job) {
          await manager.ack(job.id, { result: 'done' });
          processed++;
        }
      }

      const duration = performance.now() - start;
      const stats = manager.getStats();

      expect(stats.completed).toBe(500);
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
      console.log(`500 jobs processed: ${duration.toFixed(2)}ms (${(500 / (duration / 1000)).toFixed(0)} ops/sec)`);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('3. Multiple Queues', () => {
    test('handles 10 queues with 100 jobs each', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      const QUEUE_COUNT = 10;
      const JOBS_PER_QUEUE = 100;

      // Push to all queues
      for (let q = 0; q < QUEUE_COUNT; q++) {
        for (let j = 0; j < JOBS_PER_QUEUE; j++) {
          await manager.push(`multi-queue-${q}`, { data: { queue: q, job: j } });
        }
      }

      let stats = manager.getStats();
      expect(stats.waiting).toBe(QUEUE_COUNT * JOBS_PER_QUEUE);

      // Process from all queues
      let totalProcessed = 0;
      for (let q = 0; q < QUEUE_COUNT; q++) {
        for (let j = 0; j < JOBS_PER_QUEUE; j++) {
          const job = await manager.pull(`multi-queue-${q}`, 100);
          if (job) {
            await manager.ack(job.id, {});
            totalProcessed++;
          }
        }
      }

      expect(totalProcessed).toBe(QUEUE_COUNT * JOBS_PER_QUEUE);

      stats = manager.getStats();
      expect(stats.completed).toBe(QUEUE_COUNT * JOBS_PER_QUEUE);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('4. Concurrent Workers Simulation', () => {
    test('simulates 5 workers processing concurrently', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      const TOTAL_JOBS = 200;
      const WORKER_COUNT = 5;

      // Push jobs
      for (let i = 0; i < TOTAL_JOBS; i++) {
        await manager.push('worker-queue', { data: { index: i } });
      }

      const processedByWorker: number[] = Array(WORKER_COUNT).fill(0);

      // Simulate workers
      const workerPromises = Array(WORKER_COUNT)
        .fill(null)
        .map(async (_, workerId) => {
          while (true) {
            const job = await manager.pull('worker-queue', 50);
            if (!job) break;

            // Simulate some work
            await Bun.sleep(Math.random() * 5);

            await manager.ack(job.id, { worker: workerId });
            processedByWorker[workerId]++;
          }
        });

      await Promise.all(workerPromises);

      const totalProcessed = processedByWorker.reduce((a, b) => a + b, 0);
      expect(totalProcessed).toBe(TOTAL_JOBS);

      // Check distribution (each worker should have processed some jobs)
      for (let i = 0; i < WORKER_COUNT; i++) {
        expect(processedByWorker[i]).toBeGreaterThan(0);
      }

      console.log(`Worker distribution: ${processedByWorker.join(', ')}`);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('5. Priority Under Load', () => {
    test('maintains priority order under load', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      // Push jobs with different priorities (interleaved)
      for (let i = 0; i < 100; i++) {
        const priority = i % 3 === 0 ? 10 : i % 3 === 1 ? 5 : 1;
        await manager.push('priority-stress-queue', {
          data: { index: i, priority },
          priority,
        });
      }

      // Pull and verify priority order
      let lastPriority = Infinity;
      let orderCorrect = true;

      for (let i = 0; i < 100; i++) {
        const job = await manager.pull('priority-stress-queue', 100);
        if (job) {
          const currentPriority = (job.data as { priority: number }).priority;
          if (currentPriority > lastPriority) {
            orderCorrect = false;
          }
          lastPriority = currentPriority;
          await manager.ack(job.id, {});
        }
      }

      expect(orderCorrect).toBe(true);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('6. Failure Recovery', () => {
    test('handles rapid fail/retry cycles', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      // Push jobs that will fail
      for (let i = 0; i < 50; i++) {
        await manager.push('fail-queue', {
          data: { index: i },
          maxAttempts: 3,
          backoff: 1,
        });
      }

      let failCount = 0;
      let successCount = 0;

      // Process with 50% failure rate
      for (let round = 0; round < 5; round++) {
        await Bun.sleep(20); // Let retries become ready

        while (true) {
          const job = await manager.pull('fail-queue', 50);
          if (!job) break;

          if (Math.random() < 0.5) {
            await manager.fail(job.id, 'Random failure');
            failCount++;
          } else {
            await manager.ack(job.id, { success: true });
            successCount++;
          }
        }
      }

      const stats = manager.getStats();
      console.log(`Failures: ${failCount}, Successes: ${successCount}, DLQ: ${stats.dlq}`);

      // All jobs should eventually be either completed or in DLQ
      expect(stats.completed + stats.dlq).toBe(50);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('7. Delayed Jobs Under Load', () => {
    test('handles many delayed jobs correctly', async () => {
      await cleanup();
      const manager = new QueueManager({ dataPath: TEST_DB });

      // Push mix of immediate and delayed jobs
      for (let i = 0; i < 100; i++) {
        const delay = i % 2 === 0 ? 0 : 100; // Every other job is delayed
        await manager.push('delayed-stress-queue', {
          data: { index: i, delayed: delay > 0 },
          delay,
        });
      }

      let stats = manager.getStats();
      expect(stats.waiting).toBe(50); // Immediate jobs
      expect(stats.delayed).toBe(50); // Delayed jobs

      // Process immediate jobs
      let processed = 0;
      while (true) {
        const job = await manager.pull('delayed-stress-queue', 50);
        if (!job) break;
        await manager.ack(job.id, {});
        processed++;
      }
      expect(processed).toBe(50);

      // Wait for delayed jobs to become ready
      await Bun.sleep(150);

      // Now process delayed jobs
      while (true) {
        const job = await manager.pull('delayed-stress-queue', 50);
        if (!job) break;
        await manager.ack(job.id, {});
        processed++;
      }

      expect(processed).toBe(100);

      stats = manager.getStats();
      expect(stats.completed).toBe(100);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('8. Persistence Under Load', () => {
    test('persists and recovers under load', async () => {
      await cleanup();

      // Phase 1: Create and fill
      let manager = new QueueManager({ dataPath: TEST_DB });
      for (let i = 0; i < 200; i++) {
        await manager.push('persist-stress-queue', { data: { index: i } });
      }
      let stats = manager.getStats();
      expect(stats.waiting).toBe(200);
      manager.shutdown();

      // Phase 2: Recover and partial process
      manager = new QueueManager({ dataPath: TEST_DB });
      stats = manager.getStats();
      expect(stats.waiting).toBe(200); // All recovered

      for (let i = 0; i < 100; i++) {
        const job = await manager.pull('persist-stress-queue', 100);
        if (job) await manager.ack(job.id, {});
      }
      stats = manager.getStats();
      expect(stats.completed).toBe(100);
      expect(stats.waiting).toBe(100);
      manager.shutdown();

      // Phase 3: Recover again and finish
      manager = new QueueManager({ dataPath: TEST_DB });
      stats = manager.getStats();
      expect(stats.waiting).toBe(100); // Remaining recovered

      for (let i = 0; i < 100; i++) {
        const job = await manager.pull('persist-stress-queue', 100);
        if (job) await manager.ack(job.id, {});
      }
      stats = manager.getStats();
      expect(stats.completed).toBe(100);

      manager.shutdown();
      await cleanup();
    });
  });

  describe('9. Edge Cases', () => {
    let manager: QueueManager;

    beforeEach(async () => {
      await cleanup();
      manager = new QueueManager({ dataPath: TEST_DB });
    });

    afterEach(async () => {
      manager.shutdown();
      await cleanup();
    });

    test('handles very long queue names', async () => {
      const longName = 'q'.repeat(200);
      const job = await manager.push(longName, { data: { test: true } });
      expect(job).toBeDefined();

      const pulled = await manager.pull(longName, 100);
      expect(pulled).toBeDefined();
    });

    test('handles special characters in queue names', async () => {
      const specialNames = ['queue-with-dash', 'queue_with_underscore', 'queue.with.dot', 'queue:with:colon'];

      for (const name of specialNames) {
        const job = await manager.push(name, { data: { queue: name } });
        expect(job).toBeDefined();

        const pulled = await manager.pull(name, 100);
        expect(pulled).toBeDefined();
        await manager.ack(pulled!.id, {});
      }
    });

    test('handles null and undefined in job data', async () => {
      const job1 = await manager.push('null-queue', { data: null });
      const job2 = await manager.push('null-queue', { data: { value: null, nested: { deep: null } } });

      expect(job1).toBeDefined();
      expect(job2).toBeDefined();

      const pulled1 = await manager.pull('null-queue', 100);
      const pulled2 = await manager.pull('null-queue', 100);

      expect(pulled1!.data).toBeNull();
      expect((pulled2!.data as any).value).toBeNull();
    });

    test('handles empty strings in job data', async () => {
      const job = await manager.push('empty-string-queue', {
        data: { name: '', nested: { value: '' } },
      });

      const pulled = await manager.pull('empty-string-queue', 100);
      expect((pulled!.data as any).name).toBe('');
    });

    test('handles deeply nested objects', async () => {
      const deepObject: any = { level: 0 };
      let current = deepObject;
      for (let i = 1; i <= 50; i++) {
        current.nested = { level: i };
        current = current.nested;
      }

      const job = await manager.push('deep-queue', { data: deepObject });
      const pulled = await manager.pull('deep-queue', 100);

      // Verify depth
      let depth = 0;
      let obj = pulled!.data as any;
      while (obj && obj.nested) {
        depth++;
        obj = obj.nested;
      }
      expect(depth).toBe(50);
    });
  });
});
