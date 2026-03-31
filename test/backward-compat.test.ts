/**
 * Backward Compatibility Tests
 * Ensures pipelining changes don't break existing functionality
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';

// Generate unique queue name per test
function uniqueQueueName(base: string): string {
  return `${base}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe('Backward Compatibility', () => {
  describe('Protocol Negotiation', () => {
    let queue: Queue;
    let queueName: string;

    beforeEach(async () => {
      queueName = uniqueQueueName('test-compat');
      queue = new Queue(queueName, {
        connection: { host: 'localhost', port: 6789 },
      });
      await new Promise((r) => setTimeout(r, 100));
    });

    afterEach(async () => {
      await queue.close();
    });

    it('should work without explicit Hello command', async () => {
      // Standard operations should work without needing Hello
      const result = await queue.add('test-job', { data: 'test' });
      expect(result.id).toBeDefined();

      const counts = await queue.getJobCounts();
      expect(counts.waiting).toBeGreaterThanOrEqual(1);
    });

    it('should handle Hello command for capability discovery', async () => {
      // Send Hello command directly to get server capabilities
      // This tests the Hello endpoint works
      const tcp = (queue as unknown as { tcp: { send: (cmd: unknown) => Promise<unknown> } }).tcp;

      if (tcp && tcp.send) {
        const response = await tcp.send({
          cmd: 'Hello',
          protocolVersion: 2,
          capabilities: ['pipelining'],
        }) as { ok: boolean; protocolVersion?: number; capabilities?: string[] };

        expect(response.ok).toBe(true);
        expect(response.protocolVersion).toBe(2);
        expect(response.capabilities).toContain('pipelining');
      }
    });
  });

  describe('Pipelining Disabled', () => {
    let queue: Queue;
    let queueName: string;

    beforeEach(async () => {
      queueName = uniqueQueueName('test-no-pipeline');
      queue = new Queue(queueName, {
        connection: {
          host: 'localhost',
          port: 6789,
          pipelining: false, // Explicitly disable
        },
      });
      await new Promise((r) => setTimeout(r, 100));
    });

    afterEach(async () => {
      await queue.close();
    });

    it('should work with pipelining disabled', async () => {
      const results = [];
      for (let i = 0; i < 10; i++) {
        const result = await queue.add(`job-${i}`, { index: i });
        results.push(result);
      }

      expect(results.length).toBe(10);
      const ids = new Set(results.map((r) => r.id));
      expect(ids.size).toBe(10);
    });

    it('should handle parallel requests even without pipelining', async () => {
      // Even without client pipelining, parallel requests should work
      // (they just won't be as optimized)
      const promises = Array.from({ length: 20 }, (_, i) =>
        queue.add(`parallel-${i}`, { index: i })
      );

      const results = await Promise.all(promises);
      expect(results.length).toBe(20);
    });
  });

  describe('Legacy Client Simulation', () => {
    let queue: Queue;
    let queueName: string;

    beforeEach(async () => {
      queueName = uniqueQueueName('test-legacy');
      queue = new Queue(queueName, {
        connection: { host: 'localhost', port: 6789 },
      });
      await new Promise((r) => setTimeout(r, 100));
    });

    afterEach(async () => {
      await queue.close();
    });

    it('should handle sequential operations (legacy pattern)', async () => {
      // Simulate legacy sequential pattern
      const job1 = await queue.add('job-1', { data: 1 });
      const job2 = await queue.add('job-2', { data: 2 });
      const job3 = await queue.add('job-3', { data: 3 });

      expect(job1.id).toBeDefined();
      expect(job2.id).toBeDefined();
      expect(job3.id).toBeDefined();

      // Verify jobs are in queue
      const counts = await queue.getJobCounts();
      expect(counts.waiting).toBeGreaterThanOrEqual(3);
    });

    it('should handle mixed sync and async patterns', async () => {
      // Some sequential, some parallel
      const seq1 = await queue.add('seq-1', { type: 'sequential' });

      const parallelPromises = Array.from({ length: 5 }, (_, i) =>
        queue.add(`par-${i}`, { type: 'parallel', i })
      );
      const parallelResults = await Promise.all(parallelPromises);

      const seq2 = await queue.add('seq-2', { type: 'sequential' });

      expect(seq1.id).toBeDefined();
      expect(seq2.id).toBeDefined();
      expect(parallelResults.length).toBe(5);
    });
  });

  describe('Worker Compatibility', () => {
    it('should work with standard worker configuration', async () => {
      const workerQueueName = uniqueQueueName('test-worker-compat');
      const processedJobs: string[] = [];

      const queue = new Queue(workerQueueName, {
        connection: { host: 'localhost', port: 6789 },
      });

      const worker = new Worker(
        workerQueueName,
        async (job) => {
          processedJobs.push(job.id);
          return { processed: true };
        },
        {
          connection: { host: 'localhost', port: 6789 },
          concurrency: 5,
        }
      );

      // Push some jobs
      const jobs = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          queue.add(`worker-job-${i}`, { index: i })
        )
      );

      // Wait for processing
      await new Promise((r) => setTimeout(r, 1500));

      expect(processedJobs.length).toBe(10);

      await worker.close();
      await queue.close();
    });

    it('should work with low concurrency worker', async () => {
      const workerQueueName = uniqueQueueName('test-low-concurrency');
      const processedJobs: string[] = [];

      const queue = new Queue(workerQueueName, {
        connection: { host: 'localhost', port: 6789 },
      });

      // Low concurrency worker (legacy-like)
      const worker = new Worker(
        workerQueueName,
        async (job) => {
          processedJobs.push(job.id);
          return { ok: true };
        },
        {
          connection: { host: 'localhost', port: 6789 },
          concurrency: 1, // Single-threaded
        }
      );

      // Push jobs
      await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          queue.add(`low-conc-${i}`, { index: i })
        )
      );

      // Wait for processing (longer due to low concurrency)
      await new Promise((r) => setTimeout(r, 2000));

      expect(processedJobs.length).toBe(5);

      await worker.close();
      await queue.close();
    });
  });

  describe('Error Handling Compatibility', () => {
    let queue: Queue;
    let queueName: string;

    beforeEach(async () => {
      queueName = uniqueQueueName('test-errors');
      queue = new Queue(queueName, {
        connection: { host: 'localhost', port: 6789 },
      });
      await new Promise((r) => setTimeout(r, 100));
    });

    afterEach(async () => {
      await queue.close();
    });

    it('should handle getJob for non-existent job', async () => {
      const job = await queue.getJob('non-existent-job-id');
      expect(job).toBeNull();
    });

    it('should handle errors in parallel requests', async () => {
      // Mix of valid and invalid operations
      const promises = [
        queue.add('valid-job-1', { data: 1 }),
        queue.getJob('non-existent-1'),
        queue.add('valid-job-2', { data: 2 }),
        queue.getJob('non-existent-2'),
      ];

      const results = await Promise.all(promises);

      // Valid jobs should succeed
      expect((results[0] as { id: string }).id).toBeDefined();
      expect((results[2] as { id: string }).id).toBeDefined();

      // Non-existent jobs should return null
      expect(results[1]).toBeNull();
      expect(results[3]).toBeNull();
    });
  });
});
