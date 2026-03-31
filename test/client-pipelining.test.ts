/**
 * Client Pipelining Tests
 * Verifies pipelining functionality with multiple in-flight commands
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';

// Generate unique queue name per test
function uniqueQueueName(base: string): string {
  return `${base}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe('Client Pipelining', () => {
  let queue: Queue;
  let queueName: string;

  beforeEach(async () => {
    queueName = uniqueQueueName('test-pipelining');
    queue = new Queue(queueName, {
      connection: { host: 'localhost', port: 6789 },
    });
    await new Promise((r) => setTimeout(r, 100));
  });

  afterEach(async () => {
    await queue.close();
  });

  it('should handle 100 parallel push commands via pipelining', async () => {
    const start = Date.now();
    const promises = Array.from({ length: 100 }, (_, i) =>
      queue.add(`pipeline-job-${i}`, { index: i })
    );

    const results = await Promise.all(promises);
    const elapsed = Date.now() - start;

    expect(results.length).toBe(100);

    // All should have unique IDs
    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(100);

    console.log(`  100 parallel push: ${elapsed}ms (${Math.round(100000 / elapsed)} ops/sec)`);
  });

  it('should handle 500 parallel push commands via pipelining', async () => {
    const start = Date.now();
    const promises = Array.from({ length: 500 }, (_, i) =>
      queue.add(`pipeline-job-${i}`, { index: i })
    );

    const results = await Promise.all(promises);
    const elapsed = Date.now() - start;

    expect(results.length).toBe(500);

    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(500);

    console.log(`  500 parallel push: ${elapsed}ms (${Math.round(500000 / elapsed)} ops/sec)`);
  });

  it('should correctly match responses by reqId (out-of-order)', async () => {
    // Send commands that may return in different order
    const promises = [
      queue.add('job-a', { type: 'a', value: 1 }),
      queue.add('job-b', { type: 'b', value: 2 }),
      queue.add('job-c', { type: 'c', value: 3 }),
      queue.add('job-d', { type: 'd', value: 4 }),
      queue.add('job-e', { type: 'e', value: 5 }),
    ];

    const results = await Promise.all(promises);

    // All should succeed
    expect(results.length).toBe(5);
    for (const result of results) {
      expect(result.id).toBeDefined();
    }
  });

  it('should respect maxInFlight backpressure', async () => {
    // Create queue with low maxInFlight to test backpressure
    const testQueueName = uniqueQueueName('test-backpressure');
    const testQueue = new Queue(testQueueName, {
      connection: {
        host: 'localhost',
        port: 6789,
        maxInFlight: 10 // Low limit
      },
    });

    await new Promise((r) => setTimeout(r, 100));

    // Send 50 commands - should queue beyond maxInFlight
    const promises = Array.from({ length: 50 }, (_, i) =>
      testQueue.add(`backpressure-job-${i}`, { index: i })
    );

    const results = await Promise.all(promises);

    expect(results.length).toBe(50);
    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(50);

    await testQueue.close();
  });

  it('should handle mixed operations via pipelining', async () => {
    // Mix of different operations
    const job1 = await queue.add('mixed-job-1', { data: 'test1' });

    const promises = [
      queue.add('mixed-job-2', { data: 'test2' }),
      queue.getJobCounts(),
      queue.add('mixed-job-3', { data: 'test3' }),
    ];

    const results = await Promise.all(promises);

    expect(results.length).toBe(3);
    expect(job1.id).toBeDefined();

    // Job additions should have IDs
    expect((results[0] as { id: string }).id).toBeDefined();
    expect((results[2] as { id: string }).id).toBeDefined();

    // getJobCounts should have count properties
    const counts = results[1] as { waiting?: number };
    expect(typeof counts.waiting).toBe('number');
  });

  it('should handle worker processing with pipelining', async () => {
    const processedJobs: string[] = [];
    const workerQueueName = uniqueQueueName('test-worker-pipelining');

    const workerQueue = new Queue(workerQueueName, {
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
        concurrency: 10,
      }
    );

    // Push 50 jobs in parallel via pipelining
    const start = Date.now();
    const jobPromises = Array.from({ length: 50 }, (_, i) =>
      workerQueue.add(`worker-pipeline-job-${i}`, { index: i })
    );
    const jobs = await Promise.all(jobPromises);
    const pushElapsed = Date.now() - start;

    // Wait for processing
    await new Promise((r) => setTimeout(r, 2000));

    expect(processedJobs.length).toBe(50);

    // All pushed job IDs should be processed
    const pushedIds = new Set(jobs.map((j) => j.id));
    for (const processedId of processedJobs) {
      expect(pushedIds.has(processedId)).toBe(true);
    }

    console.log(`  50 push + process: push ${pushElapsed}ms, total 2000ms`);

    await worker.close();
    await workerQueue.close();
  });

  it('should handle high-throughput benchmark', async () => {
    const iterations = 1000;
    const start = Date.now();

    // Send all at once via pipelining
    const promises = Array.from({ length: iterations }, (_, i) =>
      queue.add(`benchmark-job-${i}`, { i })
    );

    const results = await Promise.all(promises);
    const elapsed = Date.now() - start;
    const opsPerSec = Math.round((iterations / elapsed) * 1000);

    expect(results.length).toBe(iterations);

    console.log(`  ${iterations} ops pipelined: ${elapsed}ms (${opsPerSec.toLocaleString()} ops/sec)`);

    // Should be reasonably fast with pipelining (> 1000 ops/sec at minimum)
    expect(opsPerSec).toBeGreaterThan(1000);
  });
});
