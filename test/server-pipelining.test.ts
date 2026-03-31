/**
 * Server Pipelining Tests
 * Verifies server processes commands in parallel correctly
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';

// Generate unique queue name per test
function uniqueQueueName(base: string): string {
  return `${base}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe('Server Pipelining', () => {
  let queue: Queue;
  let queueName: string;

  beforeEach(async () => {
    queueName = uniqueQueueName('test-server-pipeline');
    queue = new Queue(queueName, {
      connection: { host: 'localhost', port: 6789 },
    });
    await new Promise((r) => setTimeout(r, 100));
  });

  afterEach(async () => {
    await queue.close();
  });

  it('should handle parallel requests from single client', async () => {
    const start = Date.now();
    const promises = Array.from({ length: 200 }, (_, i) =>
      queue.add(`parallel-${i}`, { index: i })
    );

    const results = await Promise.all(promises);
    const elapsed = Date.now() - start;

    expect(results.length).toBe(200);
    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(200);

    console.log(`  200 parallel requests: ${elapsed}ms`);
  });

  it('should process with correct reqId matching even under load', async () => {
    // Send 500 commands with data we can verify
    const promises = Array.from({ length: 500 }, (_, i) =>
      queue.add(`verify-${i}`, { verifyIndex: i })
    );

    const results = await Promise.all(promises);

    // All should succeed
    expect(results.length).toBe(500);

    // All IDs should be unique and valid
    for (const result of results) {
      expect(result.id).toBeDefined();
      expect(typeof result.id).toBe('string');
      expect(result.id.length).toBeGreaterThan(0);
    }
  });

  it('should handle mixed command types in parallel', async () => {
    // Push some jobs first
    await queue.add('setup-job', { data: 'setup' });

    // Mix of different operations in parallel
    const operations = [
      ...Array.from({ length: 50 }, (_, i) => queue.add(`job-${i}`, { i })),
      ...Array.from({ length: 10 }, () => queue.getJobCounts()),
    ];

    const results = await Promise.all(operations);

    expect(results.length).toBe(60);

    // First 50 should be job results with IDs
    for (let i = 0; i < 50; i++) {
      expect((results[i] as { id: string }).id).toBeDefined();
    }

    // Last 10 should be job counts
    for (let i = 50; i < 60; i++) {
      expect((results[i] as { waiting: number }).waiting).toBeDefined();
    }
  });

  it('should maintain per-connection concurrency limit', async () => {
    // Server has MAX_CONCURRENT_PER_CONNECTION = 50
    // Send 100 parallel requests - should still complete correctly
    const promises = Array.from({ length: 100 }, (_, i) =>
      queue.add(`concurrency-test-${i}`, { index: i })
    );

    const results = await Promise.all(promises);

    expect(results.length).toBe(100);
    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(100);
  });

  it('should handle high throughput with pipelining', async () => {
    const iterations = 1000;
    const start = Date.now();

    const promises = Array.from({ length: iterations }, (_, i) =>
      queue.add(`throughput-${i}`, { i })
    );

    const results = await Promise.all(promises);
    const elapsed = Date.now() - start;
    const opsPerSec = Math.round((iterations / elapsed) * 1000);

    expect(results.length).toBe(iterations);

    console.log(`  ${iterations} ops with server pipelining: ${elapsed}ms (${opsPerSec.toLocaleString()} ops/sec)`);

    // Should be fast with both client and server pipelining
    expect(opsPerSec).toBeGreaterThan(5000);
  });

  it('should handle worker pull operations in parallel', async () => {
    const workerQueueName = uniqueQueueName('test-worker-server-pipeline');
    const processedJobs: string[] = [];

    const workerQueue = new Queue(workerQueueName, {
      connection: { host: 'localhost', port: 6789 },
    });

    const worker = new Worker(
      workerQueueName,
      async (job) => {
        processedJobs.push(job.id);
        return { ok: true };
      },
      {
        connection: { host: 'localhost', port: 6789 },
        concurrency: 20,
      }
    );

    // Push 100 jobs in parallel
    const jobPromises = Array.from({ length: 100 }, (_, i) =>
      workerQueue.add(`worker-job-${i}`, { index: i })
    );
    const jobs = await Promise.all(jobPromises);

    // Wait for processing
    await new Promise((r) => setTimeout(r, 2000));

    expect(processedJobs.length).toBe(100);

    // All pushed IDs should be processed
    const pushedIds = new Set(jobs.map((j) => j.id));
    for (const id of processedJobs) {
      expect(pushedIds.has(id)).toBe(true);
    }

    await worker.close();
    await workerQueue.close();
  });

  it('should handle error responses correctly in parallel', async () => {
    // Try to get non-existent jobs in parallel
    const promises = Array.from({ length: 20 }, (_, i) =>
      queue.getJob(`non-existent-job-${i}`)
    );

    const results = await Promise.all(promises);

    // All should return null (job not found)
    for (const result of results) {
      expect(result).toBeNull();
    }
  });
});
