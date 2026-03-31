/**
 * Protocol reqId Tests
 * Verifies reqId is always present and echoed correctly for pipelining support
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';

// Generate unique queue name per test
function uniqueQueueName(base: string): string {
  return `${base}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe('Protocol reqId', () => {
  let queue: Queue;
  let worker: Worker | null = null;
  let queueName: string;

  beforeEach(async () => {
    queueName = uniqueQueueName('test-reqid');
    queue = new Queue(queueName, {
      connection: { host: 'localhost', port: 6789 },
    });
    // Wait for connection
    await new Promise((r) => setTimeout(r, 100));
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    await queue.close();
  });

  it('should include reqId in push response', async () => {
    const result = await queue.add('test-job', { data: 'test' });
    expect(result).toBeDefined();
    expect(result.id).toBeDefined();
  });

  it('should handle multiple sequential commands with unique reqIds', async () => {
    const results = [];
    for (let i = 0; i < 10; i++) {
      const result = await queue.add(`job-${i}`, { index: i });
      results.push(result);
    }

    // All should succeed with unique IDs
    expect(results.length).toBe(10);
    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(10); // All unique
  });

  it('should handle parallel commands correctly', async () => {
    // Send 50 commands in parallel
    const promises = Array.from({ length: 50 }, (_, i) =>
      queue.add(`parallel-job-${i}`, { index: i })
    );

    const results = await Promise.all(promises);

    expect(results.length).toBe(50);
    const ids = new Set(results.map((r) => r.id));
    expect(ids.size).toBe(50); // All unique
  });

  it('should maintain reqId through worker processing', async () => {
    const processedJobs: string[] = [];
    const workerQueueName = uniqueQueueName('test-worker-reqid');

    const workerQueue = new Queue(workerQueueName, {
      connection: { host: 'localhost', port: 6789 },
    });

    worker = new Worker(
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
    const jobPromises = Array.from({ length: 10 }, (_, i) =>
      workerQueue.add(`worker-job-${i}`, { index: i })
    );
    const jobs = await Promise.all(jobPromises);

    // Wait for processing
    await new Promise((r) => setTimeout(r, 1000));

    // All jobs should be processed
    expect(processedJobs.length).toBe(10);

    // Job IDs should match what was returned from push
    const pushedIds = new Set(jobs.map((j) => j.id));
    for (const processedId of processedJobs) {
      expect(pushedIds.has(processedId)).toBe(true);
    }

    await workerQueue.close();
  });

  it('should handle bulk push with reqId', async () => {
    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: `bulk-job-${i}`,
      data: { index: i },
    }));

    const result = await queue.addBulk(jobs);

    expect(result.length).toBe(100);
    const ids = new Set(result.map((j) => j.id));
    expect(ids.size).toBe(100); // All unique
  });

  it('should handle getJobCounts with reqId', async () => {
    // Push some jobs first
    await queue.add('count-test', { data: 'test' });

    const counts = await queue.getJobCounts();

    expect(counts).toBeDefined();
    expect(typeof counts.waiting).toBe('number');
    expect(typeof counts.active).toBe('number');
    expect(typeof counts.completed).toBe('number');
    expect(typeof counts.failed).toBe('number');
    // delayed may not be present in all responses
  });

  it('should handle rapid fire commands without losing responses', async () => {
    // Rapid fire 200 commands
    const startTime = Date.now();
    const promises: Promise<{ id: string }>[] = [];

    for (let i = 0; i < 200; i++) {
      promises.push(queue.add(`rapid-${i}`, { i }));
    }

    const results = await Promise.all(promises);
    const elapsed = Date.now() - startTime;

    expect(results.length).toBe(200);

    // Verify all responses are valid
    for (const result of results) {
      expect(result.id).toBeDefined();
      expect(typeof result.id).toBe('string');
      expect(result.id.length).toBeGreaterThan(0);
    }

    console.log(`  Rapid fire: 200 commands in ${elapsed}ms (${Math.round(200000 / elapsed)} ops/sec)`);
  });
});
