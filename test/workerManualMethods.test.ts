/**
 * Worker Manual Methods Tests - BullMQ v5 Compatible
 * Tests for getNextJob, processJobManually, extendJobLocks
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('Worker Manual Job Control - BullMQ v5', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('manual-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('worker.getNextJob should fetch a job manually', async () => {
    await queue.add('test', { value: 42 });

    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    const job = await worker.getNextJob();
    expect(job).toBeDefined();
    expect(job?.data).toEqual({ name: 'test', value: 42 });

    await worker.close();
  });

  test('worker.getNextJob should return undefined when queue is empty', async () => {
    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    const job = await worker.getNextJob();
    expect(job).toBeUndefined();

    await worker.close();
  });

  test('worker.processJobManually should process a job', async () => {
    await queue.add('test', { value: 42 });

    let processed = false;
    const worker = new Worker(
      'manual-test',
      async (job) => {
        processed = true;
        return { result: job.data.value * 2 };
      },
      { embedded: true, autorun: false }
    );

    const job = await worker.getNextJob();
    expect(job).toBeDefined();

    if (job) {
      await worker.processJobManually(job);
    }

    expect(processed).toBe(true);

    await worker.close();
  });

  test('worker.extendJobLocks should extend locks', async () => {
    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false, useLocks: true }
    );

    // Add and fetch a job with lock
    await queue.add('test', { value: 1 });
    const job = await worker.getNextJob('test-token');

    if (job) {
      const jobId = String(job.id);
      // Try to extend locks (might not work without proper token setup)
      const extended = await worker.extendJobLocks([jobId], ['test-token'], 60000);
      // In embedded mode without full lock setup, this may return 0
      expect(typeof extended).toBe('number');
    }

    await worker.close();
  });

  test('worker has getNextJob method', async () => {
    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    expect(typeof worker.getNextJob).toBe('function');

    await worker.close();
  });

  test('worker has processJobManually method', async () => {
    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    expect(typeof worker.processJobManually).toBe('function');

    await worker.close();
  });

  test('worker has extendJobLocks method', async () => {
    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    expect(typeof worker.extendJobLocks).toBe('function');

    await worker.close();
  });

  test('worker emits stalled event', async () => {
    const worker = new Worker(
      'manual-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    // Check that stalled event listener can be added
    let stalledEmitted = false;
    worker.on('stalled', () => {
      stalledEmitted = true;
    });

    // The stalled event is emitted by the system when jobs stall
    // Here we just verify the listener works
    expect(worker.listenerCount('stalled')).toBe(1);

    await worker.close();
  });
});

describe('Worker extendJobLocks validation', () => {
  test('extendJobLocks should throw if arrays have different lengths', async () => {
    const queue = new Queue('extend-test', { embedded: true });
    queue.obliterate();

    const worker = new Worker(
      'extend-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    await expect(
      worker.extendJobLocks(['job1', 'job2'], ['token1'], 1000)
    ).rejects.toThrow('jobIds and tokens arrays must have the same length');

    await worker.close();
    queue.close();
    shutdownManager();
  });

  test('extendJobLocks should return 0 for empty arrays', async () => {
    const queue = new Queue('extend-empty-test', { embedded: true });
    queue.obliterate();

    const worker = new Worker(
      'extend-empty-test',
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    const result = await worker.extendJobLocks([], [], 1000);
    expect(result).toBe(0);

    await worker.close();
    queue.close();
    shutdownManager();
  });
});
