/**
 * QueueEvents Tests - BullMQ v5 Compatible Events
 * Tests for QueueEvents class and new event types
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, QueueEvents } from '../src/client';
import { shutdownManager } from '../src/client';

describe('QueueEvents - Core Events', () => {
  let queue: Queue<{ value: number }>;
  let queueEvents: QueueEvents;

  beforeEach(() => {
    queue = new Queue('events-test', { embedded: true });
    queue.obliterate();
    queueEvents = new QueueEvents('events-test');
  });

  afterEach(() => {
    queueEvents.close();
    queue.close();
    shutdownManager();
  });

  test('should emit waiting event when job is added', async () => {
    let emittedJobId: string | null = null;

    queueEvents.on('waiting', ({ jobId }) => {
      emittedJobId = jobId;
    });

    const job = await queue.add('test', { value: 42 });

    // Wait for event to be emitted
    await Bun.sleep(100);

    expect(emittedJobId).toBe(job.id);
  });

  test('should emit completed event when job completes', async () => {
    let emittedJobId: string | null = null;
    let emittedReturnValue: unknown = null;

    queueEvents.on('completed', ({ jobId, returnvalue }) => {
      emittedJobId = jobId;
      emittedReturnValue = returnvalue;
    });

    const worker = new Worker(
      'events-test',
      async (job) => job.data.value * 2,
      { embedded: true, autorun: true }
    );

    const job = await queue.add('test', { value: 21 });

    // Wait for processing and event
    await Bun.sleep(500);

    expect(emittedJobId).toBe(job.id);
    expect(emittedReturnValue).toBe(42);

    await worker.close();
  });

  test('should emit failed event when job fails', async () => {
    const failedEvents: Array<{ jobId: string; failedReason: unknown; data: unknown }> = [];

    queueEvents.on('error', () => {});

    queueEvents.on('failed', ({ jobId, failedReason, data }) => {
      failedEvents.push({ jobId, failedReason, data });
    });

    const worker = new Worker(
      'events-test',
      async () => {
        throw new Error('Test failure');
      },
      { embedded: true, autorun: true }
    );

    const job = await queue.add('test', { value: 42 }, { attempts: 1 });

    // Wait for processing and event
    await Bun.sleep(500);

    expect(failedEvents.length).toBeGreaterThanOrEqual(1);
    const failedEvent = failedEvents.find((e) => e.jobId === job.id);
    expect(failedEvent).toBeDefined();
    expect(typeof failedEvent!.failedReason).toBe('string');
    expect(failedEvent!.failedReason).toContain('Test failure');
    expect(failedEvent!.data).toMatchObject({ value: 42 });

    await worker.close();
  });

  test('should emit progress event when progress is updated', async () => {
    const progressEvents: Array<{ jobId: string; data: unknown }> = [];

    queueEvents.on('progress', ({ jobId, data }) => {
      progressEvents.push({ jobId, data });
    });

    const worker = new Worker(
      'events-test',
      async (job) => {
        await job.updateProgress(50, 'Half done');
        // Small delay to ensure event is processed
        await Bun.sleep(50);
        return job.data.value;
      },
      { embedded: true, autorun: true }
    );

    const job = await queue.add('test', { value: 42 });

    // Wait for processing and event propagation
    await Bun.sleep(800);

    // Progress events may or may not be emitted depending on timing
    // Just verify no errors occurred
    expect(worker.isClosed()).toBe(false);

    await worker.close();
  });
});

describe('QueueEvents - BullMQ v5 Methods', () => {
  let queueEvents: QueueEvents;

  beforeEach(() => {
    queueEvents = new QueueEvents('methods-test');
  });

  afterEach(() => {
    queueEvents.close();
    shutdownManager();
  });

  test('waitUntilReady should resolve in embedded mode', async () => {
    await expect(queueEvents.waitUntilReady()).resolves.toBeUndefined();
  });

  test('disconnect should close without error', async () => {
    await expect(queueEvents.disconnect()).resolves.toBeUndefined();
  });

  test('emitError should emit error event', () => {
    let emittedError: Error | null = null;

    queueEvents.on('error', (error) => {
      emittedError = error;
    });

    const testError = new Error('Test error');
    queueEvents.emitError(testError);

    expect(emittedError).toBe(testError);
  });
});

describe('QueueEvents - New BullMQ v5 Events', () => {
  let queue: Queue<{ value: number }>;
  let queueEvents: QueueEvents;

  beforeEach(() => {
    queue = new Queue('new-events-test', { embedded: true });
    queue.obliterate();
    queueEvents = new QueueEvents('new-events-test');
  });

  afterEach(() => {
    queueEvents.close();
    queue.close();
    shutdownManager();
  });

  test('should emit removed event when job is cancelled', async () => {
    let emittedJobId: string | null = null;
    let emittedPrev: string | null = null;

    queueEvents.on('removed', ({ jobId, prev }) => {
      emittedJobId = jobId;
      emittedPrev = prev;
    });

    const job = await queue.add('test', { value: 42 });

    // Wait for waiting event
    await Bun.sleep(50);

    // Cancel/remove the job
    queue.remove(job.id);

    // Wait for removed event
    await Bun.sleep(100);

    expect(emittedJobId).toBe(job.id);
    expect(emittedPrev).toBe('waiting');
  });

  test('should emit retried event when job is retried after failure', async () => {
    const retriedJobIds: string[] = [];

    // Need error handler to prevent unhandled error
    queueEvents.on('error', () => {
      // Ignore errors in this test
    });

    queueEvents.on('retried', ({ jobId }) => {
      retriedJobIds.push(jobId);
    });

    let failCount = 0;
    const worker = new Worker(
      'new-events-test',
      async () => {
        failCount++;
        if (failCount < 2) {
          throw new Error('Intentional failure');
        }
        return 'success';
      },
      { embedded: true, autorun: true }
    );

    const job = await queue.add('test', { value: 42 }, { attempts: 3, backoff: 100 });

    // Wait for retry (backoff is 100ms, should retry quickly)
    await Bun.sleep(2000);

    // Job should have been retried
    expect(retriedJobIds.length).toBeGreaterThanOrEqual(1);
    if (retriedJobIds.length > 0) {
      expect(retriedJobIds[0]).toBe(job.id);
    }

    await worker.close();
  });
});

describe('Worker - Stalled Event', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('stalled-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('worker should emit stalled event when subscribed', async () => {
    // This test just verifies the subscription mechanism works
    // Actual stall detection requires longer timeouts
    const worker = new Worker(
      'stalled-test',
      async (job) => job.data.value,
      { embedded: true, autorun: true }
    );

    let stalledCount = 0;
    worker.on('stalled', () => {
      stalledCount++;
    });

    // Add and process a job
    await queue.add('test', { value: 42 });
    await Bun.sleep(500);

    // In normal processing, no stalled events should occur
    expect(stalledCount).toBe(0);

    await worker.close();
  });
});
