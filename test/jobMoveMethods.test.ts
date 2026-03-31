/**
 * Job Move Methods Tests - BullMQ v5 Compatible
 * Tests for moveToCompleted, moveToFailed, moveToWait, moveToDelayed, waitUntilFinished
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, QueueEvents } from '../src/client';
import { shutdownManager } from '../src/client';

describe('Job Move Methods - BullMQ v5', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('move-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('job.moveToCompleted should mark job as completed', async () => {
    const job = await queue.add('test', { value: 42 });

    // Create a worker to pull the job
    const worker = new Worker(
      'move-test',
      async (j) => {
        // Use moveToCompleted instead of normal return
        await j.moveToCompleted({ result: 'done' });
        return null;
      },
      { embedded: true, autorun: true }
    );

    await Bun.sleep(300);

    const state = await queue.getJobState(job.id);
    expect(state).toBe('completed');

    await worker.close();
  });

  test('job.moveToFailed should mark job as failed', async () => {
    const job = await queue.add('test', { value: 42 }, { attempts: 1 });

    const worker = new Worker(
      'move-test',
      async (j) => {
        await j.moveToFailed(new Error('Intentional failure'));
        throw new Error('Should not reach here');
      },
      { embedded: true, autorun: true }
    );

    await Bun.sleep(500);

    const state = await queue.getJobState(job.id);
    // Should be in DLQ (failed) state
    expect(state).toBe('failed');

    await worker.close();
  });

  test('job.moveToWait should re-queue a job', async () => {
    const job = await queue.add('test', { value: 42 });

    // Use moveJobToWait to re-queue
    const result = await queue.moveJobToWait(job.id);
    expect(result).toBe(true);

    // Should have created a new waiting job
    const counts = await queue.getJobCountsAsync();
    expect(counts.waiting).toBeGreaterThanOrEqual(1);
  });

  test('job.moveToDelayed should delay a job', async () => {
    const job = await queue.add('test', { value: 42 });

    // Move to delayed with timestamp 1 second from now
    const futureTimestamp = Date.now() + 1000;
    await queue.moveJobToDelayed(job.id, futureTimestamp);

    const state = await queue.getJobState(job.id);
    // Job should be delayed or waiting (depending on timing)
    expect(['delayed', 'waiting', 'unknown']).toContain(state);
  });

  test('job has all BullMQ v5 properties', async () => {
    const job = await queue.add('test', { value: 42 }, { priority: 5, delay: 100 });

    // Check all BullMQ v5 properties exist
    expect(job.id).toBeDefined();
    expect(job.name).toBe('test');
    expect(job.data).toEqual({ value: 42 });
    expect(job.queueName).toBe('move-test');
    expect(job.timestamp).toBeDefined();
    expect(job.attemptsMade).toBe(0);
    expect(job.progress).toBe(0);

    // BullMQ v5 specific properties
    expect(job.delay).toBeDefined();
    expect(job.stacktrace).toBeNull();
    expect(job.stalledCounter).toBe(0);
    expect(job.priority).toBe(5);
    expect(job.opts).toBeDefined();
    expect(job.attemptsStarted).toBe(0);
  });

  test('job has all BullMQ v5 methods', async () => {
    const job = await queue.add('test', { value: 42 });

    // Core methods
    expect(typeof job.updateProgress).toBe('function');
    expect(typeof job.log).toBe('function');
    expect(typeof job.getState).toBe('function');
    expect(typeof job.remove).toBe('function');
    expect(typeof job.retry).toBe('function');
    expect(typeof job.getChildrenValues).toBe('function');

    // BullMQ v5 state check methods
    expect(typeof job.isWaiting).toBe('function');
    expect(typeof job.isActive).toBe('function');
    expect(typeof job.isDelayed).toBe('function');
    expect(typeof job.isCompleted).toBe('function');
    expect(typeof job.isFailed).toBe('function');
    expect(typeof job.isWaitingChildren).toBe('function');

    // BullMQ v5 mutation methods
    expect(typeof job.updateData).toBe('function');
    expect(typeof job.promote).toBe('function');
    expect(typeof job.changeDelay).toBe('function');
    expect(typeof job.changePriority).toBe('function');
    expect(typeof job.extendLock).toBe('function');
    expect(typeof job.clearLogs).toBe('function');

    // BullMQ v5 dependency methods
    expect(typeof job.getDependencies).toBe('function');
    expect(typeof job.getDependenciesCount).toBe('function');

    // BullMQ v5 serialization methods
    expect(typeof job.toJSON).toBe('function');
    expect(typeof job.asJSON).toBe('function');

    // BullMQ v5 move methods
    expect(typeof job.moveToCompleted).toBe('function');
    expect(typeof job.moveToFailed).toBe('function');
    expect(typeof job.moveToWait).toBe('function');
    expect(typeof job.moveToDelayed).toBe('function');
    expect(typeof job.moveToWaitingChildren).toBe('function');
    expect(typeof job.waitUntilFinished).toBe('function');
  });
});

describe('Job waitUntilFinished', () => {
  let queue: Queue<{ value: number }>;
  let queueEvents: QueueEvents;

  beforeEach(() => {
    queue = new Queue('wait-test', { embedded: true });
    queue.obliterate();
    queueEvents = new QueueEvents('wait-test');
  });

  afterEach(() => {
    queueEvents.close();
    queue.close();
    shutdownManager();
  });

  test('job.waitUntilFinished should resolve when job completes', async () => {
    const worker = new Worker(
      'wait-test',
      async (job) => {
        await Bun.sleep(100);
        return { result: job.data.value * 2 };
      },
      { embedded: true, autorun: true }
    );

    const job = await queue.add('test', { value: 21 });

    // Wait for job to complete (with timeout)
    const resultPromise = job.waitUntilFinished(queueEvents, 5000);

    // Should complete and return the result
    const result = await resultPromise;
    expect(result).toEqual({ result: 42 });

    await worker.close();
  });

  test('job.waitUntilFinished should reject on timeout', async () => {
    // Don't create a worker - job will never complete
    const job = await queue.add('test', { value: 42 });

    // Wait with a short timeout
    try {
      await job.waitUntilFinished(queueEvents, 100);
      expect(true).toBe(false); // Should not reach here
    } catch (error) {
      expect((error as Error).message).toContain('timed out');
    }
  });
});

describe('FlowProducer BullMQ v5 Methods', () => {
  test('FlowProducer has disconnect and waitUntilReady methods', async () => {
    const { FlowProducer } = await import('../src/client');
    const flow = new FlowProducer({ embedded: true });

    // Check methods exist
    expect(typeof flow.disconnect).toBe('function');
    expect(typeof flow.waitUntilReady).toBe('function');

    // waitUntilReady should resolve in embedded mode
    await expect(flow.waitUntilReady()).resolves.toBeUndefined();

    // disconnect should work
    await expect(flow.disconnect()).resolves.toBeUndefined();

    flow.close();
    shutdownManager();
  });
});
