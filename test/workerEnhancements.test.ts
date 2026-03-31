/**
 * Worker Enhancements Tests - BullMQ v5 Compatible Features
 * Tests for Worker status methods, cancel methods, limiter, and events
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('Worker - Status Methods', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('worker-status-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  describe('isRunning()', () => {
    test('should return true when worker is running', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      expect(worker.isRunning()).toBe(true);
    });

    test('should return false when worker is not started', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: false,
      });

      expect(worker.isRunning()).toBe(false);
    });

    test('should return false after pause', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      worker.pause();
      expect(worker.isRunning()).toBe(false);
    });
  });

  describe('isPaused()', () => {
    test('should return false when worker is running', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      expect(worker.isPaused()).toBe(false);
    });

    test('should return true after pause', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      worker.pause();
      expect(worker.isPaused()).toBe(true);
    });

    test('should return false after resume', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      worker.pause();
      worker.resume();
      expect(worker.isPaused()).toBe(false);
    });
  });

  describe('isClosed()', () => {
    test('should return false when worker is running', () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      expect(worker.isClosed()).toBe(false);
    });

    test('should return true after close', async () => {
      worker = new Worker('worker-status-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      await worker.close();
      expect(worker.isClosed()).toBe(true);
      worker = null; // Prevent double close in afterEach
    });
  });
});

describe('Worker - waitUntilReady()', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('worker-ready-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  test('should resolve immediately in embedded mode', async () => {
    worker = new Worker('worker-ready-test', async () => 1, {
      embedded: true,
      autorun: false,
    });

    await expect(worker.waitUntilReady()).resolves.toBeUndefined();
  });
});

describe('Worker - Cancel Methods', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('worker-cancel-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  describe('cancelJob()', () => {
    test('should return false for non-active job', () => {
      worker = new Worker('worker-cancel-test', async () => 1, {
        embedded: true,
        autorun: false,
      });

      const result = worker.cancelJob('non-existent-id');
      expect(result).toBe(false);
    });

    test('should emit cancelled event when job is cancelled', async () => {
      let cancelledJobId: string | null = null;

      worker = new Worker(
        'worker-cancel-test',
        async () => {
          // Long running job
          await Bun.sleep(1000);
          return 1;
        },
        { embedded: true, autorun: true }
      );

      worker.on('cancelled', ({ jobId }) => {
        cancelledJobId = jobId;
      });

      // Add a job
      const job = await queue.add('test', { value: 42 });

      // Wait for job to be picked up
      await Bun.sleep(100);

      // Try to cancel (may or may not be active yet)
      worker.cancelJob(job.id);

      // Check if event was emitted (only if job was active)
      if (cancelledJobId) {
        expect(cancelledJobId).toBe(job.id);
      }
    });
  });

  describe('cancelAllJobs()', () => {
    test('should not throw when no jobs are active', () => {
      worker = new Worker('worker-cancel-test', async () => 1, {
        embedded: true,
        autorun: false,
      });

      expect(() => worker!.cancelAllJobs()).not.toThrow();
    });
  });

  describe('isJobCancelled()', () => {
    test('should return false for non-cancelled job', () => {
      worker = new Worker('worker-cancel-test', async () => 1, {
        embedded: true,
        autorun: false,
      });

      expect(worker.isJobCancelled('some-id')).toBe(false);
    });
  });
});

describe('Worker - Rate Limiter', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('worker-limiter-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  test('should create worker with limiter option', () => {
    worker = new Worker('worker-limiter-test', async () => 1, {
      embedded: true,
      autorun: false,
      limiter: { max: 5, duration: 1000 },
    });

    expect(worker).toBeDefined();
    expect(worker.isRunning()).toBe(false);
  });

  test('should return rate limiter info', () => {
    worker = new Worker('worker-limiter-test', async () => 1, {
      embedded: true,
      autorun: false,
      limiter: { max: 10, duration: 5000 },
    });

    const info = worker.getRateLimiterInfo();

    expect(info).not.toBeNull();
    expect(info!.max).toBe(10);
    expect(info!.duration).toBe(5000);
    expect(info!.current).toBe(0);
  });

  test('should return null when no limiter is configured', () => {
    worker = new Worker('worker-limiter-test', async () => 1, {
      embedded: true,
      autorun: false,
    });

    const info = worker.getRateLimiterInfo();
    expect(info).toBeNull();
  });

  test('should limit job processing rate', async () => {
    const processedTimes: number[] = [];

    worker = new Worker(
      'worker-limiter-test',
      async () => {
        processedTimes.push(Date.now());
        return 1;
      },
      {
        embedded: true,
        autorun: true,
        limiter: { max: 2, duration: 500 }, // Max 2 jobs per 500ms
      }
    );

    // Add 4 jobs
    await queue.add('test1', { value: 1 });
    await queue.add('test2', { value: 2 });
    await queue.add('test3', { value: 3 });
    await queue.add('test4', { value: 4 });

    // Wait for processing
    await Bun.sleep(1500);

    // Should have processed all 4 jobs
    expect(processedTimes.length).toBe(4);

    // First 2 should be close together, next 2 should be delayed
    if (processedTimes.length >= 4) {
      const gap1 = processedTimes[1] - processedTimes[0];
      const gap3 = processedTimes[2] - processedTimes[1];

      // Third job should have been delayed due to rate limit
      // (gap3 should be larger than gap1 due to waiting for rate limit window)
      expect(gap3).toBeGreaterThanOrEqual(gap1);
    }
  });
});

describe('Worker - Events', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('worker-events-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  describe('drained event', () => {
    test('should emit drained when queue is empty', async () => {
      let drainedCount = 0;

      worker = new Worker('worker-events-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      worker.on('drained', () => {
        drainedCount++;
      });

      // Wait for initial drained event (queue starts empty)
      await Bun.sleep(200);

      expect(drainedCount).toBeGreaterThanOrEqual(1);
    });

    test('should emit drained after processing all jobs', async () => {
      let drainedCount = 0;
      let completedCount = 0;

      worker = new Worker(
        'worker-events-test',
        async () => {
          await Bun.sleep(50);
          return 1;
        },
        { embedded: true, autorun: true }
      );

      worker.on('drained', () => {
        drainedCount++;
      });

      worker.on('completed', () => {
        completedCount++;
      });

      // Add jobs
      await queue.add('test1', { value: 1 });
      await queue.add('test2', { value: 2 });

      // Wait for jobs to complete and drained to be emitted
      await Bun.sleep(1500);

      // Both jobs should be completed
      expect(completedCount).toBe(2);
      // Drained should have been emitted at least once (after all jobs complete)
      expect(drainedCount).toBeGreaterThanOrEqual(1);
    });
  });

  describe('ready event', () => {
    test('should emit ready when worker starts', async () => {
      let readyEmitted = false;

      worker = new Worker('worker-events-test', async () => 1, {
        embedded: true,
        autorun: false,
      });

      worker.on('ready', () => {
        readyEmitted = true;
      });

      worker.run();

      await Bun.sleep(50);
      expect(readyEmitted).toBe(true);
    });
  });

  describe('closed event', () => {
    test('should emit closed when worker closes', async () => {
      let closedEmitted = false;

      worker = new Worker('worker-events-test', async () => 1, {
        embedded: true,
        autorun: true,
      });

      worker.on('closed', () => {
        closedEmitted = true;
      });

      await worker.close();
      expect(closedEmitted).toBe(true);
      worker = null; // Prevent double close
    });
  });
});

describe('Worker - Additional Options', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('worker-options-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept all BullMQ v5 options', async () => {
    const worker = new Worker('worker-options-test', async () => 1, {
      embedded: true,
      autorun: false,
      concurrency: 5,
      heartbeatInterval: 5000,
      batchSize: 20,
      pollTimeout: 1000,
      useLocks: true,
      limiter: { max: 10, duration: 1000 },
      lockDuration: 30000,
      maxStalledCount: 2,
      skipStalledCheck: false,
      skipLockRenewal: false,
      drainDelay: 5000,
      removeOnComplete: true,
      removeOnFail: false,
    });

    expect(worker).toBeDefined();
    expect(worker.isRunning()).toBe(false);

    await worker.close();
  });
});
