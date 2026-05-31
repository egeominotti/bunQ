/**
 * JobOptions BullMQ v5 Tests
 * Tests for all BullMQ v5 compatible JobOptions
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('JobOptions - Backoff Object', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('backoff-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept backoff as number (legacy)', async () => {
    const job = await queue.add('test', { value: 1 }, { backoff: 5000 });
    expect(job.id).toBeDefined();
    expect(job.opts.backoff).toBe(5000);
  });

  test('should accept backoff as fixed object', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { backoff: { type: 'fixed', delay: 3000 } }
    );
    expect(job.id).toBeDefined();
    expect(job.opts.backoff).toEqual({ type: 'fixed', delay: 3000 });
  });

  test('should accept backoff as exponential object', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { backoff: { type: 'exponential', delay: 1000 } }
    );
    expect(job.id).toBeDefined();
    expect(job.opts.backoff).toEqual({ type: 'exponential', delay: 1000 });
  });

  test('fixed backoff should use constant delay on retry', async () => {
    let attempts = 0;
    const delays: number[] = [];
    let lastAttemptTime = Date.now();

    const worker = new Worker(
      'backoff-test',
      async () => {
        const now = Date.now();
        if (attempts > 0) {
          delays.push(now - lastAttemptTime);
        }
        lastAttemptTime = now;
        attempts++;
        if (attempts < 3) {
          throw new Error('Retry please');
        }
        return { done: true };
      },
      { embedded: true, autorun: true }
    );

    await queue.add(
      'test',
      { value: 1 },
      { attempts: 3, backoff: { type: 'fixed', delay: 100 } }
    );

    await Bun.sleep(1000);

    expect(attempts).toBe(3);
    // Fixed backoff should be roughly constant (allow some variance)
    for (const delay of delays) {
      expect(delay).toBeGreaterThanOrEqual(90);
      expect(delay).toBeLessThan(300);
    }

    await worker.close();
  });
});

describe('JobOptions - LIFO', () => {
  let queue: Queue<{ order: number }>;

  beforeEach(() => {
    queue = new Queue('lifo-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept lifo option', async () => {
    const job = await queue.add('test', { order: 1 }, { lifo: true });
    expect(job.id).toBeDefined();
    expect(job.opts.lifo).toBe(true);
  });

  test('lifo should default to false', async () => {
    const job = await queue.add('test', { order: 1 });
    expect(job.opts.lifo).toBeFalsy();
  });

  test('lifo jobs should be processed in reverse order', async () => {
    // Pause queue to add jobs without processing
    queue.pause();

    // Add jobs with LIFO
    await queue.add('test', { order: 1 }, { lifo: true });
    await queue.add('test', { order: 2 }, { lifo: true });
    await queue.add('test', { order: 3 }, { lifo: true });

    const processedOrder: number[] = [];

    const worker = new Worker(
      'lifo-test',
      async (job) => {
        processedOrder.push(job.data.order);
        return { processed: job.data.order };
      },
      { embedded: true, autorun: true }
    );

    // Resume and let worker process
    queue.resume();
    await Bun.sleep(500);

    // LIFO: last added should be processed first
    expect(processedOrder).toEqual([3, 2, 1]);

    await worker.close();
  });
});

describe('JobOptions - Extended Repeat', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('repeat-ext-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept repeat with startDate as Date', async () => {
    const startDate = new Date(Date.now() + 60000);
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, startDate } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with startDate as string', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, startDate: '2025-12-31T00:00:00Z' } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with startDate as number', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, startDate: Date.now() + 60000 } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with endDate', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, endDate: Date.now() + 3600000 } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with timezone', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { pattern: '0 9 * * *', tz: 'Europe/Rome' } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with immediately flag', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 5000, immediately: true } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with count', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, count: 5 } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with offset', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, offset: 500 } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept repeat with custom jobId', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { repeat: { every: 1000, jobId: 'my-repeat-job' } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept all repeat options together', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      {
        repeat: {
          every: 5000,
          limit: 10,
          startDate: Date.now(),
          endDate: Date.now() + 3600000,
          tz: 'UTC',
          immediately: true,
          count: 0,
          offset: 100,
          jobId: 'full-repeat-job',
        },
      }
    );
    expect(job.id).toBeDefined();
  });
});

describe('JobOptions - Deduplication', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('dedup-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept deduplication option', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { deduplication: { id: 'unique-task-1' } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept deduplication with ttl', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { deduplication: { id: 'unique-task-2', ttl: 60000 } }
    );
    expect(job.id).toBeDefined();
  });

  test('deduplication should prevent duplicate jobs', async () => {
    // Add first job
    const job1 = await queue.add(
      'test',
      { value: 1 },
      { deduplication: { id: 'same-task' } }
    );

    // Try to add duplicate
    const job2 = await queue.add(
      'test',
      { value: 2 },
      { deduplication: { id: 'same-task' } }
    );

    // Both should have the same ID (deduped)
    expect(job1.id).toBe(job2.id);
  });
});

describe('JobOptions - Debounce', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('debounce-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept debounce option', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { debounce: { id: 'debounce-1', ttl: 1000 } }
    );
    expect(job.id).toBeDefined();
  });

  test('should accept debounce with different ttl values', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { debounce: { id: 'debounce-2', ttl: 5000 } }
    );
    expect(job.id).toBeDefined();
  });
});

describe('JobOptions - Parent/Children', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('parent-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept failParentOnFailure option', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { failParentOnFailure: true }
    );
    expect(job.id).toBeDefined();
    expect(job.opts.failParentOnFailure).toBe(true);
  });

  test('failParentOnFailure should default to false', async () => {
    const job = await queue.add('test', { value: 1 });
    expect(job.opts.failParentOnFailure).toBeFalsy();
  });

  test('should accept removeDependencyOnFailure option', async () => {
    const job = await queue.add(
      'test',
      { value: 1 },
      { removeDependencyOnFailure: true }
    );
    expect(job.id).toBeDefined();
    expect(job.opts.removeDependencyOnFailure).toBe(true);
  });

  test('removeDependencyOnFailure should default to false', async () => {
    const job = await queue.add('test', { value: 1 });
    expect(job.opts.removeDependencyOnFailure).toBeFalsy();
  });

  test('should accept parent option', async () => {
    const parentJob = await queue.add('parent', { value: 0 });
    const childJob = await queue.add(
      'child',
      { value: 1 },
      { parent: { id: parentJob.id, queue: 'parent-test' } }
    );
    expect(childJob.id).toBeDefined();
    expect(childJob.opts.parent).toEqual({ id: parentJob.id, queue: 'parent-test' });
  });
});

describe('JobOptions - Stack/Logs/Size Limits', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('limits-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept stackTraceLimit option', async () => {
    const job = await queue.add('test', { value: 1 }, { stackTraceLimit: 5 });
    expect(job.id).toBeDefined();
    expect(job.opts.stackTraceLimit).toBe(5);
  });

  test('stackTraceLimit should default to 10', async () => {
    const job = await queue.add('test', { value: 1 });
    // Default is 10 in JOB_DEFAULTS
    expect(job.opts.stackTraceLimit).toBe(10);
  });

  test('should accept keepLogs option', async () => {
    const job = await queue.add('test', { value: 1 }, { keepLogs: 100 });
    expect(job.id).toBeDefined();
    expect(job.opts.keepLogs).toBe(100);
  });

  test('should accept sizeLimit option', async () => {
    const job = await queue.add('test', { value: 1 }, { sizeLimit: 1024 * 1024 });
    expect(job.id).toBeDefined();
    expect(job.opts.sizeLimit).toBe(1024 * 1024);
  });
});

describe('JobOptions - Extended RemoveOnComplete/RemoveOnFail', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('remove-ext-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept removeOnComplete as boolean (true)', async () => {
    const job = await queue.add('test', { value: 1 }, { removeOnComplete: true });
    expect(job.id).toBeDefined();
    expect(job.opts.removeOnComplete).toBe(true);
  });

  test('should accept removeOnComplete as boolean (false)', async () => {
    const job = await queue.add('test', { value: 1 }, { removeOnComplete: false });
    expect(job.id).toBeDefined();
    expect(job.opts.removeOnComplete).toBe(false);
  });

  test('removeOnComplete non-boolean is coerced (age/count retention not supported)', async () => {
    // The type is boolean-only (#90); a JS caller passing a number is coerced
    // to a boolean rather than enabling silent age-based retention.
    const job = await queue.add(
      'test',
      { value: 1 },
      { removeOnComplete: 3600000 as unknown as boolean }
    );
    const fetched = await queue.getJob(job.id);
    expect(typeof fetched!.opts.removeOnComplete).toBe('boolean');
  });

  test('should accept removeOnFail as boolean (true)', async () => {
    const job = await queue.add('test', { value: 1 }, { removeOnFail: true });
    expect(job.id).toBeDefined();
    expect(job.opts.removeOnFail).toBe(true);
  });

  test('should accept removeOnFail as boolean (false)', async () => {
    const job = await queue.add('test', { value: 1 }, { removeOnFail: false });
    expect(job.id).toBeDefined();
    expect(job.opts.removeOnFail).toBe(false);
  });
});

describe('JobOptions - addBulk with BullMQ v5 options', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('bulk-opts-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('addBulk should accept all BullMQ v5 options', async () => {
    const jobs = await queue.addBulk([
      {
        name: 'job1',
        data: { value: 1 },
        opts: {
          lifo: true,
          backoff: { type: 'exponential', delay: 1000 },
          stackTraceLimit: 5,
        },
      },
      {
        name: 'job2',
        data: { value: 2 },
        opts: {
          deduplication: { id: 'bulk-dedup', ttl: 5000 },
          failParentOnFailure: true,
        },
      },
      {
        name: 'job3',
        data: { value: 3 },
        opts: {
          repeat: {
            every: 1000,
            startDate: Date.now(),
            tz: 'UTC',
            immediately: true,
          },
        },
      },
    ]);

    expect(jobs).toHaveLength(3);
    expect(jobs[0].id).toBeDefined();
    expect(jobs[1].id).toBeDefined();
    expect(jobs[2].id).toBeDefined();
  });
});

describe('JobOptions - Combined options', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('combined-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  test('should accept all BullMQ v5 options together', async () => {
    const job = await queue.add(
      'comprehensive',
      { value: 42 },
      {
        // Standard options
        priority: 10,
        delay: 100,
        attempts: 5,
        timeout: 30000,
        jobId: 'comprehensive-job',

        // BullMQ v5 backoff
        backoff: { type: 'exponential', delay: 1000 },

        // BullMQ v5 behavior
        lifo: false,
        removeOnComplete: true,
        removeOnFail: false,

        // BullMQ v5 limits
        stackTraceLimit: 15,
        keepLogs: 50,
        sizeLimit: 1024 * 1024,

        // BullMQ v5 parent/children
        failParentOnFailure: false,
        removeDependencyOnFailure: false,

        // BullMQ v5 deduplication
        deduplication: { id: 'comprehensive-dedup', ttl: 60000 },
      }
    );

    expect(job.id).toBeDefined();
    expect(job.name).toBe('comprehensive');
    expect(job.data).toEqual({ value: 42 });
  });
});
