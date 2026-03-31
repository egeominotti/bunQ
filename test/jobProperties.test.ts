/**
 * Job Properties Tests - BullMQ v5 Compatible Features
 * Tests for Job properties and methods added in Phase 4
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('Job - BullMQ v5 Properties', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('job-props-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('Core Properties', () => {
    test('should have delay property', async () => {
      const job = await queue.add('test', { value: 42 }, { delay: 1000 });
      expect(job.delay).toBe(1000);
    });

    test('should have delay = 0 for immediate jobs', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.delay).toBe(0);
    });

    test('should have priority property', async () => {
      const job = await queue.add('test', { value: 42 }, { priority: 5 });
      expect(job.priority).toBe(5);
    });

    test('should have stalledCounter property', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.stalledCounter).toBe(0);
    });

    test('should have attemptsStarted property', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.attemptsStarted).toBe(0);
    });

    test('should have opts property with original options', async () => {
      const job = await queue.add('test', { value: 42 }, { priority: 3, delay: 500 });
      expect(job.opts).toBeDefined();
      expect(job.opts.priority).toBe(3);
      expect(job.opts.delay).toBe(500);
    });

    test('should have deduplicationId when jobId is set', async () => {
      const job = await queue.add('test', { value: 42 }, { jobId: 'custom-id-123' });
      expect(job.deduplicationId).toBe('custom-id-123');
    });

    test('should have stacktrace as null by default', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.stacktrace).toBeNull();
    });
  });

  describe('Timing Properties', () => {
    test('should have processedOn undefined before processing', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.processedOn).toBeUndefined();
    });

    test('should have finishedOn undefined before completion', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.finishedOn).toBeUndefined();
    });
  });

  describe('Reference Properties', () => {
    test('should have token undefined when not processing', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.token).toBeUndefined();
    });

    test('should have processedBy undefined when not processing', async () => {
      const job = await queue.add('test', { value: 42 });
      expect(job.processedBy).toBeUndefined();
    });
  });
});

describe('Job - State Check Methods', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('job-state-test', { embedded: true });
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

  describe('isWaiting()', () => {
    test('should return true for waiting job', async () => {
      const job = await queue.add('test', { value: 42 });
      const isWaiting = await job.isWaiting();
      expect(isWaiting).toBe(true);
    });
  });

  describe('isDelayed()', () => {
    test('should return true for delayed job', async () => {
      const job = await queue.add('test', { value: 42 }, { delay: 60000 });
      const isDelayed = await job.isDelayed();
      expect(isDelayed).toBe(true);
    });

    test('should return false for immediate job', async () => {
      const job = await queue.add('test', { value: 42 });
      const isDelayed = await job.isDelayed();
      expect(isDelayed).toBe(false);
    });
  });

  describe('isCompleted()', () => {
    test('should return false for waiting job', async () => {
      const job = await queue.add('test', { value: 42 });
      const isCompleted = await job.isCompleted();
      expect(isCompleted).toBe(false);
    });

    test('should return true after job completes', async () => {
      let processedJob: typeof job | null = null;

      worker = new Worker(
        'job-state-test',
        async (j) => {
          processedJob = j;
          return j.data.value * 2;
        },
        { embedded: true, autorun: true }
      );

      const job = await queue.add('test', { value: 21 });

      // Wait for processing
      await Bun.sleep(500);

      // Check state via queue.getJob (fresh fetch)
      const freshJob = await queue.getJob(job.id);
      if (freshJob) {
        const isCompleted = await freshJob.isCompleted();
        expect(isCompleted).toBe(true);
      }
    });
  });

  describe('isFailed()', () => {
    test('should return false for waiting job', async () => {
      const job = await queue.add('test', { value: 42 });
      const isFailed = await job.isFailed();
      expect(isFailed).toBe(false);
    });
  });

  describe('isActive()', () => {
    test('should return false for waiting job', async () => {
      const job = await queue.add('test', { value: 42 });
      const isActive = await job.isActive();
      expect(isActive).toBe(false);
    });
  });

  describe('isWaitingChildren()', () => {
    test('should return false for job without children', async () => {
      const job = await queue.add('test', { value: 42 });
      const isWaitingChildren = await job.isWaitingChildren();
      expect(isWaitingChildren).toBe(false);
    });
  });
});

describe('Job - Mutation Methods', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('job-mutation-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('promote()', () => {
    test('should promote delayed job to waiting', async () => {
      const job = await queue.add('test', { value: 42 }, { delay: 60000 });

      // Verify it's delayed
      let isDelayed = await job.isDelayed();
      expect(isDelayed).toBe(true);

      // Promote it
      await job.promote();

      // Verify it's now waiting (refetch to get updated state)
      const freshJob = await queue.getJob(job.id);
      if (freshJob) {
        isDelayed = await freshJob.isDelayed();
        expect(isDelayed).toBe(false);
      }
    });
  });

  describe('changeDelay()', () => {
    test('should not throw when changing delay', async () => {
      const job = await queue.add('test', { value: 42 });
      await expect(job.changeDelay(5000)).resolves.toBeUndefined();
    });
  });

  describe('changePriority()', () => {
    test('should not throw when changing priority', async () => {
      const job = await queue.add('test', { value: 42 }, { priority: 1 });
      await expect(job.changePriority({ priority: 10 })).resolves.toBeUndefined();
    });
  });

  describe('clearLogs()', () => {
    test('should clear job logs', async () => {
      const job = await queue.add('test', { value: 42 });

      // Add some logs using queue method (job.log is no-op for queue.add jobs)
      await queue.addJobLog(job.id, 'First log');
      await queue.addJobLog(job.id, 'Second log');
      await queue.addJobLog(job.id, 'Third log');

      // Verify logs were added
      let logs = await queue.getJobLogs(job.id);
      expect(logs.count).toBe(3);

      // Clear logs
      await job.clearLogs();

      // Verify logs are cleared
      logs = await queue.getJobLogs(job.id);
      expect(logs.count).toBe(0);
    });

    test('should keep specified number of logs', async () => {
      const job = await queue.add('test', { value: 42 });

      // Add some logs using queue method
      await queue.addJobLog(job.id, 'First log');
      await queue.addJobLog(job.id, 'Second log');
      await queue.addJobLog(job.id, 'Third log');

      // Verify logs were added
      let logs = await queue.getJobLogs(job.id);
      expect(logs.count).toBe(3);

      // Clear but keep 2
      await job.clearLogs(2);

      // Verify only 2 logs remain
      logs = await queue.getJobLogs(job.id);
      expect(logs.count).toBe(2);
    });
  });
});

describe('Job - Dependency Methods', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('job-deps-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('getDependencies()', () => {
    test('should return empty dependencies for job without children', async () => {
      const job = await queue.add('test', { value: 42 });
      const deps = await job.getDependencies();

      expect(deps.processed).toEqual({});
      expect(deps.unprocessed).toEqual([]);
    });
  });

  describe('getDependenciesCount()', () => {
    test('should return zero counts for job without children', async () => {
      const job = await queue.add('test', { value: 42 });
      const counts = await job.getDependenciesCount();

      expect(counts.processed).toBe(0);
      expect(counts.unprocessed).toBe(0);
    });
  });
});

describe('Job - Serialization Methods', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('job-serial-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('toJSON()', () => {
    test('should return job as JSON object', async () => {
      const job = await queue.add('test-job', { value: 42 }, { priority: 5, delay: 1000 });
      const json = job.toJSON();

      expect(json.id).toBe(job.id);
      expect(json.name).toBe('test-job');
      expect(json.data).toEqual({ value: 42 });
      expect(json.opts.priority).toBe(5);
      expect(json.opts.delay).toBe(1000);
      expect(json.delay).toBe(1000);
      expect(json.timestamp).toBe(job.timestamp);
      expect(json.attemptsMade).toBe(0);
      expect(json.progress).toBe(0);
      expect(json.stacktrace).toBeNull();
      expect(json.queueQualifiedName).toBe('bull:job-serial-test');
    });
  });

  describe('asJSON()', () => {
    test('should return job as raw JSON with stringified values', async () => {
      const job = await queue.add('test-job', { value: 42 }, { priority: 5 });
      const raw = job.asJSON();

      expect(raw.id).toBe(job.id);
      expect(raw.name).toBe('test-job');
      expect(raw.data).toBe(JSON.stringify({ value: 42 }));
      expect(raw.opts).toBe(JSON.stringify(job.opts));
      expect(raw.progress).toBe(JSON.stringify(0));
      expect(raw.timestamp).toBe(String(job.timestamp));
      expect(raw.attemptsMade).toBe('0');
    });
  });
});

describe('Job - Extended Lock', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('job-lock-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('extendLock()', () => {
    test('should return 0 when no lock exists', async () => {
      const job = await queue.add('test', { value: 42 });
      const result = await job.extendLock('fake-token', 30000);
      expect(result).toBe(0);
    });
  });
});
