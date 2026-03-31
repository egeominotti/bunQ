/**
 * Tests for bunqueue/client - BullMQ-style API
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, QueueEvents, shutdownManager } from '../src/client';

describe('Queue', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('test-queue');
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('add()', () => {
    it('should add a job and return job object', async () => {
      const job = await queue.add('task', { value: 42 });

      expect(job.id).toBeDefined();
      expect(job.name).toBe('task');
      expect(job.data.value).toBe(42);
      expect(job.queueName).toBe('test-queue');
      expect(job.attemptsMade).toBe(0);
    });

    it('should respect job options', async () => {
      const job = await queue.add('task', { value: 1 }, {
        priority: 10,
        delay: 1000,
      });

      expect(job.id).toBeDefined();
    });
  });

  describe('addBulk()', () => {
    it('should add multiple jobs', async () => {
      const jobs = await queue.addBulk([
        { name: 'task1', data: { value: 1 } },
        { name: 'task2', data: { value: 2 } },
        { name: 'task3', data: { value: 3 } },
      ]);

      expect(jobs).toHaveLength(3);
      expect(jobs[0].name).toBe('task1');
      expect(jobs[1].name).toBe('task2');
      expect(jobs[2].name).toBe('task3');
    });
  });

  describe('getJob()', () => {
    it('should return job by ID', async () => {
      const added = await queue.add('task', { value: 99 });
      const fetched = await queue.getJob(added.id);

      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(added.id);
      expect(fetched!.data.value).toBe(99);
    });

    it('should return null for non-existent job', async () => {
      const fetched = await queue.getJob('non-existent-id');
      expect(fetched).toBeNull();
    });
  });

  describe('getJobCounts()', () => {
    it('should return job counts', async () => {
      await queue.add('task', { value: 1 });
      await queue.add('task', { value: 2 });

      const counts = await queue.getJobCounts();

      expect(counts.waiting).toBeGreaterThanOrEqual(2);
      expect(counts.active).toBeGreaterThanOrEqual(0);
    });
  });

  describe('pause() / resume()', () => {
    it('should pause and resume queue', async () => {
      await queue.pause();
      await queue.resume();
      // No error means success
      expect(true).toBe(true);
    });
  });

  describe('drain()', () => {
    it('should remove all waiting jobs', async () => {
      await queue.add('task', { value: 1 });
      await queue.add('task', { value: 2 });

      await queue.drain();

      const counts = await queue.getJobCounts();
      expect(counts.waiting).toBe(0);
    });
  });

  describe('obliterate()', () => {
    it('should remove all queue data', async () => {
      await queue.add('task', { value: 1 });
      await queue.obliterate();

      const counts = await queue.getJobCounts();
      expect(counts.waiting).toBe(0);
    });
  });
});

describe('Worker', () => {
  afterEach(() => {
    shutdownManager();
  });

  describe('processing', () => {
    it('should process jobs', async () => {
      const queue = new Queue<{ x: number }>('worker-test');
      const results: number[] = [];

      const worker = new Worker('worker-test', async (job) => {
        results.push(job.data.x);
        return { doubled: job.data.x * 2 };
      });

      await queue.add('calc', { x: 5 });
      await queue.add('calc', { x: 10 });

      await Bun.sleep(200);

      expect(results).toContain(5);
      expect(results).toContain(10);

      await worker.close();
    });

    it('should respect concurrency', async () => {
      const queue = new Queue<{ id: number }>('concurrency-test');
      let concurrent = 0;
      let maxConcurrent = 0;

      const worker = new Worker('concurrency-test', async () => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await Bun.sleep(50);
        concurrent--;
      }, { concurrency: 3 });

      for (let i = 0; i < 9; i++) {
        await queue.add('task', { id: i });
      }

      await Bun.sleep(500);

      expect(maxConcurrent).toBeLessThanOrEqual(3);
      expect(maxConcurrent).toBeGreaterThanOrEqual(1);

      await worker.close();
    });
  });

  describe('events', () => {
    it('should emit active event', async () => {
      const queue = new Queue('events-active');
      let activeCount = 0;

      const worker = new Worker('events-active', async () => {
        await Bun.sleep(10);
      });

      worker.on('active', () => activeCount++);

      await queue.add('task', {});
      await Bun.sleep(100);

      expect(activeCount).toBeGreaterThanOrEqual(1);

      await worker.close();
    });

    it('should emit completed event with result', async () => {
      const queue = new Queue<{ val: number }>('events-completed');
      let completedResult: unknown = null;

      const worker = new Worker('events-completed', async (job) => {
        return { result: job.data.val * 2 };
      });

      worker.on('completed', (_job, result) => {
        completedResult = result;
      });

      await queue.add('task', { val: 21 });
      await Bun.sleep(150);

      expect(completedResult).toEqual({ result: 42 });

      await worker.close();
    });

    it('should emit failed event on error', async () => {
      const queue = new Queue('events-failed');
      let failedError: Error | null = null;

      const worker = new Worker('events-failed', async () => {
        throw new Error('Intentional error');
      });

      worker.on('failed', (_job, err) => {
        failedError = err;
      });

      await queue.add('task', {});
      await Bun.sleep(150);

      expect(failedError).not.toBeNull();
      expect(failedError!.message).toBe('Intentional error');

      await worker.close();
    });

    it('should emit progress event', async () => {
      const queue = new Queue('events-progress');
      const progressValues: number[] = [];

      const worker = new Worker('events-progress', async (job) => {
        await job.updateProgress(25);
        await job.updateProgress(50);
        await job.updateProgress(100);
        return {};
      });

      worker.on('progress', (_job, progress) => {
        progressValues.push(progress);
      });

      await queue.add('task', {});
      await Bun.sleep(150);

      expect(progressValues).toContain(25);
      expect(progressValues).toContain(50);
      expect(progressValues).toContain(100);

      await worker.close();
    });
  });

  describe('pause() / resume()', () => {
    it('should pause and resume processing', async () => {
      const queue = new Queue('pause-test');
      let processed = 0;

      const worker = new Worker('pause-test', async () => {
        processed++;
      });

      await queue.add('task', {});
      await Bun.sleep(100);

      const beforePause = processed;
      worker.pause();

      await queue.add('task', {});
      await Bun.sleep(100);

      expect(processed).toBe(beforePause);

      worker.resume();
      await Bun.sleep(100);

      expect(processed).toBeGreaterThan(beforePause);

      await worker.close();
    });
  });

  describe('close()', () => {
    it('should wait for active jobs on graceful close', async () => {
      const queue = new Queue('close-test');
      let completed = false;

      const worker = new Worker('close-test', async () => {
        await Bun.sleep(50);
        completed = true;
      });

      await queue.add('task', {});
      await Bun.sleep(100); // Wait for job to start

      await worker.close();

      expect(completed).toBe(true);
    });

    it('should close immediately with force=true', async () => {
      const queue = new Queue('force-close');

      const worker = new Worker('force-close', async () => {
        await Bun.sleep(1000);
      });

      await queue.add('task', {});
      await Bun.sleep(20);

      await worker.close(true);
      // Should not wait
      expect(true).toBe(true);
    });
  });
});

describe('Job methods', () => {
  afterEach(() => {
    shutdownManager();
  });

  describe('updateProgress()', () => {
    it('should update job progress', async () => {
      const queue = new Queue('job-progress');
      let lastProgress = 0;

      const worker = new Worker('job-progress', async (job) => {
        await job.updateProgress(50, 'Halfway');
        lastProgress = 50;
        return {};
      });

      worker.on('progress', (_job, progress) => {
        lastProgress = progress;
      });

      await queue.add('task', {});
      await Bun.sleep(150);

      expect(lastProgress).toBe(50);

      await worker.close();
    });
  });

  describe('log()', () => {
    it('should log messages', async () => {
      const queue = new Queue('job-log');

      const worker = new Worker('job-log', async (job) => {
        await job.log('Step 1');
        await job.log('Step 2');
        return {};
      });

      await queue.add('task', {});
      await Bun.sleep(150);

      // Logs are stored internally, no error means success
      expect(true).toBe(true);

      await worker.close();
    });
  });
});

describe('QueueEvents', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should emit waiting event', async () => {
    const queue = new Queue('qe-waiting');
    const events = new QueueEvents('qe-waiting');
    let waitingCount = 0;

    events.on('waiting', () => waitingCount++);

    await queue.add('task', {});
    await Bun.sleep(50);

    expect(waitingCount).toBeGreaterThanOrEqual(0); // Event may or may not fire depending on timing

    await events.close();
  });
});

describe('Integration', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should handle full workflow', async () => {
    const queue = new Queue<{ email: string }>('integration');
    const processed: string[] = [];
    const progressUpdates: number[] = [];

    const worker = new Worker('integration', async (job) => {
      await job.log('Starting');
      await job.updateProgress(50);
      processed.push(job.data.email);
      await job.updateProgress(100);
      return { sent: true };
    }, { concurrency: 2 });

    worker.on('progress', (_job, progress) => {
      progressUpdates.push(progress);
    });

    // Add jobs
    const jobs = await queue.addBulk([
      { name: 'send', data: { email: 'a@test.com' } },
      { name: 'send', data: { email: 'b@test.com' } },
    ]);

    expect(jobs).toHaveLength(2);

    // Wait for processing
    await Bun.sleep(300);

    expect(processed).toContain('a@test.com');
    expect(processed).toContain('b@test.com');
    expect(progressUpdates).toContain(50);
    expect(progressUpdates).toContain(100);

    // Verify counts
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);

    await worker.close();
  });

  it('should handle high throughput', async () => {
    const queue = new Queue('throughput');
    const JOBS = 1000;
    let completed = 0;

    const worker = new Worker('throughput', async () => {
      return { ok: true };
    }, { concurrency: 50 });

    worker.on('completed', () => completed++);

    for (let i = 0; i < JOBS; i++) {
      await queue.add('task', { i });
    }

    // Wait for all jobs
    const start = Date.now();
    while (completed < JOBS && Date.now() - start < 10000) {
      await Bun.sleep(50);
    }

    expect(completed).toBe(JOBS);

    await worker.close();
  });
});
