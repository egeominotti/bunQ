/**
 * Documentation Quickstart Verification Tests
 * Verifies all code examples from the quickstart guide work correctly
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

const sleep = (ms: number) => Bun.sleep(ms);

describe('Quickstart Documentation Examples', () => {
  beforeEach(() => {
    const manager = getSharedManager();
    manager.drain('emails');
    manager.drain('tasks');
  });

  describe('Create a Queue', () => {
    test('typed queue creation works', () => {
      interface EmailJob {
        to: string;
        subject: string;
        body: string;
      }

      const emailQueue = new Queue<EmailJob>('emails', { embedded: true });

      expect(emailQueue).toBeDefined();
      expect(emailQueue.name).toBe('emails');

      emailQueue.close();
    });
  });

  describe('Add Jobs', () => {
    test('add a single job', async () => {
      interface EmailJob {
        to: string;
        subject: string;
        body: string;
      }

      const emailQueue = new Queue<EmailJob>('emails', { embedded: true });

      const job = await emailQueue.add('send-email', {
        to: 'user@example.com',
        subject: 'Welcome!',
        body: 'Thanks for signing up.',
      });

      expect(job.id).toBeDefined();
      expect(typeof job.id).toBe('string');

      emailQueue.drain();
      emailQueue.close();
    });

    test('add with options (priority, delay, attempts, backoff)', async () => {
      interface EmailJob {
        to: string;
        subject: string;
        body: string;
      }

      const emailQueue = new Queue<EmailJob>('emails', { embedded: true });
      const data = { to: 'test@test.com', subject: 'Test', body: 'Body' };

      const job = await emailQueue.add('send-email', data, {
        priority: 10,
        delay: 5000,
        attempts: 3,
        backoff: 1000,
      });

      // Job is created successfully with options
      expect(job.id).toBeDefined();
      expect(job.name).toBe('send-email');
      expect(job.data.to).toBe('test@test.com');

      // Verify we can retrieve the job
      const retrieved = await emailQueue.getJob(job.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.id).toBe(job.id);

      emailQueue.drain();
      emailQueue.close();
    });

    test('addBulk works', async () => {
      interface EmailJob {
        to: string;
        subject: string;
        body: string;
      }

      const emailQueue = new Queue<EmailJob>('emails', { embedded: true });

      const jobs = await emailQueue.addBulk([
        { name: 'send-email', data: { to: 'a@test.com', subject: 'Hi', body: '...' } },
        { name: 'send-email', data: { to: 'b@test.com', subject: 'Hi', body: '...' } },
      ]);

      expect(jobs.length).toBe(2);
      expect(jobs[0].id).toBeDefined();
      expect(jobs[1].id).toBeDefined();

      emailQueue.drain();
      emailQueue.close();
    });
  });

  describe('Create a Worker', () => {
    test('worker with embedded: true processes jobs', async () => {
      interface EmailJob {
        to: string;
        subject: string;
        body: string;
      }

      const emailQueue = new Queue<EmailJob>('emails', { embedded: true });
      const processed: string[] = [];

      const worker = new Worker<EmailJob>(
        'emails',
        async (job) => {
          processed.push(job.data.to);
          await job.updateProgress(50, 'Sending email...');
          await job.log('Email sent successfully');
          return { sent: true, timestamp: Date.now() };
        },
        {
          embedded: true,
          concurrency: 5,
        }
      );

      await emailQueue.add('send-email', {
        to: 'user@example.com',
        subject: 'Welcome!',
        body: 'Thanks for signing up.',
      });

      await sleep(300);

      expect(processed).toContain('user@example.com');

      await worker.close();
      emailQueue.close();
    });
  });

  describe('Handle Events', () => {
    test('completed event fires', async () => {
      interface EmailJob {
        to: string;
        subject: string;
      }

      const queue = new Queue<EmailJob>('emails', { embedded: true });
      const completedJobs: string[] = [];

      const worker = new Worker<EmailJob, { sent: boolean }>(
        'emails',
        async (job) => {
          return { sent: true };
        },
        { embedded: true }
      );

      worker.on('completed', (job, result) => {
        completedJobs.push(String(job.id));
        expect(result).toEqual({ sent: true });
      });

      await queue.add('welcome', { to: 'test@test.com', subject: 'Hi' });

      await sleep(300);

      expect(completedJobs.length).toBe(1);

      await worker.close();
      queue.close();
    });

    test('failed event fires', async () => {
      interface EmailJob {
        to: string;
        subject: string;
      }

      const queue = new Queue<EmailJob>('emails', { embedded: true });
      const failedJobs: string[] = [];

      const worker = new Worker<EmailJob>(
        'emails',
        async () => {
          throw new Error('Simulated failure');
        },
        { embedded: true }
      );

      worker.on('failed', (job, error) => {
        failedJobs.push(String(job.id));
        expect(error.message).toContain('Simulated failure');
      });

      await queue.add(
        'fail-job',
        { to: 'test@test.com', subject: 'Fail' },
        { attempts: 1 } // Only 1 attempt so it fails immediately
      );

      await sleep(300);

      expect(failedJobs.length).toBe(1);

      await worker.close();
      queue.close();
    });

    test('progress event fires', async () => {
      interface EmailJob {
        to: string;
        subject: string;
      }

      const queue = new Queue<EmailJob>('emails', { embedded: true });
      const progressUpdates: number[] = [];

      const worker = new Worker<EmailJob>(
        'emails',
        async (job) => {
          await job.updateProgress(50);
          await job.updateProgress(100);
          return { done: true };
        },
        { embedded: true }
      );

      worker.on('progress', (job, progress) => {
        progressUpdates.push(progress);
      });

      await queue.add('progress-job', { to: 'test@test.com', subject: 'Progress' });

      await sleep(300);

      expect(progressUpdates).toContain(50);
      expect(progressUpdates).toContain(100);

      await worker.close();
      queue.close();
    });

    test('active event fires', async () => {
      interface EmailJob {
        to: string;
        subject: string;
      }

      const queue = new Queue<EmailJob>('emails', { embedded: true });
      const activeJobs: string[] = [];

      const worker = new Worker<EmailJob>(
        'emails',
        async (job) => {
          return { done: true };
        },
        { embedded: true }
      );

      worker.on('active', (job) => {
        activeJobs.push(String(job.id));
      });

      await queue.add('active-job', { to: 'test@test.com', subject: 'Active' });

      await sleep(300);

      expect(activeJobs.length).toBe(1);

      await worker.close();
      queue.close();
    });
  });

  describe('Full Example', () => {
    test('complete producer-consumer pattern', async () => {
      interface EmailJob {
        to: string;
        subject: string;
      }

      // Producer - must have embedded: true
      const queue = new Queue<EmailJob>('emails', { embedded: true });

      const completed: string[] = [];

      // Consumer - must have embedded: true
      const worker = new Worker<EmailJob>(
        'emails',
        async (job) => {
          await job.updateProgress(100);
          return { sent: true };
        },
        { embedded: true, concurrency: 3 }
      );

      worker.on('completed', (job) => {
        completed.push(String(job.id));
      });

      // Add some jobs
      await queue.add('welcome', { to: 'new@user.com', subject: 'Welcome!' });
      await queue.add('newsletter', { to: 'sub@user.com', subject: 'News' });

      await sleep(500);

      expect(completed.length).toBe(2);

      await worker.close();
      queue.close();
    });
  });

  describe('Mixed Mode Error (Common Mistake)', () => {
    test('both embedded works correctly', async () => {
      const queue = new Queue('tasks', { embedded: true });
      const worker = new Worker(
        'tasks',
        async () => {
          return { ok: true };
        },
        { embedded: true }
      );

      const job = await queue.add('test', { foo: 'bar' });
      expect(job.id).toBeDefined();

      await sleep(200);

      await worker.close();
      queue.close();
    });

    // Note: We can't easily test the TCP mode without a running server,
    // but we verify the embedded mode works correctly which is the main point
  });

  describe('Persistence (DATA_PATH)', () => {
    test('works with in-memory mode (no DATA_PATH)', async () => {
      // This test verifies the default in-memory behavior
      const queue = new Queue('tasks', { embedded: true });
      const worker = new Worker(
        'tasks',
        async () => {
          return { processed: true };
        },
        { embedded: true }
      );

      const job = await queue.add('memory-job', { data: 'test' });
      expect(job.id).toBeDefined();

      await sleep(200);

      await worker.close();
      queue.close();
    });
  });
});

describe('Common Mistake Scenarios', () => {
  beforeEach(() => {
    const manager = getSharedManager();
    manager.drain('test-queue');
  });

  test('Queue.add returns immediately in embedded mode (no timeout)', async () => {
    const queue = new Queue('test-queue', { embedded: true });

    // Drain first to ensure clean state
    queue.drain();

    const countsBefore = queue.getJobCounts();
    const startTime = Date.now();

    // Add 10 jobs - should be fast
    for (let i = 0; i < 10; i++) {
      await queue.add('job', { index: i });
    }

    const elapsed = Date.now() - startTime;

    // Should complete in under 500ms (not timeout at 30s)
    expect(elapsed).toBeLessThan(500);

    const countsAfter = queue.getJobCounts();
    expect(countsAfter.waiting).toBe(countsBefore.waiting + 10);

    queue.drain();
    queue.close();
  });

  test('Worker processes jobs when both have embedded: true', async () => {
    const queue = new Queue<{ batchId: string }>('test-queue', { embedded: true });
    const processed: string[] = [];

    const worker = new Worker<{ batchId: string }>(
      'test-queue',
      async (job) => {
        processed.push(job.data.batchId);
        return { success: true };
      },
      { embedded: true }
    );

    // Simulate the user's recoverPendingJobs pattern
    const batches = [
      { id: 'batch-1', expiresAt: new Date(Date.now() + 100) },
      { id: 'batch-2', expiresAt: new Date(Date.now() + 100) },
      { id: 'batch-3', expiresAt: new Date(Date.now() + 100) },
    ];

    for (const batch of batches) {
      await queue.add(
        'rollover',
        { batchId: batch.id },
        {
          delay: Math.max(0, batch.expiresAt.getTime() - Date.now()),
          jobId: `rollover-${batch.id}`,
        }
      );
    }

    // Wait for delayed jobs to process
    await sleep(400);

    expect(processed.length).toBe(3);
    expect(processed).toContain('batch-1');
    expect(processed).toContain('batch-2');
    expect(processed).toContain('batch-3');

    await worker.close();
    queue.close();
  });
});
