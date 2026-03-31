/**
 * DLQ (Dead Letter Queue) Patterns - Embedded Mode
 *
 * Tests covering core DLQ behaviors: failed jobs landing in DLQ,
 * different error types, retry from DLQ, purge, data preservation,
 * and accumulation of multiple failures.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('DLQ Patterns - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. failed job goes to DLQ', async () => {
    const queue = new Queue<{ value: number }>('dlq-pat-failed', { embedded: true });
    queue.obliterate();
    queue.setDlqConfig({ maxEntries: 1000 });

    const job = await queue.add('will-fail', { value: 42 }, { attempts: 1, backoff: 0 });

    const worker = new Worker(
      'dlq-pat-failed',
      async () => {
        throw new Error('intentional failure');
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for the job to exhaust its single attempt and land in DLQ
    for (let i = 0; i < 100; i++) {
      const entries = queue.getDlq();
      if (entries.length >= 1) break;
      await Bun.sleep(50);
    }

    await worker.close();

    const entries = queue.getDlq();
    expect(entries.length).toBeGreaterThanOrEqual(1);

    const entry = entries.find((e) => e.job.id === job.id);
    expect(entry).toBeDefined();
    expect(entry!.reason).toBe('max_attempts_exceeded');

    queue.close();
  }, 30000);

  test('2. DLQ with different error types', async () => {
    const queue = new Queue<{ errorType: string }>('dlq-pat-errors', { embedded: true });
    queue.obliterate();
    queue.setDlqConfig({ maxEntries: 1000 });

    const errorMessages = [
      'database connection timeout',
      'invalid input format',
      'rate limit exceeded',
    ];

    const jobIds: string[] = [];
    for (const msg of errorMessages) {
      const job = await queue.add('error-job', { errorType: msg }, { attempts: 1, backoff: 0 });
      jobIds.push(job.id);
    }

    const worker = new Worker(
      'dlq-pat-errors',
      async (job) => {
        const data = job.data as { errorType: string };
        throw new Error(data.errorType);
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 3 jobs to land in DLQ
    for (let i = 0; i < 100; i++) {
      const entries = queue.getDlq();
      if (entries.length >= 3) break;
      await Bun.sleep(50);
    }

    await worker.close();

    const entries = queue.getDlq();
    expect(entries.length).toBeGreaterThanOrEqual(3);

    // Verify each job is in DLQ with its respective error
    for (let i = 0; i < jobIds.length; i++) {
      const entry = entries.find((e) => e.job.id === jobIds[i]);
      expect(entry).toBeDefined();
      expect(entry!.error).toContain(errorMessages[i]);
    }

    queue.close();
  }, 30000);

  test('3. retry from DLQ', async () => {
    const queue = new Queue<{ key: string }>('dlq-pat-retry', { embedded: true });
    queue.obliterate();
    queue.setDlqConfig({ maxEntries: 1000 });

    const job = await queue.add('retry-me', { key: 'retry-test' }, { attempts: 1, backoff: 0 });

    // First worker: always fails
    const worker1 = new Worker(
      'dlq-pat-retry',
      async () => {
        throw new Error('fail on first pass');
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for job to land in DLQ
    for (let i = 0; i < 100; i++) {
      const entries = queue.getDlq();
      if (entries.length >= 1) break;
      await Bun.sleep(50);
    }

    await worker1.close();

    // Verify job is in DLQ
    const dlqBefore = queue.getDlq();
    const entryBefore = dlqBefore.find((e) => e.job.id === job.id);
    expect(entryBefore).toBeDefined();

    // Retry from DLQ
    const retried = queue.retryDlq();
    expect(retried).toBeGreaterThanOrEqual(1);

    // Second worker: succeeds
    let processedJobId: string | null = null;
    const worker2 = new Worker(
      'dlq-pat-retry',
      async (j) => {
        processedJobId = j.id;
        return { success: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for the retried job to be processed
    for (let i = 0; i < 100; i++) {
      if (processedJobId !== null) break;
      await Bun.sleep(50);
    }

    await worker2.close();

    // The retried job should have been processed
    expect(processedJobId).toBe(job.id);

    // DLQ should no longer contain the job
    const dlqAfter = queue.getDlq();
    const entryAfter = dlqAfter.find((e) => e.job.id === job.id);
    expect(entryAfter).toBeUndefined();

    queue.close();
  }, 30000);

  test('4. purge DLQ', async () => {
    const queue = new Queue<{ idx: number }>('dlq-pat-purge', { embedded: true });
    queue.obliterate();
    queue.setDlqConfig({ maxEntries: 1000 });

    // Push 5 jobs that will all fail
    for (let i = 0; i < 5; i++) {
      await queue.add('purge-job', { idx: i }, { attempts: 1, backoff: 0 });
    }

    const worker = new Worker(
      'dlq-pat-purge',
      async () => {
        throw new Error('always fails');
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 5 to land in DLQ
    for (let i = 0; i < 100; i++) {
      const entries = queue.getDlq();
      if (entries.length >= 5) break;
      await Bun.sleep(50);
    }

    await worker.close();

    const entriesBefore = queue.getDlq();
    expect(entriesBefore.length).toBeGreaterThanOrEqual(5);

    // Purge the DLQ
    const purged = queue.purgeDlq();
    expect(purged).toBeGreaterThanOrEqual(5);

    // DLQ should be empty
    const entriesAfter = queue.getDlq();
    expect(entriesAfter.length).toBe(0);

    queue.close();
  }, 30000);

  test('5. DLQ preserves job data', async () => {
    const queue = new Queue<{ user: string; action: string; amount: number }>(
      'dlq-pat-data',
      { embedded: true }
    );
    queue.obliterate();
    queue.setDlqConfig({ maxEntries: 1000 });

    const originalData = { user: 'alice', action: 'transfer', amount: 9999 };
    const job = await queue.add('data-job', originalData, { attempts: 1, backoff: 0 });

    const worker = new Worker(
      'dlq-pat-data',
      async () => {
        throw new Error('processing failed');
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for job to land in DLQ
    for (let i = 0; i < 100; i++) {
      const entries = queue.getDlq();
      if (entries.length >= 1) break;
      await Bun.sleep(50);
    }

    await worker.close();

    const entries = queue.getDlq();
    const entry = entries.find((e) => e.job.id === job.id);
    expect(entry).toBeDefined();

    // Verify the original job data is preserved in the DLQ entry
    const data = entry!.job.data as { user: string; action: string; amount: number };
    expect(data.user).toBe('alice');
    expect(data.action).toBe('transfer');
    expect(data.amount).toBe(9999);

    queue.close();
  }, 30000);

  test('6. multiple failures accumulate in DLQ', async () => {
    const queue = new Queue<{ seq: number }>('dlq-pat-accumulate', { embedded: true });
    queue.obliterate();
    queue.setDlqConfig({ maxEntries: 1000 });

    const jobIds: string[] = [];
    for (let i = 0; i < 10; i++) {
      const job = await queue.add(`fail-${i}`, { seq: i }, { attempts: 1, backoff: 0 });
      jobIds.push(job.id);
    }

    const worker = new Worker(
      'dlq-pat-accumulate',
      async () => {
        throw new Error('sequential failure');
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 10 to land in DLQ
    for (let i = 0; i < 100; i++) {
      const entries = queue.getDlq();
      if (entries.length >= 10) break;
      await Bun.sleep(50);
    }

    await worker.close();

    const entries = queue.getDlq();
    expect(entries.length).toBeGreaterThanOrEqual(10);

    // Verify all 10 job IDs are present in the DLQ
    for (const id of jobIds) {
      const entry = entries.find((e) => e.job.id === id);
      expect(entry).toBeDefined();
    }

    queue.close();
  }, 30000);
});
