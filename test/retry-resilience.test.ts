/**
 * Retry Resilience Tests (Embedded Mode)
 * Tests retry behavior, backoff timing, concurrent retries, and DLQ integration
 * using the client SDK with embedded mode.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Retry Resilience - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('fail then succeed - job fails twice, succeeds on 3rd attempt', async () => {
    const queue = new Queue<{ value: number }>('retry-res-fail-succeed', { embedded: true });
    queue.obliterate();

    const attemptCounts = new Map<string, number>();
    let completedJobId: string | null = null;

    await queue.add('task', { value: 1 }, { attempts: 3, backoff: 200 });

    const worker = new Worker<{ value: number }>(
      'retry-res-fail-succeed',
      async (job) => {
        const count = (attemptCounts.get(job.id) ?? 0) + 1;
        attemptCounts.set(job.id, count);
        if (count < 3) {
          throw new Error(`Attempt ${count} failed`);
        }
        return { success: true };
      },
      { concurrency: 1 }
    );

    worker.on('completed', (job) => {
      completedJobId = job.id;
    });

    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (completedJobId) break;
    }

    await worker.close();

    expect(completedJobId).not.toBeNull();
    const count = attemptCounts.get(completedJobId!);
    expect(count).toBe(3);

    queue.close();
  }, 30000);

  test('exhaust all retries - job always fails', async () => {
    const queue = new Queue<{ value: number }>('retry-res-exhaust', { embedded: true });
    queue.obliterate();

    let processCount = 0;
    const failedJobAttempts: number[] = [];

    await queue.add('task', { value: 1 }, { attempts: 3, backoff: 100 });

    const worker = new Worker<{ value: number }>(
      'retry-res-exhaust',
      async () => {
        processCount++;
        throw new Error('Always fails');
      },
      { concurrency: 1 }
    );

    worker.on('failed', (job) => {
      failedJobAttempts.push(job.attemptsMade);
    });

    // Wait long enough for all 3 attempts with exponential backoff
    // Attempt 1 immediate, backoff ~200ms, attempt 2, backoff ~400ms, attempt 3
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (processCount >= 3) break;
    }

    await worker.close();

    // The 'failed' event fires on every attempt
    expect(processCount).toBe(3);
    expect(failedJobAttempts.length).toBe(3);
    // attemptsMade is set from job.attempts at pull time (before processing),
    // so the values are 0, 1, 2 (the server increments after fail)
    expect(failedJobAttempts.sort((a, b) => a - b)).toEqual([0, 1, 2]);

    queue.close();
  }, 30000);

  test('exponential backoff timing - backoff grows between failures', async () => {
    const queue = new Queue<{ value: number }>('retry-res-exp-backoff', { embedded: true });
    queue.obliterate();

    const timestamps: number[] = [];
    let completed = false;

    await queue.add('task', { value: 1 }, { attempts: 4, backoff: 200 });

    const worker = new Worker<{ value: number }>(
      'retry-res-exp-backoff',
      async () => {
        timestamps.push(Date.now());
        if (timestamps.length < 4) {
          throw new Error(`Attempt ${timestamps.length} failed`);
        }
        return { done: true };
      },
      { concurrency: 1 }
    );

    worker.on('completed', () => {
      completed = true;
    });

    for (let i = 0; i < 80; i++) {
      await Bun.sleep(100);
      if (completed) break;
    }

    await worker.close();

    expect(timestamps.length).toBeGreaterThanOrEqual(4);

    const gaps: number[] = [];
    for (let i = 1; i < timestamps.length; i++) {
      gaps.push(timestamps[i] - timestamps[i - 1]);
    }
    // Exponential backoff with jitter (base * 2^attempts * [0.5..1.5]).
    // Due to jitter, adjacent gaps may not be strictly ordered, but the
    // 3rd gap (base*2^3) should reliably exceed the 1st gap (base*2^1).
    expect(gaps[2]).toBeGreaterThan(gaps[0]);

    queue.close();
  }, 30000);

  test('multiple jobs with different retry configs', async () => {
    const queue = new Queue<{ id: number }>('retry-res-multi', { embedded: true });
    queue.obliterate();

    const attemptCounts = new Map<string, number>();
    const completedIds: string[] = [];

    // Job 1: attempts:1 -> fails on 1st attempt, no retry -> DLQ
    const job1 = await queue.add('task-1a', { id: 1 }, { attempts: 1, backoff: 100 });
    // Job 2: attempts:3 -> fails on 1st, succeeds on 2nd
    const job2 = await queue.add('task-3a', { id: 2 }, { attempts: 3, backoff: 100 });
    // Job 3: attempts:5 -> fails on 1st, succeeds on 2nd
    const job3 = await queue.add('task-5a', { id: 3 }, { attempts: 5, backoff: 100 });

    const worker = new Worker<{ id: number }>(
      'retry-res-multi',
      async (job) => {
        const count = (attemptCounts.get(job.id) ?? 0) + 1;
        attemptCounts.set(job.id, count);
        if (count === 1) {
          throw new Error(`First attempt fails for job ${job.data.id}`);
        }
        return { success: true };
      },
      { concurrency: 1 }
    );

    worker.on('completed', (job) => {
      completedIds.push(job.id);
    });

    // Wait for job2 and job3 to retry and complete
    for (let i = 0; i < 60; i++) {
      await Bun.sleep(100);
      if (completedIds.length >= 2) break;
    }

    await worker.close();

    // Job 1 (attempts:1) should NOT be completed
    expect(completedIds).not.toContain(job1.id);

    // Job 2 and Job 3 should have completed (succeeded on 2nd attempt)
    expect(completedIds).toContain(job2.id);
    expect(completedIds).toContain(job3.id);

    queue.close();
  }, 30000);

  test('concurrent retries - 10 jobs fail first, succeed on 2nd', async () => {
    const queue = new Queue<{ idx: number }>('retry-res-concurrent', { embedded: true });
    queue.obliterate();

    const attemptCounts = new Map<string, number>();
    const completedIds: string[] = [];

    for (let i = 0; i < 10; i++) {
      await queue.add(`task-${i}`, { idx: i }, { attempts: 2, backoff: 200 });
    }

    const worker = new Worker<{ idx: number }>(
      'retry-res-concurrent',
      async (job) => {
        const count = (attemptCounts.get(job.id) ?? 0) + 1;
        attemptCounts.set(job.id, count);
        if (count === 1) {
          throw new Error(`First attempt fails for idx ${job.data.idx}`);
        }
        return { done: true };
      },
      { concurrency: 5 }
    );

    worker.on('completed', (job) => {
      completedIds.push(job.id);
    });

    for (let i = 0; i < 80; i++) {
      await Bun.sleep(100);
      if (completedIds.length >= 10) break;
    }

    await worker.close();

    expect(completedIds.length).toBe(10);

    queue.close();
  }, 30000);

  test('no retry when attempts:1 - job fails immediately', async () => {
    const queue = new Queue<{ value: number }>('retry-res-no-retry', { embedded: true });
    queue.obliterate();

    let processCount = 0;
    let failedEventCount = 0;

    await queue.add('task', { value: 1 }, { attempts: 1, backoff: 100 });

    const worker = new Worker<{ value: number }>(
      'retry-res-no-retry',
      async () => {
        processCount++;
        throw new Error('Single attempt failure');
      },
      { concurrency: 1 }
    );

    worker.on('failed', () => {
      failedEventCount++;
    });

    for (let i = 0; i < 20; i++) {
      await Bun.sleep(100);
      if (failedEventCount >= 1) break;
    }

    // Extra wait to ensure no retry happens
    await Bun.sleep(500);

    await worker.close();

    expect(failedEventCount).toBe(1);
    expect(processCount).toBe(1);

    queue.close();
  }, 30000);

  test('DLQ integration - exhausted retries land in DLQ, then retryDlq works', async () => {
    const queue = new Queue<{ value: number }>('retry-res-dlq', { embedded: true });
    queue.obliterate();

    queue.setDlqConfig({ maxRetries: 2 });

    let processCount = 0;

    const job = await queue.add('task', { value: 42 }, { attempts: 2, backoff: 100 });

    // Worker 1: always fails -> exhausts retries -> goes to DLQ
    const worker1 = new Worker<{ value: number }>(
      'retry-res-dlq',
      async () => {
        processCount++;
        throw new Error('Always fails');
      },
      { concurrency: 1 }
    );

    // Wait for all attempts to exhaust (2 attempts with backoff)
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (processCount >= 2) break;
    }

    // Extra sleep to let DLQ insertion complete
    await Bun.sleep(300);

    await worker1.close();

    expect(processCount).toBe(2);

    // Verify job is in DLQ
    const dlqEntries = queue.getDlq();
    const dlqEntry = dlqEntries.find((e) => e.job.id === job.id);
    expect(dlqEntry).toBeDefined();
    expect(dlqEntry!.reason).toBe('max_attempts_exceeded');

    // Retry from DLQ
    const retried = queue.retryDlq(job.id);
    expect(retried).toBe(1);

    // Verify DLQ is now empty for this job
    const dlqAfter = queue.getDlq();
    const stillInDlq = dlqAfter.find((e) => e.job.id === job.id);
    expect(stillInDlq).toBeUndefined();

    // Worker 2: succeeds -> verify the retried job completes
    let completedAfterRetry = false;

    const worker2 = new Worker<{ value: number }>(
      'retry-res-dlq',
      async () => {
        return { recovered: true };
      },
      { concurrency: 1 }
    );

    worker2.on('completed', () => {
      completedAfterRetry = true;
    });

    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (completedAfterRetry) break;
    }

    await worker2.close();

    expect(completedAfterRetry).toBe(true);

    queue.close();
  }, 30000);
});
