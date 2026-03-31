/**
 * Priority & Delayed Jobs Tests (Embedded Mode)
 *
 * Tests for priority ordering, high-priority preemption, delayed execution,
 * promote, changePriority, mixed priority+delay, and priority with concurrency.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Priority & Delayed Jobs - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. priority ordering -- jobs processed in descending priority order', async () => {
    const queue = new Queue<{ priority: number }>('prio-ordering', { embedded: true });
    queue.obliterate();

    // Push 5 jobs with priorities 1,5,3,2,4
    const priorities = [1, 5, 3, 2, 4];
    for (const p of priorities) {
      await queue.add(`job-p${p}`, { priority: p }, { priority: p });
    }

    const processedOrder: number[] = [];

    const worker = new Worker<{ priority: number }>(
      'prio-ordering',
      async (job) => {
        processedOrder.push(job.data.priority);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 5 to complete
    for (let i = 0; i < 100; i++) {
      if (processedOrder.length >= 5) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processedOrder.length).toBe(5);
    // Higher priority = processed first (descending order)
    expect(processedOrder).toEqual([5, 4, 3, 2, 1]);

    queue.obliterate();
    queue.close();
  }, 30000);

  test('2. high priority preemption -- high-priority job processed before remaining low-priority', async () => {
    const queue = new Queue<{ label: string }>('prio-preempt', { embedded: true });
    queue.obliterate();

    const processedOrder: string[] = [];
    let processingStarted = false;

    // Push 10 low-priority jobs
    for (let i = 0; i < 10; i++) {
      await queue.add(`low-${i}`, { label: `low-${i}` }, { priority: 1 });
    }

    // Start a worker with concurrency 1, each job takes some time
    const worker = new Worker<{ label: string }>(
      'prio-preempt',
      async (job) => {
        processingStarted = true;
        processedOrder.push(job.data.label);
        await Bun.sleep(100); // Simulate work
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for worker to start processing the first job
    for (let i = 0; i < 50; i++) {
      if (processingStarted) break;
      await Bun.sleep(20);
    }

    // Now add 1 high-priority job while worker is busy
    await queue.add('high-1', { label: 'HIGH' }, { priority: 100 });

    // Wait for all 11 jobs to complete
    for (let i = 0; i < 200; i++) {
      if (processedOrder.length >= 11) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processedOrder.length).toBe(11);
    // The HIGH job should appear before all remaining low-priority jobs
    // It cannot be first (first low job was already processing), but should
    // come before most low jobs
    const highIndex = processedOrder.indexOf('HIGH');
    expect(highIndex).toBeGreaterThanOrEqual(0);
    // HIGH should be processed well before the last low jobs
    // It should be within the first few (after the ones already pulled)
    expect(highIndex).toBeLessThan(5);

    queue.obliterate();
    queue.close();
  }, 30000);

  test('3. delayed job execution -- job not processed immediately but after delay', async () => {
    const queue = new Queue<{ value: number }>('prio-delayed-exec', { embedded: true });
    queue.obliterate();

    let processedAt = 0;
    const addedAt = Date.now();

    // Push a job with 500ms delay
    await queue.add('delayed-job', { value: 42 }, { delay: 500 });

    const worker = new Worker<{ value: number }>(
      'prio-delayed-exec',
      async (job) => {
        processedAt = Date.now();
        return { value: job.data.value };
      },
      { embedded: true, concurrency: 1 }
    );

    // Check that it is NOT processed immediately (within 200ms)
    await Bun.sleep(200);
    expect(processedAt).toBe(0);

    // Wait for the delay to pass and the job to be processed
    for (let i = 0; i < 100; i++) {
      if (processedAt > 0) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processedAt).toBeGreaterThan(0);
    // Verify it was processed after the delay
    const elapsed = processedAt - addedAt;
    expect(elapsed).toBeGreaterThanOrEqual(400); // Allow some tolerance

    queue.obliterate();
    queue.close();
  }, 30000);

  test('4. promote delayed job -- promoted job processed immediately', async () => {
    const queue = new Queue<{ value: number }>('prio-promote', { embedded: true });
    queue.obliterate();

    let processed = false;

    // Push a job with very long delay (5000ms)
    const job = await queue.add('long-delayed', { value: 99 }, { delay: 5000 });

    // Verify it is in delayed state
    const stateBefore = await queue.getJobState(job.id);
    expect(stateBefore).toBe('delayed');

    // Promote it
    await queue.promoteJob(job.id);

    // Verify state changed to waiting
    const stateAfter = await queue.getJobState(job.id);
    expect(stateAfter).toBe('waiting');

    // Start worker and verify it processes immediately
    const startTime = Date.now();
    const worker = new Worker<{ value: number }>(
      'prio-promote',
      async () => {
        processed = true;
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Should be processed quickly (well before the original 5s delay)
    for (let i = 0; i < 100; i++) {
      if (processed) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processed).toBe(true);
    const elapsed = Date.now() - startTime;
    // Should have been processed in well under 5 seconds
    expect(elapsed).toBeLessThan(3000);

    queue.obliterate();
    queue.close();
  }, 30000);

  test('5. change job priority -- low priority changed to high runs before other low-priority', async () => {
    const queue = new Queue<{ label: string }>('prio-change', { embedded: true });
    queue.obliterate();

    // Push 3 low-priority jobs
    const job1 = await queue.add('first', { label: 'A' }, { priority: 1 });
    await queue.add('second', { label: 'B' }, { priority: 1 });
    await queue.add('third', { label: 'C' }, { priority: 1 });

    // Change job1's priority to very high
    await queue.changeJobPriority(job1.id, { priority: 100 });

    const processedOrder: string[] = [];

    const worker = new Worker<{ label: string }>(
      'prio-change',
      async (job) => {
        processedOrder.push(job.data.label);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 3 to complete
    for (let i = 0; i < 100; i++) {
      if (processedOrder.length >= 3) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processedOrder.length).toBe(3);
    // Job A (now priority 100) should be processed first
    expect(processedOrder[0]).toBe('A');

    queue.obliterate();
    queue.close();
  }, 30000);

  test('6. mixed priority and delay -- correct ordering with both dimensions', async () => {
    const queue = new Queue<{ label: string }>('prio-mixed', { embedded: true });
    queue.obliterate();

    const processedOrder: string[] = [];

    // In this queue system, delayed jobs with higher priority sit at the top of
    // the priority heap and block lower-priority immediate jobs from being pulled
    // until the delay expires. This test verifies that behavior:
    //
    // 1. A delayed high-priority job blocks immediate low-priority jobs
    // 2. Once the delay expires, the high-priority job is processed first
    // 3. Among same-priority immediate jobs, order is FIFO

    // Add immediate low-priority jobs
    await queue.add('imm-low-1', { label: 'ImmLow1' }, { priority: 1 });
    await queue.add('imm-low-2', { label: 'ImmLow2' }, { priority: 1 });

    // Add delayed medium-priority job (short delay so test is fast)
    await queue.add('del-med', { label: 'DelMed' }, { priority: 50, delay: 300 });

    // Add immediate high-priority job (higher priority than low, lower than delayed)
    // Since DelMed has highest priority, it sits at top of heap and blocks all pulls
    // until its delay expires. But ImmHigh has lower priority (10), so once DelMed
    // is processed, ImmHigh goes next, then the low-priority jobs.
    await queue.add('imm-high', { label: 'ImmHigh' }, { priority: 10 });

    // Start worker with concurrency 1
    const worker = new Worker<{ label: string }>(
      'prio-mixed',
      async (job) => {
        processedOrder.push(job.data.label);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 4 to complete
    for (let i = 0; i < 200; i++) {
      if (processedOrder.length >= 4) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processedOrder.length).toBe(4);

    // DelMed (priority 50, delayed 300ms) blocks the queue and processes first once ready
    expect(processedOrder[0]).toBe('DelMed');

    // After DelMed, ImmHigh (priority 10) should come before ImmLow (priority 1)
    const immHighIdx = processedOrder.indexOf('ImmHigh');
    const immLow1Idx = processedOrder.indexOf('ImmLow1');
    const immLow2Idx = processedOrder.indexOf('ImmLow2');

    expect(immHighIdx).toBeLessThan(immLow1Idx);
    expect(immHighIdx).toBeLessThan(immLow2Idx);

    queue.obliterate();
    queue.close();
  }, 30000);

  test('7. priority with concurrency -- higher priority jobs generally complete first', async () => {
    const queue = new Queue<{ priority: number; index: number }>('prio-concurrent', {
      embedded: true,
    });
    queue.obliterate();

    // Push 20 jobs with varied priorities
    // 5 high (priority 100), 5 medium (priority 50), 10 low (priority 1)
    const jobs: Array<{ name: string; data: { priority: number; index: number }; opts: { priority: number } }> = [];

    for (let i = 0; i < 5; i++) {
      jobs.push({
        name: `high-${i}`,
        data: { priority: 100, index: i },
        opts: { priority: 100 },
      });
    }
    for (let i = 0; i < 5; i++) {
      jobs.push({
        name: `med-${i}`,
        data: { priority: 50, index: i + 5 },
        opts: { priority: 50 },
      });
    }
    for (let i = 0; i < 10; i++) {
      jobs.push({
        name: `low-${i}`,
        data: { priority: 1, index: i + 10 },
        opts: { priority: 1 },
      });
    }

    await queue.addBulk(jobs);

    const completionOrder: Array<{ priority: number; index: number }> = [];

    const worker = new Worker<{ priority: number; index: number }>(
      'prio-concurrent',
      async (job) => {
        completionOrder.push({ priority: job.data.priority, index: job.data.index });
        await Bun.sleep(30); // Small delay to simulate work
        return { ok: true };
      },
      { embedded: true, concurrency: 3 }
    );

    // Wait for all 20 to complete
    for (let i = 0; i < 200; i++) {
      if (completionOrder.length >= 20) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(completionOrder.length).toBe(20);

    // With concurrency 3, exact ordering isn't deterministic, but higher-priority
    // jobs should generally complete earlier. Check that the average position of
    // high-priority jobs is earlier than low-priority jobs.
    const highPositions = completionOrder
      .map((item, idx) => ({ ...item, pos: idx }))
      .filter((item) => item.priority === 100)
      .map((item) => item.pos);

    const lowPositions = completionOrder
      .map((item, idx) => ({ ...item, pos: idx }))
      .filter((item) => item.priority === 1)
      .map((item) => item.pos);

    const avgHighPos = highPositions.reduce((a, b) => a + b, 0) / highPositions.length;
    const avgLowPos = lowPositions.reduce((a, b) => a + b, 0) / lowPositions.length;

    // High-priority jobs should have a lower average position (processed earlier)
    expect(avgHighPos).toBeLessThan(avgLowPos);

    // Additionally, at least 3 of the first 5 completed should be high-priority
    const firstFive = completionOrder.slice(0, 5);
    const highInFirstFive = firstFive.filter((item) => item.priority === 100).length;
    expect(highInFirstFive).toBeGreaterThanOrEqual(3);

    queue.obliterate();
    queue.close();
  }, 30000);
});
