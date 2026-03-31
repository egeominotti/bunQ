/**
 * Pause/Resume Patterns (Embedded Mode)
 *
 * Tests for queue pause/resume behavior including processing stops,
 * active job completion, isPaused state, multiple cycles, concurrent
 * workers, and queued jobs while paused.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Pause/Resume Patterns - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. pause stops processing', async () => {
    const queue = new Queue('pr-pause-stops', { embedded: true });
    queue.obliterate();

    const processed: number[] = [];

    const worker = new Worker(
      'pr-pause-stops',
      async (job) => {
        const data = job.data as { index: number };
        processed.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Push 5 jobs and let the worker process them
    for (let i = 0; i < 5; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Wait until some jobs are processed
    for (let i = 0; i < 100; i++) {
      if (processed.length >= 5) break;
      await Bun.sleep(50);
    }
    expect(processed.length).toBe(5);

    // Pause the queue
    queue.pause();

    const countAtPause = processed.length;

    // Push more jobs after pause
    for (let i = 10; i < 15; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Wait a bit and verify no new jobs are processed
    await Bun.sleep(500);

    expect(processed.length).toBe(countAtPause);

    await worker.close();
    queue.close();
  }, 30000);

  test('2. resume restarts processing', async () => {
    const queue = new Queue('pr-resume-restarts', { embedded: true });
    queue.obliterate();

    const processed: number[] = [];

    const worker = new Worker(
      'pr-resume-restarts',
      async (job) => {
        const data = job.data as { index: number };
        processed.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Pause the queue first
    queue.pause();

    // Push 5 jobs while paused
    for (let i = 0; i < 5; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Verify nothing is processed while paused
    await Bun.sleep(300);
    expect(processed.length).toBe(0);

    // Resume the queue
    queue.resume();

    // Wait for all 5 to be processed
    for (let i = 0; i < 200; i++) {
      if (processed.length >= 5) break;
      await Bun.sleep(50);
    }

    expect(processed.length).toBe(5);

    // Verify all indices present
    const unique = new Set(processed);
    expect(unique.size).toBe(5);

    await worker.close();
    queue.close();
  }, 30000);

  test('3. pause during active processing — active job completes, no new jobs start', async () => {
    const queue = new Queue('pr-pause-active', { embedded: true });
    queue.obliterate();

    const processed: number[] = [];
    let slowJobStarted = false;
    let slowJobFinished = false;

    const worker = new Worker(
      'pr-pause-active',
      async (job) => {
        const data = job.data as { index: number; slow?: boolean };
        if (data.slow) {
          slowJobStarted = true;
          await Bun.sleep(1000);
          slowJobFinished = true;
        }
        processed.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Push a slow job first
    await queue.add('slow-job', { index: 0, slow: true });

    // Wait for the slow job to start processing
    for (let i = 0; i < 100; i++) {
      if (slowJobStarted) break;
      await Bun.sleep(50);
    }
    expect(slowJobStarted).toBe(true);

    // Pause queue while slow job is active
    queue.pause();

    // Push more jobs after pause
    for (let i = 1; i <= 5; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Wait for the slow job to complete
    for (let i = 0; i < 100; i++) {
      if (slowJobFinished) break;
      await Bun.sleep(50);
    }
    expect(slowJobFinished).toBe(true);

    // The slow job should have completed
    expect(processed).toContain(0);

    // Wait a bit to confirm no additional jobs are processed
    await Bun.sleep(500);

    // Only the slow job should have been processed; new jobs should not start
    expect(processed.length).toBe(1);

    await worker.close();
    queue.close();
  }, 30000);

  test('4. isPaused reflects state', async () => {
    const queue = new Queue('pr-ispaused-state', { embedded: true });
    queue.obliterate();

    // Initially not paused
    expect(queue.isPaused()).toBe(false);

    // Pause
    queue.pause();
    expect(queue.isPaused()).toBe(true);

    // Resume
    queue.resume();
    expect(queue.isPaused()).toBe(false);

    // Pause again
    queue.pause();
    expect(queue.isPaused()).toBe(true);

    // Resume again
    queue.resume();
    expect(queue.isPaused()).toBe(false);

    queue.close();
  }, 30000);

  test('5. multiple pause/resume cycles', async () => {
    const queue = new Queue('pr-multi-cycle', { embedded: true });
    queue.obliterate();

    const processed: number[] = [];

    const worker = new Worker(
      'pr-multi-cycle',
      async (job) => {
        const data = job.data as { index: number };
        processed.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Cycle 1: pause, push 3 jobs, resume, wait for processing
    queue.pause();
    for (let i = 0; i < 3; i++) {
      await queue.add(`cycle1-${i}`, { index: i });
    }
    queue.resume();

    for (let i = 0; i < 200; i++) {
      if (processed.length >= 3) break;
      await Bun.sleep(50);
    }
    expect(processed.length).toBe(3);

    // Cycle 2: pause again, push 3 more jobs, resume, wait
    queue.pause();
    for (let i = 3; i < 6; i++) {
      await queue.add(`cycle2-${i}`, { index: i });
    }

    // Verify nothing new processed while paused
    await Bun.sleep(300);
    expect(processed.length).toBe(3);

    queue.resume();

    for (let i = 0; i < 200; i++) {
      if (processed.length >= 6) break;
      await Bun.sleep(50);
    }
    expect(processed.length).toBe(6);

    // Verify all 6 unique indices
    const unique = new Set(processed);
    expect(unique.size).toBe(6);

    await worker.close();
    queue.close();
  }, 30000);

  test('6. pause with concurrent workers', async () => {
    const queue = new Queue('pr-concurrent-workers', { embedded: true });
    queue.obliterate();

    const processed: number[] = [];
    const workers: Worker[] = [];

    // Create 3 workers on the same queue
    for (let w = 0; w < 3; w++) {
      const worker = new Worker(
        'pr-concurrent-workers',
        async (job) => {
          const data = job.data as { index: number };
          processed.push(data.index);
          return { ok: true };
        },
        { embedded: true, concurrency: 2 }
      );
      workers.push(worker);
    }

    // Push initial jobs and let them process
    for (let i = 0; i < 5; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    for (let i = 0; i < 200; i++) {
      if (processed.length >= 5) break;
      await Bun.sleep(50);
    }
    expect(processed.length).toBe(5);

    // Pause the queue
    queue.pause();
    const countAtPause = processed.length;

    // Push more jobs while paused
    for (let i = 10; i < 15; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Wait and verify none of the 3 workers process new jobs
    await Bun.sleep(500);
    expect(processed.length).toBe(countAtPause);

    // Resume and verify processing resumes
    queue.resume();

    for (let i = 0; i < 200; i++) {
      if (processed.length >= countAtPause + 5) break;
      await Bun.sleep(50);
    }
    expect(processed.length).toBe(countAtPause + 5);

    for (const w of workers) {
      await w.close();
    }
    queue.close();
  }, 30000);

  test('7. jobs added while paused are queued', async () => {
    const queue = new Queue('pr-queued-while-paused', { embedded: true });
    queue.obliterate();

    const processed: number[] = [];

    const worker = new Worker(
      'pr-queued-while-paused',
      async (job) => {
        const data = job.data as { index: number };
        processed.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Pause the queue
    queue.pause();

    // Add 10 jobs
    for (let i = 0; i < 10; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Verify all 10 are in waiting state
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(10);
    expect(counts.active).toBe(0);

    // Verify none are processed
    expect(processed.length).toBe(0);

    // Resume
    queue.resume();

    // Wait for all 10 to be processed
    for (let i = 0; i < 200; i++) {
      if (processed.length >= 10) break;
      await Bun.sleep(50);
    }

    expect(processed.length).toBe(10);

    // Verify all 10 unique indices
    const unique = new Set(processed);
    expect(unique.size).toBe(10);

    await worker.close();
    queue.close();
  }, 30000);
});
