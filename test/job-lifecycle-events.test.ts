/**
 * Job Lifecycle & Events Tests (Embedded Mode)
 *
 * Tests covering state transitions, event ordering, progress tracking,
 * multiple event listeners, job data updates, and completed event results.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Job Lifecycle & Events - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. complete state transitions — waiting -> active -> completed', async () => {
    const queue = new Queue('lifecycle-complete-states', { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 1 });
    const jobId = job.id;

    // After push, job should be in waiting state
    const stateAfterPush = await queue.getJobState(jobId);
    expect(stateAfterPush).toBe('waiting');

    let sawActive = false;
    let sawCompleted = false;

    const worker = new Worker(
      'lifecycle-complete-states',
      async (job) => {
        // While processing, job should be active
        const stateWhileActive = await queue.getJobState(job.id);
        if (stateWhileActive === 'active') {
          sawActive = true;
        }
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('completed', () => {
      sawCompleted = true;
    });

    // Wait for job to complete
    for (let i = 0; i < 300; i++) {
      if (sawCompleted) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(sawActive).toBe(true);
    expect(sawCompleted).toBe(true);

    // After completion, job should be in completed state
    const stateAfterComplete = await queue.getJobState(jobId);
    expect(stateAfterComplete).toBe('completed');

    queue.close();
  }, 30000);

  test('2. failed state transition — waiting -> active -> failed', async () => {
    const queue = new Queue('lifecycle-failed-states', { embedded: true });
    queue.obliterate();

    const job = await queue.add('failing-task', { value: 2 }, { attempts: 1 });
    const jobId = job.id;

    // After push, job should be in waiting state
    const stateAfterPush = await queue.getJobState(jobId);
    expect(stateAfterPush).toBe('waiting');

    let sawActive = false;
    let sawFailed = false;

    const worker = new Worker(
      'lifecycle-failed-states',
      async (job) => {
        const stateWhileActive = await queue.getJobState(job.id);
        if (stateWhileActive === 'active') {
          sawActive = true;
        }
        throw new Error('Deliberate failure');
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('failed', () => {
      sawFailed = true;
    });

    // Wait for job to fail
    for (let i = 0; i < 300; i++) {
      if (sawFailed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(sawActive).toBe(true);
    expect(sawFailed).toBe(true);

    // After failure with attempts=1, job should be in failed state
    const stateAfterFail = await queue.getJobState(jobId);
    expect(stateAfterFail).toBe('failed');

    queue.close();
  }, 30000);

  test('3. event ordering — active fires before completed/failed', async () => {
    const queue = new Queue('lifecycle-event-order', { embedded: true });
    queue.obliterate();

    await queue.add('ordered-task', { value: 3 });

    const eventOrder: string[] = [];

    const worker = new Worker(
      'lifecycle-event-order',
      async () => {
        return { result: 'ok' };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('active', () => {
      eventOrder.push('active');
    });

    worker.on('completed', () => {
      eventOrder.push('completed');
    });

    // Wait for completion
    for (let i = 0; i < 300; i++) {
      if (eventOrder.includes('completed')) break;
      await Bun.sleep(100);
    }

    await worker.close();

    // Active should fire before completed
    expect(eventOrder.length).toBeGreaterThanOrEqual(2);
    expect(eventOrder.indexOf('active')).toBeLessThan(eventOrder.indexOf('completed'));

    queue.close();
  }, 30000);

  test('4. progress tracking end-to-end — updateProgress values received via event', async () => {
    const queue = new Queue('lifecycle-progress', { embedded: true });
    queue.obliterate();

    await queue.add('progress-task', { value: 4 });

    const progressValues: number[] = [];
    let completed = false;

    const worker = new Worker(
      'lifecycle-progress',
      async (job) => {
        await job.updateProgress(25);
        await job.updateProgress(50);
        await job.updateProgress(100);
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('progress', (_job, progress) => {
      progressValues.push(progress);
    });

    worker.on('completed', () => {
      completed = true;
    });

    // Wait for completion
    for (let i = 0; i < 300; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completed).toBe(true);
    expect(progressValues).toEqual([25, 50, 100]);

    queue.close();
  }, 30000);

  test('5. multiple event listeners — all 3 completed listeners fire for each job', async () => {
    const queue = new Queue('lifecycle-multi-listener', { embedded: true });
    queue.obliterate();

    await queue.add('multi-task', { value: 5 });

    let listener1Fired = false;
    let listener2Fired = false;
    let listener3Fired = false;

    const worker = new Worker(
      'lifecycle-multi-listener',
      async () => {
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('completed', () => {
      listener1Fired = true;
    });

    worker.on('completed', () => {
      listener2Fired = true;
    });

    worker.on('completed', () => {
      listener3Fired = true;
    });

    // Wait for all listeners to fire
    for (let i = 0; i < 300; i++) {
      if (listener1Fired && listener2Fired && listener3Fired) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(listener1Fired).toBe(true);
    expect(listener2Fired).toBe(true);
    expect(listener3Fired).toBe(true);

    queue.close();
  }, 30000);

  test('6. job data update — update data mid-processing and verify', async () => {
    const queue = new Queue('lifecycle-data-update', { embedded: true });
    queue.obliterate();

    const job = await queue.add('update-task', { original: true, count: 0 });
    const jobId = job.id;

    let dataBeforeUpdate: unknown = null;
    let dataAfterUpdate: unknown = null;
    let completed = false;

    const worker = new Worker(
      'lifecycle-data-update',
      async (job) => {
        dataBeforeUpdate = { ...job.data };

        // Update the job data mid-processing
        await queue.updateJobData(job.id, { original: false, count: 42, updated: true });

        // Retrieve the job again to verify data was updated
        const updatedJob = await queue.getJob(job.id);
        if (updatedJob) {
          dataAfterUpdate = updatedJob.data;
        }

        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('completed', () => {
      completed = true;
    });

    // Wait for completion
    for (let i = 0; i < 300; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completed).toBe(true);
    expect(dataBeforeUpdate).toBeDefined();
    expect((dataBeforeUpdate as { original: boolean }).original).toBe(true);
    expect(dataAfterUpdate).toBeDefined();
    expect((dataAfterUpdate as { original: boolean }).original).toBe(false);
    expect((dataAfterUpdate as { count: number }).count).toBe(42);
    expect((dataAfterUpdate as { updated: boolean }).updated).toBe(true);

    queue.close();
  }, 30000);

  test('7. completed event with result — result accessible in event listener', async () => {
    const queue = new Queue('lifecycle-result', { embedded: true });
    queue.obliterate();

    await queue.add('result-task', { value: 7 });

    let capturedResult: unknown = null;
    let capturedJobId: string | null = null;
    let completed = false;

    const worker = new Worker(
      'lifecycle-result',
      async () => {
        return { answer: 42 };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('completed', (job, result) => {
      capturedJobId = job.id;
      capturedResult = result;
      completed = true;
    });

    // Wait for completion
    for (let i = 0; i < 300; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completed).toBe(true);
    expect(capturedJobId).toBeDefined();
    expect(capturedResult).toBeDefined();
    expect((capturedResult as { answer: number }).answer).toBe(42);

    queue.close();
  }, 30000);
});
