/**
 * BullMQ v5 Compatibility - Manual State Transition Operations
 * Tests for moveToWait (Failed->Waiting), moveToDelayed (Waiting->Delayed),
 * and moveToWaitingChildren (Active->WaitingChildren) in embedded mode.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('BullMQ v5 moveToWait - Failed to Waiting', () => {
  let queue: Queue<{ value: number }>;

  afterEach(() => {
    queue?.close();
    shutdownManager();
  });

  test('moveToWait should move a failed/DLQ job back to waiting', async () => {
    queue = new Queue('move-wait-dlq', { embedded: true });
    queue.obliterate();

    // Add a job with only 1 attempt so it goes to DLQ immediately on failure
    const job = await queue.add('test', { value: 42 }, { attempts: 1 });

    // Process and fail it so it lands in DLQ (attempts exhausted)
    const worker = new Worker(
      'move-wait-dlq',
      async () => {
        throw new Error('Intentional failure');
      },
      { embedded: true }
    );

    // Wait for the job to be processed and land in DLQ
    for (let i = 0; i < 50; i++) {
      const state = await queue.getJobState(job.id);
      if (state === 'failed') break;
      await Bun.sleep(100);
    }
    await worker.close();

    // Confirm the job is now in failed state (DLQ)
    const stateBeforeMove = await queue.getJobState(job.id);
    expect(stateBeforeMove).toBe('failed');

    // Now move it back to waiting
    const result = await queue.moveJobToWait(job.id);
    expect(result).toBe(true);

    // Verify state is now waiting
    const stateAfterMove = await queue.getJobState(job.id);
    expect(stateAfterMove).toBe('waiting');
  });
});

describe('BullMQ v5 moveToWait - Active to Waiting', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, unknown>;

  afterEach(async () => {
    await worker?.close(true);
    queue?.close();
    shutdownManager();
  });

  test(
    'moveToWait from active state should re-queue the job',
    async () => {
      queue = new Queue('move-active-wait', { embedded: true });
      queue.obliterate();

      const job = await queue.add('test', { value: 100 });

      worker = new Worker(
        'move-active-wait',
        async () => {
          // Block the worker so the job stays active
          await new Promise((r) => setTimeout(r, 60000));
          return null;
        },
        { embedded: true }
      );

      // Poll until the job becomes active
      for (let i = 0; i < 100; i++) {
        const state = await queue.getJobState(job.id);
        if (state === 'active') break;
        await Bun.sleep(100);
      }

      const stateBefore = await queue.getJobState(job.id);
      expect(stateBefore).toBe('active');

      // Move active job back to wait via queue method
      const result = await queue.moveJobToWait(job.id);
      expect(result).toBe(true);

      const stateAfter = await queue.getJobState(job.id);
      expect(stateAfter).toBe('waiting');
    },
    15000
  );
});

describe('BullMQ v5 moveToDelayed - Waiting to Delayed', () => {
  let queue: Queue<{ value: number }>;

  afterEach(() => {
    queue?.close();
    shutdownManager();
  });

  test('moveToDelayed should delay a waiting job', async () => {
    queue = new Queue('move-delayed-wait', { embedded: true });
    queue.obliterate();

    // Add a job with no delay - it should be in waiting state
    const job = await queue.add('test', { value: 42 });

    // Confirm it's waiting
    const stateBefore = await queue.getJobState(job.id);
    expect(stateBefore).toBe('waiting');

    // Move it to delayed with a timestamp 10 seconds in the future
    const futureTimestamp = Date.now() + 10000;
    await queue.moveJobToDelayed(job.id, futureTimestamp);

    // Verify state is now delayed
    const stateAfter = await queue.getJobState(job.id);
    expect(stateAfter).toBe('delayed');
  });

  test('moveToDelayed from failed state should move to delayed', async () => {
    queue = new Queue('move-delayed-failed', { embedded: true });
    queue.obliterate();

    // Add a job with 1 attempt to make it fail immediately into DLQ
    const job = await queue.add('test', { value: 99 }, { attempts: 1 });

    const worker = new Worker(
      'move-delayed-failed',
      async () => {
        throw new Error('Intentional failure');
      },
      { embedded: true }
    );

    // Wait for DLQ
    for (let i = 0; i < 50; i++) {
      const state = await queue.getJobState(job.id);
      if (state === 'failed') break;
      await Bun.sleep(100);
    }
    await worker.close();

    // Confirm failed state
    const stateBeforeMove = await queue.getJobState(job.id);
    expect(stateBeforeMove).toBe('failed');

    // Move to delayed with a future timestamp
    const futureTimestamp = Date.now() + 10000;
    await queue.moveJobToDelayed(job.id, futureTimestamp);

    // Verify state is now delayed
    const stateAfterMove = await queue.getJobState(job.id);
    expect(stateAfterMove).toBe('delayed');
  });
});

describe('BullMQ v5 moveToWaitingChildren (embedded)', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, unknown>;

  afterEach(async () => {
    await worker?.close(true);
    queue?.close();
    shutdownManager();
  });

  test(
    'moveToWaitingChildren should move active job to waiting-children',
    async () => {
      queue = new Queue('move-wc', { embedded: true });
      queue.obliterate();

      const job = await queue.add('parent', { value: 1 });

      worker = new Worker(
        'move-wc',
        async () => {
          // Block the worker so the job stays active
          await new Promise((r) => setTimeout(r, 60000));
          return null;
        },
        { embedded: true }
      );

      // Poll until the job becomes active
      for (let i = 0; i < 100; i++) {
        const state = await queue.getJobState(job.id);
        if (state === 'active') break;
        await Bun.sleep(100);
      }

      const stateBefore = await queue.getJobState(job.id);
      expect(stateBefore).toBe('active');

      // Move active job to waiting-children via queue method
      const moveResult = await queue.moveJobToWaitingChildren(job.id);
      expect(moveResult).toBe(true);

      // Verify state is now waiting-children
      const state = await queue.getJobState(job.id);
      expect(state).toBe('waiting-children');
    },
    15000
  );
});
