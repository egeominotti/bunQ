/**
 * Test: Worker API enhancements (BullMQ v5 compatibility)
 *
 * Tests for: concurrency getter/setter, closing promise, off(), name/opts properties
 */
import { describe, it, expect, afterEach } from 'bun:test';
import { Worker } from '../src/client/worker/worker';
import { getSharedManager, shutdownManager } from '../src/client/manager';
import { unlinkSync } from 'fs';

const DB_PATH = '/tmp/test-worker-api-enhancements.db';

function cleanup() {
  for (const f of [DB_PATH, `${DB_PATH}-wal`, `${DB_PATH}-shm`]) {
    try {
      unlinkSync(f);
    } catch {}
  }
}

describe('Worker API enhancements', () => {
  let worker: Worker;

  afterEach(async () => {
    try {
      await worker?.close(true);
    } catch {}
    try {
      shutdownManager();
    } catch {}
    cleanup();
  });

  // ============ name / opts properties ============

  it('name is publicly accessible', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    expect(worker.name).toBe('test-queue');
  });

  it('opts is publicly accessible with resolved defaults', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      concurrency: 5,
    });

    expect(worker.opts.concurrency).toBe(5);
    expect(worker.opts.autorun).toBe(false);
    expect(worker.opts.embedded).toBe(true);
    expect(typeof worker.opts.heartbeatInterval).toBe('number');
    expect(typeof worker.opts.batchSize).toBe('number');
  });

  // ============ concurrency getter/setter ============

  it('concurrency getter returns current value', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      concurrency: 3,
    });

    expect(worker.concurrency).toBe(3);
  });

  it('concurrency setter changes value at runtime', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      concurrency: 1,
    });

    expect(worker.concurrency).toBe(1);
    worker.concurrency = 10;
    expect(worker.concurrency).toBe(10);
    expect(worker.opts.concurrency).toBe(10);
  });

  it('concurrency setter clamps to minimum 1', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    worker.concurrency = 0;
    expect(worker.concurrency).toBe(1);

    worker.concurrency = -5;
    expect(worker.concurrency).toBe(1);
  });

  it('concurrency setter works while worker is running', async () => {
    cleanup();

    const queue = 'concurrency-test';
    const processed: string[] = [];

    // Create worker first — this initializes the shared manager
    worker = new Worker(
      queue,
      async (job) => {
        processed.push(job.name);
        await Bun.sleep(50);
        return 'done';
      },
      {
        embedded: true,
        dataPath: DB_PATH,
        autorun: false,
        concurrency: 1,
      }
    );

    // Push jobs via the same shared manager
    const mgr = getSharedManager(DB_PATH);
    for (let i = 0; i < 6; i++) {
      await mgr.push(queue, { name: `job-${i}`, data: { i } });
    }

    worker.run();
    expect(worker.concurrency).toBe(1);

    // Increase concurrency mid-flight
    await Bun.sleep(100);
    worker.concurrency = 3;
    expect(worker.concurrency).toBe(3);

    // Wait for all jobs to process
    await Bun.sleep(500);
    expect(processed.length).toBe(6);
  });

  // ============ closing property ============

  it('closing is null when worker is not closing', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    expect(worker.closing).toBeNull();
  });

  it('closing returns a Promise during close()', async () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    worker.run();
    const closePromise = worker.close();

    // closing should now be a Promise
    expect(worker.closing).not.toBeNull();
    expect(worker.closing).toBeInstanceOf(Promise);

    await closePromise;
    expect(worker.isClosed()).toBe(true);
  });

  it('multiple close() calls return the same promise', async () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    worker.run();
    const p1 = worker.close();
    const p2 = worker.close();

    // Both should reference the same underlying promise
    expect(worker.closing).not.toBeNull();

    await Promise.all([p1, p2]);
    expect(worker.isClosed()).toBe(true);
  });

  // ============ off() method ============

  it('off() removes event listener', async () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    let callCount = 0;
    const listener = () => {
      callCount++;
    };

    worker.on('drained', listener);
    worker.emit('drained');
    expect(callCount).toBe(1);

    worker.off('drained', listener);
    worker.emit('drained');
    expect(callCount).toBe(1); // Should not increase
  });

  it('off() works with typed events', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    let called = false;
    const listener = () => {
      called = true;
    };

    worker.on('closed', listener);
    worker.off('closed', listener);
    worker.emit('closed');
    expect(called).toBe(false);
  });

  // ============ skipLockRenewal ============

  it('skipLockRenewal disables heartbeat timer', async () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      skipLockRenewal: true,
      heartbeatInterval: 10000,
    });

    worker.run();
    // Access internal heartbeatTimer — should be null when skipLockRenewal is true
    const timer = (worker as unknown as { heartbeatTimer: unknown }).heartbeatTimer;
    expect(timer).toBeNull();
    expect(worker.opts.skipLockRenewal).toBe(true);
  });

  // ============ skipStalledCheck ============

  it('skipStalledCheck disables stalled event subscription', async () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      skipStalledCheck: true,
    });

    worker.run();
    const unsubscribe = (worker as unknown as { stalledUnsubscribe: unknown }).stalledUnsubscribe;
    expect(unsubscribe).toBeNull();
    expect(worker.opts.skipStalledCheck).toBe(true);
  });

  // ============ drainDelay ============

  it('drainDelay is stored in opts', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      drainDelay: 200,
    });

    expect(worker.opts.drainDelay).toBe(200);
  });

  it('drainDelay defaults to 50', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    expect(worker.opts.drainDelay).toBe(50);
  });

  // ============ lockDuration / maxStalledCount ============

  it('lockDuration and maxStalledCount stored with defaults', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
    });

    expect(worker.opts.lockDuration).toBe(30000);
    expect(worker.opts.maxStalledCount).toBe(1);
  });

  it('lockDuration and maxStalledCount accept custom values', () => {
    cleanup();
    worker = new Worker('test-queue', async () => 'ok', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      lockDuration: 60000,
      maxStalledCount: 5,
    });

    expect(worker.opts.lockDuration).toBe(60000);
    expect(worker.opts.maxStalledCount).toBe(5);
  });

  // ============ removeOnComplete / removeOnFail ============

  it('removeOnComplete applies worker default to jobs', async () => {
    cleanup();

    worker = new Worker('remove-test', async () => 'done', {
      embedded: true,
      dataPath: DB_PATH,
      autorun: false,
      removeOnComplete: true,
    });

    const mgr = getSharedManager(DB_PATH);
    const job = await mgr.push('remove-test', { name: 'job-1', data: {} });

    worker.run();
    await Bun.sleep(300);

    // Job should be removed after completion (removeOnComplete = true)
    const state = await mgr.getJobState(job.id);
    expect(state).toBe('unknown'); // removed from all indexes
  });

  it('removeOnFail applies worker default to jobs', async () => {
    cleanup();

    worker = new Worker(
      'remove-fail-test',
      async () => {
        throw new Error('intentional failure');
      },
      {
        embedded: true,
        dataPath: DB_PATH,
        autorun: false,
        removeOnFail: true,
      }
    );

    // Suppress error events from crashing the test
    worker.on('error', () => {});

    const mgr = getSharedManager(DB_PATH);
    const job = await mgr.push('remove-fail-test', {
      name: 'fail-job',
      data: {},
      maxAttempts: 1,
      backoff: 0,
    });

    worker.run();
    await Bun.sleep(500);

    // Job should be removed after final failure (removeOnFail = true)
    const state = await mgr.getJobState(job.id);
    expect(state).toBe('unknown');
  });
});
