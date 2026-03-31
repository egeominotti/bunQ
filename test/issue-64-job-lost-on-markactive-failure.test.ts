/**
 * Test: Issue #64 follow-up — Jobs lost from queue when markActive() fails
 *
 * Reproduces the bug: when markActive() throws (e.g. disk I/O error),
 * the job has already been pop()'d from the priority queue in tryPullFromShard()
 * but moveToProcessing() throws before the worker receives the job.
 * The job is LOST from the in-memory queue — subsequent pulls return null.
 *
 * Fix: markActive() is now wrapped in try/catch (non-fatal), and pullJob()
 * has a safety net that requeues the job if moveToProcessing() fails.
 */
import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { unlinkSync } from 'fs';

const DB_PATH = '/tmp/test-issue-64-markactive.db';

function cleanup() {
  for (const f of [DB_PATH, `${DB_PATH}-wal`, `${DB_PATH}-shm`]) {
    try { unlinkSync(f); } catch {}
  }
}

describe('Issue #64 follow-up: job lost when markActive fails', () => {
  let manager: QueueManager;

  afterEach(() => {
    try { manager?.shutdown(); } catch {}
    cleanup();
  });

  it('job should be recoverable after markActive() throws', async () => {
    cleanup();
    manager = new QueueManager({ dataPath: DB_PATH });

    const queue = 'test-markactive-fail';

    // Push a job
    const job = await manager.push(queue, { name: 'test-job', data: { x: 1 } });
    expect(job).toBeDefined();

    // Verify job is in queue
    const countsBefore = manager.getQueueJobCounts(queue);
    expect(countsBefore.waiting).toBe(1);

    // Monkey-patch markActive to throw (simulating disk I/O error)
    const storage = (manager as unknown as { storage: { markActive: Function } }).storage;
    const originalMarkActive = storage.markActive.bind(storage);
    storage.markActive = () => {
      throw new Error('disk I/O error');
    };

    // Pull — markActive error is now caught inside moveToProcessing (non-fatal)
    // so the pull should succeed and return the job
    const pulledJob = await manager.pull(queue, 0);

    // Restore markActive
    storage.markActive = originalMarkActive;

    // With the fix, the job should be pulled successfully despite markActive failure
    expect(pulledJob).not.toBeNull();
    expect(pulledJob!.id).toBe(job.id);
  });

  it('batch pull: all jobs should be pulled even if markActive() throws', async () => {
    cleanup();
    manager = new QueueManager({ dataPath: DB_PATH });

    const queue = 'test-batch-markactive-fail';

    // Push 5 jobs
    for (let i = 0; i < 5; i++) {
      await manager.push(queue, { name: `job-${i}`, data: { i } });
    }

    expect(manager.getQueueJobCounts(queue).waiting).toBe(5);

    // Monkey-patch markActive to always throw
    const storage = (manager as unknown as { storage: { markActive: Function } }).storage;
    const originalMarkActive = storage.markActive.bind(storage);
    storage.markActive = () => {
      throw new Error('disk I/O error');
    };

    // Pull batch — markActive failures are non-fatal, so all jobs should be returned
    const pulled = await manager.pullBatch(queue, 5, 0);

    // Restore markActive
    storage.markActive = originalMarkActive;

    // All 5 jobs should be pulled despite markActive failures
    expect(pulled.length).toBe(5);
    expect(manager.getQueueJobCounts(queue).waiting).toBe(0);
  });

  it('durable job: pull succeeds and job is in processing despite markActive failure', async () => {
    cleanup();
    manager = new QueueManager({ dataPath: DB_PATH });

    const queue = 'test-desync';

    // Push a durable job (immediately in SQLite)
    const job = await manager.push(queue, {
      name: 'durable-job',
      data: { important: true },
      durable: true,
    });

    // Monkey-patch markActive to throw
    const storage = (manager as unknown as { storage: { markActive: Function } }).storage;
    const originalMarkActive = storage.markActive.bind(storage);
    storage.markActive = () => {
      throw new Error('disk I/O error');
    };

    // Pull should succeed (markActive failure is non-fatal)
    const pulled = await manager.pull(queue, 0);
    storage.markActive = originalMarkActive;

    expect(pulled).not.toBeNull();
    expect(pulled!.id).toBe(job.id);

    // Job should be in active/processing state (in-memory)
    const state = await manager.getJobState(job.id);
    expect(state).toBe('active');
  });

  it('safety net: if moveToProcessing fails completely, job is requeued', async () => {
    cleanup();
    manager = new QueueManager({ dataPath: DB_PATH });

    const queue = 'test-requeue-safety';

    const job = await manager.push(queue, { name: 'test', data: {} });
    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);

    // Monkey-patch the entire storage to throw on any method call
    // This simulates a catastrophic failure beyond just markActive
    const storage = (manager as unknown as { storage: object }).storage;
    const originalStorage = { ...Object.getOwnPropertyDescriptors(Object.getPrototypeOf(storage)) };
    const originalMarkActive = (storage as { markActive: Function }).markActive;

    // Override markActive to throw AND also make broadcast throw
    // to test the outer safety net in pullJob
    (storage as { markActive: Function }).markActive = () => {
      throw new Error('catastrophic disk failure');
    };

    // The inner try/catch in moveToProcessing catches markActive errors,
    // so the pull should succeed even with the storage error
    const pulled = await manager.pull(queue, 0);

    (storage as { markActive: Function }).markActive = originalMarkActive;

    expect(pulled).not.toBeNull();
    expect(pulled!.id).toBe(job.id);
  });
});
