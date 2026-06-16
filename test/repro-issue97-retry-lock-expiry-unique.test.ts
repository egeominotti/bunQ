/**
 * ISSUE #97: Retrying a job that reached `failed` via the LOCK-EXPIRY path
 * throws `UNIQUE constraint failed: jobs.id`.
 *
 * Root cause: src/application/lockManager.ts `handleMaxStallsExceeded` moves the
 * job to the DLQ using ONLY in-memory state (`shard.addToDlq` + `jobIndex.set`).
 * Unlike its three sibling paths — which all persist the move:
 *   - max-attempts        ack.ts `moveFailedJobToDlq`        (saveDlqEntry + deleteJob)
 *   - heartbeat-stall     stallDetection.ts `moveStalliedJobToDlq` (saveDlqEntry + deleteJob)
 *   - startup-recovery    backgroundTasks.ts                 (saveDlqEntry + deleteJob)
 * — the lock-expiry path calls NEITHER `storage.saveDlqEntry(entry)` NOR
 * `storage.deleteJob(jobId)`. So:
 *   1. the `jobs` row survives in SQLite as an orphan (state still active), and
 *   2. the DLQ entry exists only in memory (lost on restart).
 *
 * When the failed job is then retried, dlqManager.retryDlqJob re-inserts the
 * job with its original id via `storage.insertJob(job, true)`, and the
 * `insertJob` prepared statement is a PLAIN `INSERT INTO jobs` (not
 * `INSERT OR REPLACE` like insertResult/insertCron). Re-inserting the surviving
 * orphan row hits the PRIMARY KEY → `UNIQUE constraint failed: jobs.id`.
 *
 * Deterministic repro (no background timers / no TCP):
 *   setStallConfig(maxStalls: 1) so the FIRST lock expiry is terminal and goes
 *   straight to handleMaxStallsExceeded; durable push so the jobs row is on disk
 *   immediately; pullWithLock with a short TTL that is never renewed; sleep past
 *   the TTL; run checkExpiredLocks() to drive the lock-expiry sweep.
 *
 * CORRECT (post-fix) behavior: the lock-expiry DLQ move persists like its
 * siblings — the orphan `jobs` row is deleted, the DLQ entry is saved, and
 * retryDlq() resolves without throwing.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';
import { checkExpiredLocks } from '../src/application/lockManager';

function getInternalLockContext(qm: QueueManager): any {
  const qmAny = qm as any;
  return {
    shards: qmAny.shards,
    shardLocks: qmAny.shardLocks,
    processingShards: qmAny.processingShards,
    processingLocks: qmAny.processingLocks,
    jobIndex: qmAny.jobIndex,
    jobLocks: qmAny.jobLocks,
    clientJobs: qmAny.clientJobs,
    eventsManager: qmAny.eventsManager,
    stalledCandidates: qmAny.stalledCandidates,
    storage: qmAny.storage,
    dashboardEmit: null,
  };
}

describe('Issue #97: retry after lock-expiry DLQ must not throw UNIQUE constraint', () => {
  const TEST_DB = `/tmp/bunq-issue97-${Date.now()}-${process.pid}.db`;
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager({ dataPath: TEST_DB });
  });

  afterEach(() => {
    qm.shutdown();
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        rmSync(TEST_DB + suffix);
      } catch {
        /* ignore */
      }
    }
  });

  test('lock-expiry → max-stalls DLQ persists (no orphan jobs row) and retry succeeds', async () => {
    const QUEUE = 'retry-unique';

    // maxStalls = 1 → the first lock expiry is terminal and routes to
    // handleMaxStallsExceeded (the buggy path), not requeueExpiredJob.
    qm.setStallConfig(QUEUE, { maxStalls: 1 });

    // durable push → the jobs row is written to SQLite immediately (bypasses the
    // 10ms write buffer), so the orphan-row assertion below is deterministic.
    const job = await qm.push(QUEUE, { data: { kind: 'stall' }, maxAttempts: 3, durable: true });

    const storage = (qm as any).storage;
    // Precondition: the jobs row is on disk.
    expect(storage.getJob(job.id)).not.toBeNull();

    // Pull with a short lock TTL; the lock is never renewed.
    const pulled = await qm.pullWithLock(QUEUE, 'worker-1', 0, 50);
    expect(pulled.job).not.toBeNull();

    // Let the lock expire.
    await Bun.sleep(80);

    // Drive the lock-expiry sweep: stallCount 0 → 1 >= maxStalls (1) →
    // handleMaxStallsExceeded → DLQ.
    await checkExpiredLocks(getInternalLockContext(qm));

    // The job is now failed (in the DLQ).
    expect(await qm.getJobState(job.id)).toBe('failed');

    // ===== CORRECT post-fix persistence behavior =====
    // 1) The orphan jobs row must be deleted from SQLite.
    //    PRE-FIX: still present (handleMaxStallsExceeded never called deleteJob).
    expect(storage.getJob(job.id)).toBeNull();
    // 2) The DLQ entry must be persisted to SQLite (not memory-only).
    //    PRE-FIX: false (handleMaxStallsExceeded never called saveDlqEntry).
    expect(storage.hasDlqEntry(job.id)).toBe(true);

    // ===== The user-visible symptom: retry must not throw =====
    let threw: string | null = null;
    let retried = 0;
    try {
      retried = qm.retryDlq(QUEUE, job.id);
    } catch (err) {
      threw = (err as Error).message;
    }
    // PRE-FIX: threw === 'UNIQUE constraint failed: jobs.id'
    expect(threw).toBeNull();
    expect(retried).toBe(1);

    // After a successful retry the job is back in 'waiting' and pullable.
    expect(await qm.getJobState(job.id)).toBe('waiting');
    const repulled = await qm.pull(QUEUE, 0);
    expect(repulled).not.toBeNull();
    expect(String(repulled!.id)).toBe(String(job.id));
  });
});
