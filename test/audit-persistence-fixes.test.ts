/**
 * Audit reproduction tests for two confirmed persistence bugs.
 *
 * These tests assert the CORRECT behavior, so they FAIL on the current
 * (buggy) code and would pass once each bug is fixed (red -> green).
 *
 * ---------------------------------------------------------------------------
 * BUG H2 — Critical write-buffer losses are never persisted to DLQ.
 *   src/infrastructure/persistence/sqlite.ts:102-141 (handleCriticalLoss)
 *   + sqliteBatch.ts:209-216
 *
 *   When the WriteBuffer exhausts its 10 retry attempts, the dropped jobs are
 *   logged to stderr and retained in the in-memory `_criticalLosses` array
 *   (capped at 100), but are NEVER persisted via saveDlqEntry(). After a
 *   process restart the in-memory array is gone, so the lost jobs are
 *   unrecoverable.
 *
 *   Correct behavior: lost jobs MUST be durably persisted (to the DLQ table)
 *   so they survive a restart and are recoverable via the normal DLQ path
 *   (getDlqEntry / loadDlq).
 *
 * ---------------------------------------------------------------------------
 * BUG H3 — Corrupt dependsOn blob silently becomes an empty-deps job.
 *   src/infrastructure/persistence/sqliteSerializer.ts:19-27 (unpack)
 *
 *   On a MessagePack decode failure for a job's `depends_on` blob, unpack()
 *   catches the error and silently returns the fallback []. A job whose
 *   dependency metadata is corrupt is therefore treated as having NO
 *   dependencies and is queued / runs out-of-order — its real predecessors
 *   are ignored.
 *
 *   Correct behavior: a corrupt dependsOn blob MUST NOT be silently swallowed
 *   into an empty-deps job. rowToJob() stamps the recovered Job with a
 *   collision-proof Symbol marker (CORRUPT_DEPENDS_ON) — NOT a magic-string
 *   dependency id that could collide with a real user jobId. The recovery path
 *   (backgroundTasks.recover) detects the marker and routes the job to the DLQ
 *   (FailureReason.Unknown / 'corrupt-dependency-metadata'). It must never be
 *   enqueued as ready (out-of-order execution) nor parked in waitingDeps behind
 *   a never-satisfied dependency (an unbounded leak).
 */

import { describe, test, expect } from 'bun:test';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomUUID } from 'crypto';
import { unlinkSync } from 'fs';
import { Database } from 'bun:sqlite';

import {
  WriteBuffer,
  type BatchInsertManager,
} from '../src/infrastructure/persistence/sqliteBatch';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import {
  rowToJob,
  pack,
  isCorruptDependsOn,
} from '../src/infrastructure/persistence/sqliteSerializer';
import { QueueManager } from '../src/application/queueManager';
import type { DbJob } from '../src/infrastructure/persistence/statements';
import type { Job, JobId } from '../src/domain/types/job';
import { jobId } from '../src/domain/types/job';

function mockJob(id: number, queue = 'q'): Job {
  return {
    id: BigInt(id) as JobId,
    queue,
    data: { i: id },
    priority: 0,
    createdAt: Date.now(),
    runAt: Date.now(),
    attempts: 0,
    maxAttempts: 3,
    backoff: 1000,
    ttl: null,
    timeout: null,
    uniqueKey: null,
    customId: null,
    dependsOn: [],
    parentId: null,
    childrenIds: [],
    tags: [],
    lifo: false,
    groupId: null,
    removeOnComplete: false,
    removeOnFail: false,
    stallTimeout: null,
    startedAt: null,
    completedAt: null,
    failedReason: null,
    progress: null,
    lastHeartbeat: null,
    lockToken: null,
    lockExpiresAt: null,
    timeline: [],
  } as unknown as Job;
}

function freshDbPath(): string {
  return join(tmpdir(), `bunq-audit-${randomUUID()}.db`);
}

function closeAndUnlink(storage: SqliteStorage, dbPath: string): void {
  try {
    (storage as unknown as { db: { close: (b: boolean) => void } }).db.close(false);
  } catch {
    // ignore
  }
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      unlinkSync(dbPath + suffix);
    } catch {
      // ignore
    }
  }
}

// ===========================================================================
// BUG H2
// ===========================================================================
describe('BUG H2: critical write-buffer losses must be persisted (survive restart)', () => {
  test('dropped jobs are persisted to the DLQ table, not only the in-memory array', () => {
    const dbPath = freshDbPath();
    const storage = new SqliteStorage({
      path: dbPath,
      writeBufferSize: 1000,
      writeBufferFlushMs: 60_000,
    });

    // Force every batch flush to fail (simulates persistent disk-full).
    const buffer = (storage as unknown as { writeBuffer: WriteBuffer }).writeBuffer;
    const failingMgr = {
      insertJobsBatch: () => {
        throw new Error('Simulated disk-full');
      },
    } as unknown as BatchInsertManager;
    (buffer as unknown as { batchManager: BatchInsertManager }).batchManager = failingMgr;

    const lostA = mockJob(1001);
    const lostB = mockJob(1002);
    storage.insertJob(lostA);
    storage.insertJob(lostB);

    // Exhaust the 10 retries -> WriteBuffer drops the jobs -> handleCriticalLoss.
    for (let i = 0; i < 11; i++) {
      try {
        buffer.flush();
      } catch {
        // expected
      }
    }

    // Sanity: the buggy in-memory record DOES exist.
    const losses = storage.getCriticalLosses();
    expect(losses.length).toBe(1);
    expect(losses[0].jobs.length).toBe(2);

    // CORRECT BEHAVIOR (asserted): the lost jobs must ALSO be durably persisted
    // to the DLQ table so they survive a restart. We bypass the (now failing)
    // write buffer / in-memory state and read straight from disk via the DLQ
    // recovery path. Today nothing is written -> these are null -> test FAILS.
    const recoveredA = storage.getDlqEntry(lostA.id);
    const recoveredB = storage.getDlqEntry(lostB.id);

    expect(recoveredA).not.toBeNull();
    expect(recoveredB).not.toBeNull();
    expect(recoveredA?.job.id).toBe(lostA.id);
    expect(recoveredB?.job.id).toBe(lostB.id);

    closeAndUnlink(storage, dbPath);
  });

  test('lost jobs are recoverable from disk after a simulated restart', () => {
    const dbPath = freshDbPath();

    // --- session 1: trigger the critical loss ---
    const storage1 = new SqliteStorage({
      path: dbPath,
      writeBufferSize: 1000,
      writeBufferFlushMs: 60_000,
    });
    const buffer = (storage1 as unknown as { writeBuffer: WriteBuffer }).writeBuffer;
    (buffer as unknown as { batchManager: BatchInsertManager }).batchManager = {
      insertJobsBatch: () => {
        throw new Error('Simulated disk-full');
      },
    } as unknown as BatchInsertManager;

    const lost = mockJob(2001, 'payments');
    storage1.insertJob(lost);
    for (let i = 0; i < 11; i++) {
      try {
        buffer.flush();
      } catch {
        // expected
      }
    }
    // Close the DB WITHOUT the buffer flushing successfully (jobs are lost
    // from memory). The in-memory _criticalLosses array dies with the process.
    (storage1 as unknown as { db: { close: (b: boolean) => void } }).db.close(false);

    // --- session 2: fresh process opens the same DB file ---
    const storage2 = new SqliteStorage({ path: dbPath });

    // CORRECT BEHAVIOR (asserted): the lost job is recoverable from the DLQ
    // table after restart. Today it is not persisted at all -> loadDlq() is
    // empty and getDlqEntry() is null -> test FAILS.
    const dlqByQueue = storage2.loadDlq();
    const allEntries = [...dlqByQueue.values()].flat();
    expect(allEntries.length).toBeGreaterThan(0);

    const recovered = storage2.getDlqEntry(lost.id);
    expect(recovered).not.toBeNull();
    expect(recovered?.job.id).toBe(lost.id);

    closeAndUnlink(storage2, dbPath);
  });
});

// ===========================================================================
// BUG H3
// ===========================================================================
describe('BUG H3: corrupt dependsOn blob must not silently become an empty-deps job', () => {
  // A genuinely corrupt MessagePack blob: 0xde is a map16 header claiming 5
  // entries, but the buffer is truncated -> msgpackr throws on decode.
  const CORRUPT_DEPENDS_ON = new Uint8Array([0xde, 0x00, 0x05]);

  test('rowToJob must NOT swallow a corrupt depends_on blob into dependsOn: []', () => {
    // Build a job row exactly as it would be read back from SQLite, but with
    // a corrupt depends_on BLOB. The real dependency metadata is unrecoverable.
    const row: DbJob = {
      id: '3001',
      queue: 'orders',
      data: pack({ step: 'charge' }),
      priority: 0,
      created_at: Date.now(),
      run_at: Date.now(),
      started_at: null,
      completed_at: null,
      attempts: 0,
      max_attempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      unique_key: null,
      custom_id: null,
      depends_on: CORRUPT_DEPENDS_ON,
      parent_id: null,
      children_ids: null,
      tags: null,
      state: 'waiting',
      lifo: 0,
      group_id: null,
      progress: null,
      progress_msg: null,
      remove_on_complete: 0,
      remove_on_fail: 0,
      stall_timeout: null,
      last_heartbeat: null,
      timeline: null,
    };

    // CORRECT BEHAVIOR (asserted): converting a row with corrupt dependency
    // metadata must NOT yield a job that silently looks "ready with no deps".
    // rowToJob() surfaces the corruption with a collision-proof Symbol marker
    // (isCorruptDependsOn) rather than a magic-string dependency id.
    const job = rowToJob(row);

    // The corruption MUST be signaled via the collision-proof Symbol marker.
    expect(isCorruptDependsOn(job)).toBe(true);

    // The marker must NOT be a magic string injected into dependsOn — a
    // user-supplied jobId could otherwise collide with it. dependsOn carries
    // no fabricated sentinel id.
    expect(job.dependsOn).not.toContain(jobId('__corrupt_depends_on__'));
    for (const dep of job.dependsOn) {
      expect(String(dep)).not.toContain('corrupt');
    }
  });

  // Seed a job row with a CORRUPT depends_on blob directly into the jobs table
  // (state='waiting'), exactly as it would exist on disk after a partial write.
  function seedCorruptDepsJob(dbPath: string, id: string, queue: string): void {
    const storage = new SqliteStorage({ path: dbPath });
    const db = (storage as unknown as { db: Database }).db;
    const insertSql = `
      INSERT INTO jobs (
        id, queue, data, priority, created_at, run_at, attempts,
        max_attempts, backoff, ttl, timeout, unique_key, custom_id,
        depends_on, parent_id, children_ids, tags, state, lifo, group_id,
        remove_on_complete, remove_on_fail, stall_timeout, timeline
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const now = Date.now();
    db.prepare(insertSql).run(
      id,
      queue,
      pack({ step: 'ship' }),
      0,
      now,
      now,
      0,
      3,
      1000,
      null,
      null,
      null,
      null,
      CORRUPT_DEPENDS_ON, // <-- corrupt dependency metadata
      null,
      null,
      null,
      'waiting',
      0,
      null,
      0,
      0,
      null,
      null
    );
    db.close(false);
  }

  test('recovery routes a corrupt-deps job to the DLQ and never enqueues it as ready', async () => {
    const dbPath = freshDbPath();
    seedCorruptDepsJob(dbPath, '4002', 'orders');

    // Constructing the QueueManager runs backgroundTasks.recover() over the DB.
    const qm = new QueueManager({ dataPath: dbPath });
    try {
      // CORRECT BEHAVIOR (asserted): the corrupt-deps job is routed to the DLQ
      // with the dedicated reason, NOT enqueued onto the ready queue.
      const dlqEntries = qm.getDlqEntries('orders');
      expect(dlqEntries.length).toBe(1);
      expect(String(dlqEntries[0].job.id)).toBe('4002');
      expect(dlqEntries[0].error).toBe('corrupt-dependency-metadata');

      // Job state is 'failed' (DLQ), never runnable/ready.
      const state = await qm.getJobState(jobId('4002'));
      expect(state).toBe('failed');

      // INVARIANT: it must NOT be sitting on the ready queue (out-of-order risk).
      const mem = qm.getMemoryStats();
      expect(mem.queuedTotal).toBe(0);
    } finally {
      qm.shutdown();
      unlinkSync(dbPath);
      for (const suffix of ['-wal', '-shm']) {
        try {
          unlinkSync(dbPath + suffix);
        } catch {
          // ignore
        }
      }
    }
  });

  test('recovery must NOT leak a corrupt-deps job into waitingDeps (unbounded leak)', () => {
    const dbPath = freshDbPath();
    // Several corrupt-deps jobs to make a leak visible if one occurred.
    seedCorruptDepsJob(dbPath, '5001', 'orders');
    seedCorruptDepsJob(dbPath, '5002', 'orders');
    seedCorruptDepsJob(dbPath, '5003', 'orders');

    const qm = new QueueManager({ dataPath: dbPath });
    try {
      // CORRECT BEHAVIOR (asserted): corrupt-deps jobs go to the DLQ; none are
      // parked behind a never-satisfied dependency in waitingDeps (the leak).
      const mem = qm.getMemoryStats();
      expect(mem.waitingDepsTotal).toBe(0);
      expect(mem.queuedTotal).toBe(0);
      expect(qm.getDlqEntries('orders').length).toBe(3);
    } finally {
      qm.shutdown();
      unlinkSync(dbPath);
      for (const suffix of ['-wal', '-shm']) {
        try {
          unlinkSync(dbPath + suffix);
        } catch {
          // ignore
        }
      }
    }
  });
});
