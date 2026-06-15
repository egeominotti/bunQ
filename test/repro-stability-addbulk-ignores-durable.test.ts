/**
 * Stability bug: addBulk / PUSHB silently ignores the `durable` flag.
 *
 * Root cause:
 *   - A single durable push takes the immediate-write path:
 *       push.ts:253  ctx.storage?.insertJob(result.job, input.durable);
 *       sqlite.ts:281 insertJob(job, durable) -> if (durable) insertJobImmediate(job)
 *     => the row is on disk the instant push() returns (bypasses the WriteBuffer).
 *
 *   - A batch push (addBulk / PUSHB) takes the buffered-write path:
 *       push.ts:315  ctx.storage?.insertJobsBatch(jobsToInsert);   // <-- no durable arg
 *       sqlite.ts:544 insertJobsBatch(jobs) -> this.writeBuffer.addBatch(jobs); // ALWAYS buffered
 *     => even when every job in the batch was added with { durable: true }, the rows
 *        sit in the 10ms WriteBuffer and are NOT on disk when pushBatch() returns.
 *        `insertJobsBatch` does not even accept a `durable` parameter, so the flag
 *        is dropped on the floor.
 *
 * Impact: a process crash in the buffered window loses jobs the caller explicitly
 * asked to be durable (zero-loss). The single-push durable contract is silently
 * violated for the batch path.
 *
 * Repro strategy (no crash needed): drive SqliteStorage directly with a long
 * write-buffer flush interval so the buffered window is wide and deterministic.
 *  - A single durable insert (insertJob(job, true)) MUST be visible to a separate
 *    read-only SQLite connection immediately. (Baseline — proves the durable
 *    contract for single push.)
 *  - A durable batch insert (insertJobsBatch([job])) MUST ALSO be visible
 *    immediately. The CORRECT post-fix behavior is that durable batch rows are on
 *    disk before the call returns — this assertion FAILS today (RED), because the
 *    batch path always buffers, proving the durable flag is ignored for batches.
 *
 * No src/ file is modified; this test only documents/proves the bug.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { QueueManager } from '../src/application/queueManager';
import type { Job, JobId } from '../src/domain/types/job';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomUUID } from 'crypto';
import { unlinkSync } from 'fs';

function newJob(id: number): Job {
  return {
    id: String(id) as JobId,
    queue: 'q',
    data: { v: id },
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

/** Read a job's persisted state via a SEPARATE read-only connection (true disk check). */
function readStateFromDisk(dbPath: string, id: JobId): string | null {
  const db = new Database(dbPath, { readonly: true });
  try {
    const row = db
      .prepare<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(id));
    return row?.state ?? null;
  } finally {
    db.close();
  }
}

describe('Bug: addBulk/PUSHB ignores the durable flag (batch jobs always go through the write buffer)', () => {
  let dbPath: string;
  let storage: SqliteStorage;

  beforeEach(() => {
    dbPath = join(tmpdir(), `bunq-durable-batch-${randomUUID()}.db`);
    storage = new SqliteStorage({
      path: dbPath,
      // Very long flush so the buffered window is wide & deterministic — a
      // durable write must NOT depend on the timer to reach disk.
      writeBufferFlushMs: 60_000,
      writeBufferSize: 1000,
    });
  });

  afterEach(() => {
    try {
      (storage as unknown as { close?: () => void }).close?.();
    } catch {
      // ignore
    }
    try {
      unlinkSync(dbPath);
    } catch {
      // ignore
    }
  });

  test('BASELINE: a single durable push is on disk immediately (bypasses write buffer)', () => {
    const job = newJob(1);
    storage.insertJob(job, true); // durable single push

    // Visible to a separate connection before any flush — this is the durable contract.
    expect(readStateFromDisk(dbPath, job.id)).toBe('waiting');
  });

  test('CONTROL: a non-durable single push is buffered (NOT on disk before flush)', () => {
    const job = newJob(2);
    storage.insertJob(job); // non-durable -> buffered

    // Confirms the buffered window really exists with this flush config.
    expect(readStateFromDisk(dbPath, job.id)).toBeNull();
  });

  test('STORAGE CONTRACT: a durable batch insert is on disk immediately', () => {
    const job = newJob(3);

    // The fix makes insertJobsBatch honor a durable flag (bypass the buffer),
    // matching the single durable insert. RED pre-fix: insertJobsBatch took no
    // durable param and always buffered.
    storage.insertJobsBatch([job], true);

    expect(readStateFromDisk(dbPath, job.id)).toBe('waiting');
  });

  test('STORAGE CONTRACT: a non-durable batch insert is still buffered (buffer not defeated)', () => {
    const job = newJob(4);
    storage.insertJobsBatch([job]); // no durable -> buffered

    expect(readStateFromDisk(dbPath, job.id)).toBeNull();
  });

  test('PROOF OF ASYMMETRY: durable single and durable batch are BOTH on disk immediately', () => {
    const single = newJob(100);
    const batched = newJob(101);

    storage.insertJob(single, true); // durable single -> immediate
    storage.insertJobsBatch([batched], true); // durable batch -> must ALSO be immediate

    const singleState = readStateFromDisk(dbPath, single.id);
    const batchState = readStateFromDisk(dbPath, batched.id);

    expect(singleState).toBe('waiting');
    // Durable guarantee must be symmetric across single and batch paths.
    expect(batchState).toBe(singleState);
  });
});

describe('End-to-end: QueueManager.pushBatch routes durable jobs to the immediate path', () => {
  let dbPath: string;
  let qm: QueueManager;

  beforeEach(() => {
    dbPath = join(tmpdir(), `bunq-durable-e2e-${randomUUID()}.db`);
    qm = new QueueManager({ dataPath: dbPath });
  });

  afterEach(async () => {
    try {
      await qm.shutdown();
    } catch {
      // ignore
    }
    try {
      unlinkSync(dbPath);
    } catch {
      // ignore
    }
  });

  test('a durable addBulk batch is on disk immediately (read before any flush timer)', async () => {
    // The real user path: addBulk / PUSHB -> pushJobBatch. Pre-fix, pushJobBatch
    // called insertJobsBatch with no durable arg, so durable jobs sat in the
    // 10ms write buffer. We read the DB on a SEPARATE connection synchronously
    // after pushBatch resolves (the 10ms flush timer has not fired yet), so a
    // durable job is only visible if it bypassed the buffer.
    const ids = await qm.pushBatch('durable-q', [
      { data: { i: 1 }, durable: true },
      { data: { i: 2 }, durable: true },
    ]);
    expect(ids.length).toBe(2);

    for (const id of ids) {
      expect(readStateFromDisk(dbPath, id)).toBe('waiting');
    }
  });
});
