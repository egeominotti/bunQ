/**
 * REPRODUCTION TESTS — group "critical-dataloss" (mode: embedded)
 *
 * Run with:
 *   BUNQUEUE_EMBEDDED=1 bun test test/repro-audit-critical-dataloss.test.ts
 *
 * These tests assert the CORRECT (expected) behavior so they go RED on the
 * current (buggy) code and would go GREEN once the bug is fixed. They DO NOT
 * fix anything and DO NOT touch src/.
 *
 * ----------------------------------------------------------------------------
 * SINGLETON CAVEAT
 * ----------------------------------------------------------------------------
 * In embedded mode, `new Queue(name, { embedded: true, dataPath })` resolves a
 * PROCESS-SINGLETON QueueManager via getSharedManager() (src/client/manager.ts:
 * `instance ??= new QueueManager(...)`). The FIRST embedded Queue in the process
 * pins the manager to its dataPath; later dataPaths are ignored until the
 * singleton is reset. We therefore call `shutdownManager()` at the end of each
 * test to (a) reset the singleton so the next test gets a fresh manager on its
 * OWN dataPath, and (b) deterministically trigger the final flush + drop:
 *
 *   shutdownManager() -> manager.shutdown() -> storage.close()
 *     -> writeBuffer.stop() -> flush() (fails on the poison UNIQUE collision)
 *     -> reportLostJobs() (drops EVERYTHING still buffered, incl. the innocent
 *        unrelated jobs that were batched in the same flush window)
 *
 * This avoids the ~24s "exhaust 10 retries" wait from the original repro while
 * being just as deterministic: the unrelated valid jobs are permanently absent
 * from the on-disk `jobs` table after shutdown.
 *
 * After shutdown we open a READONLY bun:sqlite Database on the same dataPath and
 * assert the unrelated VALID job(s) survived (persisted/queryable). On the buggy
 * code those rows are missing -> RED.
 */

import { describe, test, expect, afterEach, beforeEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, rmSync } from 'node:fs';
import { Queue, Worker, shutdownManager } from '../src/client';

// Track every dataPath we create so afterEach can scrub it (db + -wal + -shm).
const createdPaths = new Set<string>();
let counter = 0;
function freshDbPath(tag: string): string {
  const p = `/tmp/bq-repro-dataloss-${tag}-${process.pid}-${++counter}-${Date.now()}.db`;
  createdPaths.add(p);
  return p;
}

function rowsInJobsTable(dbPath: string, where: string, params: unknown[]): number {
  const db = new Database(dbPath, { readonly: true });
  try {
    const row = db
      .query<{ c: number }, unknown[]>(`SELECT count(*) AS c FROM jobs WHERE ${where}`)
      .get(...(params as never[]));
    return row?.c ?? 0;
  } finally {
    db.close();
  }
}

beforeEach(() => {
  // Reset the process-singleton BEFORE each test (not just after) so the first
  // `new Queue(..., { dataPath })` here actually pins the manager to OUR dataPath.
  // In the full suite a prior file may have left the singleton pinned elsewhere,
  // which would otherwise send our jobs to the wrong DB and make the on-disk
  // assertions read an empty file (flaky failure depending on file order).
  try {
    shutdownManager();
  } catch {
    /* nothing to reset */
  }
});

afterEach(() => {
  // Ensure the singleton is reset even if a test bailed before shutting down.
  try {
    shutdownManager();
  } catch {
    /* already shut down */
  }
  for (const p of createdPaths) {
    for (const suffix of ['', '-wal', '-shm', '-journal']) {
      const f = p + suffix;
      if (existsSync(f)) {
        try {
          rmSync(f, { force: true });
        } catch {
          /* best effort */
        }
      }
    }
  }
  createdPaths.clear();
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('critical-dataloss repro (embedded)', () => {
  /**
   * transport-parity-1 (critical)
   * ------------------------------------------------------------------------
   * BUG: a custom jobId reused across DIFFERENT queues on the SAME dataPath
   * collides on the global `jobs.id` PRIMARY KEY. The in-memory dedup is
   * effectively queue-scoped, so BOTH adds are accepted in memory, but the
   * batched SQLite insert throws "UNIQUE constraint failed: jobs.id". Because
   * the write-buffer flush is atomic per batch, the WHOLE batch is poisoned —
   * including an UNRELATED, uniquely-identified valid job in a THIRD queue that
   * happened to be batched in the same 10ms flush window. That valid job is
   * NEVER persisted to SQLite (silent data loss), yet in-memory counts still
   * report it as waiting.
   *
   * EXPECTED (correct): custom jobId is per-queue isolated (or rejected per-job
   * at add time). Crucially, the UNRELATED valid job must survive: it must be
   * persisted/queryable in the `jobs` table, and the in-memory waiting count for
   * its queue must equal the number of persisted rows.
   *
   * This test asserts the EXPECTED behavior, so it FAILS (RED) on current code.
   */
  test('transport-parity-1: duplicate jobId across queues must NOT drop an unrelated valid job', async () => {
    const DB = freshDbPath('xparity');
    const U = `${process.pid}-${counter}`;
    const QA = `A-${U}`;
    const QB = `B-${U}`;
    const QC = `C-${U}`;
    const PX = `PX-${U}`; // duplicated custom jobId (collides on jobs.id)
    const VALID = `VALID-${U}`; // unrelated, unique custom jobId in a third queue

    const qa = new Queue(QA, { embedded: true, dataPath: DB });
    const qb = new Queue(QB, { embedded: true, dataPath: DB });
    const qc = new Queue(QC, { embedded: true, dataPath: DB });

    // Same jobId 'PX' in two different queues -> jobs.id PRIMARY KEY collision.
    await qa.add('x', {}, { jobId: PX });
    await qb.add('x', {}, { jobId: PX });
    // Unrelated, unique id, different queue. MUST NOT be lost.
    await qc.add('x', { important: true }, { jobId: VALID });

    // Let the auto-flush (10ms) fire and fail at least once.
    await sleep(300);

    // In-memory count claims the valid job is waiting...
    const inMemWaitingC = (await qc.getJobCountsAsync()).waiting;
    const stateValid = await qc.getJobState(VALID);

    // Reset singleton -> manager.shutdown() -> storage.close() -> final flush +
    // reportLostJobs(): deterministic drop of the poisoned batch (incl. VALID).
    shutdownManager();

    // Give the close-path a tick to settle the file.
    await sleep(100);
    expect(existsSync(DB)).toBe(true);

    // ---- DETERMINISTIC DATA-LOSS ASSERTIONS (read on-disk SQLite) ----
    const validRows = rowsInJobsTable(DB, 'id = ?', [VALID]);
    const qcRows = rowsInJobsTable(DB, 'queue = ?', [QC]);

    // EXPECTED: the unrelated valid job is persisted (survives the collision of
    // two UNRELATED jobs in OTHER queues). On the buggy code validRows === 0.
    expect(validRows).toBe(1);

    // EXPECTED: in-memory waiting count for queue C == persisted rows for C.
    // On the buggy code: inMemWaitingC === 1 but qcRows === 0 (count!=persisted).
    expect(qcRows).toBe(inMemWaitingC);

    // Sanity: in-memory still believed it was waiting (documents the divergence).
    expect(stateValid).toBe('waiting');
  }, 20000);

  /**
   * dedup-jobid-1 (critical)
   * ------------------------------------------------------------------------
   * BUG: reusing a customId AFTER its job completes. markCompleted does an
   * UPDATE (does NOT delete the row), so the completed row survives on disk with
   * the same id. On re-add, push.ts takes the "allow reuse" path and reuses the
   * SAME id. The batched SQLite insert then collides with the surviving
   * completed row -> "UNIQUE constraint failed: jobs.id", poisoning the whole
   * flush batch. Unrelated valid jobs (a different queue) co-flushed in the same
   * window are permanently dropped from SQLite. Additionally, getJobState
   * returns 'completed' for the brand-new re-added job (it should be 'waiting').
   *
   * EXPECTED (correct): reusing a completed customId yields a fresh WAITING job,
   * and unrelated valid jobs in another queue are NEVER lost — they must persist
   * in the `jobs` table.
   *
   * This test asserts the EXPECTED behavior, so it FAILS (RED) on current code.
   */
  test('dedup-jobid-1: reusing a completed customId must NOT drop unrelated jobs', async () => {
    const DB = freshDbPath('dedup');
    const POISON_Q = `poison-${process.pid}-${counter}`;
    const INNOCENT_Q = `innocent-${process.pid}-${counter}`;
    const POISON_ID = `POISON-${process.pid}-${counter}`;
    const N_INNOCENT = 5;

    const q = new Queue(POISON_Q, { embedded: true, dataPath: DB });

    // 1) Add + complete a job with customId POISON.
    await q.add('x', { v: 1 }, { jobId: POISON_ID });
    const w = new Worker(POISON_Q, async () => ({ ok: true }), {
      concurrency: 1,
      heartbeatInterval: 0,
    });
    // Poll until the first POISON job is completed (deterministic, no fixed wait).
    const deadline = Date.now() + 10000;
    while ((await q.getJobState(POISON_ID)) !== 'completed') {
      if (Date.now() > deadline) throw new Error('timeout waiting for POISON to complete');
      await sleep(20);
    }
    await w.close();
    await sleep(50);

    // 2) Re-add the SAME customId (poison reuse) together with N unrelated
    //    valid jobs on a DIFFERENT queue, so they share the same flush window.
    const innocentIds: string[] = [];
    const innocentQueue = new Queue(INNOCENT_Q, { embedded: true, dataPath: DB });
    await Promise.all([
      q.add('x', { v: 2 }, { jobId: POISON_ID }), // poison reuse
      ...Array.from({ length: N_INNOCENT }, (_, i) =>
        innocentQueue.add('y', { n: i }).then((j) => {
          innocentIds.push(String(j.id));
        })
      ),
    ]);

    // Let the auto-flush fire and fail at least once.
    await sleep(300);

    // EXPECTED: the re-added job is a fresh WAITING job (bug: 'completed').
    const reuseState = await q.getJobState(POISON_ID);

    const inMemInnocentWaiting = (await innocentQueue.getJobCountsAsync()).waiting;

    // Reset singleton -> shutdown -> final flush + drop (deterministic).
    shutdownManager();
    await sleep(100);
    expect(existsSync(DB)).toBe(true);

    // ---- DETERMINISTIC DATA-LOSS ASSERTIONS (read on-disk SQLite) ----
    const innocentRows = rowsInJobsTable(DB, 'queue = ?', [INNOCENT_Q]);

    // EXPECTED: all N unrelated valid jobs persist. On the buggy code: 0.
    expect(innocentRows).toBe(N_INNOCENT);

    // EXPECTED: in-memory waiting count for the innocent queue == persisted rows.
    // On the buggy code: inMemInnocentWaiting === 5 but innocentRows === 0.
    expect(innocentRows).toBe(inMemInnocentWaiting);

    // EXPECTED: reusing a completed customId produces a fresh waiting job.
    // On the buggy code reuseState === 'completed' (never processable).
    expect(reuseState).toBe('waiting');
  }, 30000);
});
