/**
 * Floating-promise defect surfaced by the Biome migration (noFloatingPromises):
 * `Queue.removeAsync()` — which backs the BullMQ-style `job.remove()` — does NOT
 * await the cancellation on the EMBEDDED path.
 *
 *   // src/client/queue/operations/management.ts
 *   export async function removeAsync(ctx, id): Promise<void> {
 *     if (ctx.embedded) {
 *       getSharedManager().cancel(jobId(id));  // ← fire-and-forget, NOT awaited
 *       return;
 *     }
 *     await ctx.tcp!.send({ cmd: 'Cancel', id }); // TCP path DOES await
 *   }
 *
 * `cancel()` is `async` and performs the removal inside `await withWriteLock(...)`
 * (jobManagement.cancelJob). So on embedded, `await job.remove()` can resolve
 * BEFORE the job is actually removed, the cancel's result/errors are swallowed,
 * and the path is inconsistent with the (correct) TCP path.
 *
 * Deterministic repro via lock contention: hold the shard WRITE lock so the
 * cancel blocks inside `withWriteLock`. A CORRECT removeAsync (awaiting cancel)
 * must stay pending while the lock is held; the BUGGY one resolves immediately.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { rmSync } from 'node:fs';
import { Queue } from '../src/client';
import { getSharedManager, shutdownManager } from '../src/client/manager';
import { shardIndex } from '../src/shared/hash';
import { jobId } from '../src/domain/types/job';

describe('removeAsync embedded must await the cancellation (no floating cancel)', () => {
  const TEST_DB = `/tmp/bunq-removeasync-${Date.now()}-${process.pid}.db`;
  let queue: Queue<{ x: number }>;

  afterEach(async () => {
    try {
      await queue?.close?.();
    } catch {
      /* ignore */
    }
    shutdownManager();
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        rmSync(TEST_DB + suffix);
      } catch {
        /* ignore */
      }
    }
  });

  test('await queue.removeAsync(id) does not resolve until cancel() completes', async () => {
    const QUEUE = 'removeasync-await';
    queue = new Queue<{ x: number }>(QUEUE, { embedded: true, dataPath: TEST_DB });
    const job = await queue.add('t', { x: 1 }, { durable: true });
    const id = String(job.id);

    const mgr = getSharedManager() as unknown as {
      shardLocks: Array<{ acquireWrite(): Promise<{ release(): void }> }>;
      jobIndex: Map<unknown, unknown>;
    };
    const idx = shardIndex(QUEUE);

    // Hold the shard WRITE lock → cancelJob's `await withWriteLock(...)` blocks
    // inside cancel(), so the removal cannot complete while we hold it.
    const guard = await mgr.shardLocks[idx].acquireWrite();

    let removeResolved = false;
    const p = queue
      .removeAsync(id)
      .then(() => {
        removeResolved = true;
      })
      .catch(() => {
        removeResolved = true;
      });

    // release() is idempotent — the finally guarantees the lock is freed even if
    // an assertion throws first (otherwise the pending removeAsync + held lock
    // would stall afterEach's queue.close()).
    try {
      // Let removeAsync run. cancel() is blocked on the held lock.
      await Bun.sleep(60);

      // ===== CORRECT (post-fix) behavior =====
      // removeAsync awaited cancel(), which is blocked → it is still PENDING.
      // PRE-FIX: removeAsync fired cancel() without awaiting → already resolved (true).
      expect(removeResolved).toBe(false);

      // Release the lock → cancel() proceeds → removeAsync resolves.
      guard.release();
      await p;
      expect(removeResolved).toBe(true);

      // And the job is actually removed once the awaited cancel finished.
      expect(mgr.jobIndex.has(jobId(id))).toBe(false);
    } finally {
      guard.release();
    }
  });
});
