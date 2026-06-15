/**
 * Stability bug: Job completed with an `undefined` result is invisible to
 * dependency recovery on restart -> dependent children are orphaned forever.
 *
 * Root cause (three interacting facts):
 *
 *  1. ack.ts:107 gates the job_results insert on `result !== undefined`.
 *     A job acked with NO result (or `undefined`) gets `state=completed`
 *     written via storage.markCompleted(), but NO row in `job_results`.
 *
 *  2. sqlite.ts loadCompletedJobIds() (used by recovery) is implemented as
 *     `SELECT job_id FROM job_results` — it only sees jobs that produced a
 *     defined result, NOT jobs whose state column is 'completed'.
 *
 *  3. backgroundTasks.ts recover() Phase-2 (line 326) decides whether a
 *     pending child still has unmet dependencies via:
 *         job.dependsOn.every(depId =>
 *             completedJobs.has(depId) || completedInDb.has(depId))
 *     where `completedInDb = loadCompletedJobIds()`. In-memory `completedJobs`
 *     is empty right after a fresh-process restart, so the ONLY signal is the
 *     job_results-backed set.
 *
 * Consequence: parent P completes with an undefined result -> on restart the
 * recovery cannot tell P is done -> child C (dependsOn:[P]) is parked in
 * shard.waitingDeps forever and never becomes pullable. The job is orphaned
 * (silent permanent stall, looks like a hang).
 *
 * Reproduction: persistent dataPath. Push parent P (durable), push child C
 * (durable, dependsOn:[P]). Pull + ack P with NO result. shutdown() (flushes
 * + WAL-checkpoints to disk) then construct a NEW QueueManager on the SAME
 * dataPath (constructor runs recover()). Assert C is pullable after restart.
 *
 * Expected (post-fix): C becomes runnable because P's completed STATE — not
 * just a job_results row — is honored by dependency recovery.
 * Observed (bug): pull('children') returns null forever; C stays in waitingDeps.
 *
 * CONTROL: identical flow but ack P WITH a defined result writes a job_results
 * row, so recovery sees P as done and C becomes pullable (passes today).
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { QueueManager } from '../src/application/queueManager';

describe('Stability: undefined-result parent breaks dependency recovery on restart', () => {
  const dirs: string[] = [];
  let live: QueueManager | null = null;

  function newDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'bq-undef-dep-'));
    dirs.push(dir);
    return dir;
  }

  afterEach(() => {
    try {
      live?.shutdown();
    } catch {
      /* ignore */
    }
    live = null;
    for (const dir of dirs.splice(0)) {
      try {
        rmSync(dir, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
    }
  });

  test('child of an undefined-result parent becomes pullable after restart (RED: bug => stays orphaned)', async () => {
    const dbPath = join(newDir(), 'queue.db');

    // ---- Run 1: build the parent/child dependency, complete parent w/o result ----
    let qm1: QueueManager | null = new QueueManager({ dataPath: dbPath });

    const parent = await qm1.push('parents', { data: { kind: 'parent' }, durable: true });
    const child = await qm1.push('children', {
      data: { kind: 'child' },
      dependsOn: [parent.id],
      durable: true,
    });

    // Child must be parked waiting on the parent (not yet runnable).
    expect(await qm1.pull('children', 0)).toBeNull();

    // Pull + ack the parent with NO result -> state=completed, but NO job_results row.
    const pulledParent = await qm1.pull('parents', 0);
    expect(pulledParent).not.toBeNull();
    expect(pulledParent!.id).toBe(parent.id);
    await qm1.ack(parent.id); // result === undefined -> ack.ts skips storeResult()

    // Persist everything to disk, then drop the process state.
    qm1.shutdown();
    qm1 = null;

    // ---- Run 2 (restart): fresh manager on the SAME db -> constructor runs recover() ----
    live = new QueueManager({ dataPath: dbPath });

    // CORRECT post-fix behavior: the parent completed, so the child must be
    // runnable and pullable after recovery.
    const recoveredChild = await live.pull('children', 0);

    expect(recoveredChild).not.toBeNull();
    expect(recoveredChild!.id).toBe(child.id);
  });

  test('CONTROL: child of a defined-result parent IS pullable after restart (passes today)', async () => {
    const dbPath = join(newDir(), 'queue.db');

    let qm1: QueueManager | null = new QueueManager({ dataPath: dbPath });

    const parent = await qm1.push('parents', { data: { kind: 'parent' }, durable: true });
    const child = await qm1.push('children', {
      data: { kind: 'child' },
      dependsOn: [parent.id],
      durable: true,
    });

    expect(await qm1.pull('children', 0)).toBeNull();

    const pulledParent = await qm1.pull('parents', 0);
    expect(pulledParent).not.toBeNull();
    // Ack WITH a defined result -> writes a job_results row.
    await qm1.ack(parent.id, { ok: true });

    qm1.shutdown();
    qm1 = null;

    live = new QueueManager({ dataPath: dbPath });

    const recoveredChild = await live.pull('children', 0);
    expect(recoveredChild).not.toBeNull();
    expect(recoveredChild!.id).toBe(child.id);
  });
});
