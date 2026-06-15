/**
 * STABILITY BUG: Dependency promotion checks only in-memory `completedJobs` —
 * a child job is orphaned forever when its parent has `removeOnComplete: true`.
 *
 * Root cause:
 *   - Dependency readiness is decided SOLELY by `ctx.completedJobs.has(dep)`:
 *       • push.ts:181  -> insertJobToShard: needsWaiting = !dependsOn.every(d => completedJobs.has(d))
 *       • dependencyProcessor.ts:52 -> promote only if dependsOn.every(d => completedJobs.has(d))
 *   - But the ack path INTENTIONALLY skips `completedJobs.add()` when the job has
 *     `removeOnComplete: true`:
 *       • ack.ts:99-116        (single ack)      -> else branch: jobIndex.delete + storage.deleteJob, NO completedJobs.add
 *       • ackHelpers.ts:224-244 (batch finalize)  -> same: removeOnComplete skips completedJobs.add
 *   - Both ack paths STILL call onJobCompleted(jobId), which enqueues a pending
 *     dependency check and runs the dependency processor. The processor then asks
 *     `completedJobs.has(parentId)` -> FALSE (parent was never added, and its row
 *     was deleted), so the child is never promoted out of `waitingDeps`.
 *
 * Effect: the child stays in 'waiting-children' / 'waiting' (waitingDeps) FOREVER
 * and `pull()` returns null. A `removeOnComplete: true` parent silently breaks the
 * dependency graph — a data-availability / liveness bug, no restart required.
 *
 * Reproduction (deterministic, no wall-clock waits):
 *   1. Push parent P with removeOnComplete:true.
 *   2. Push child C with dependsOn:[P.id]  -> goes to waitingDeps.
 *   3. Pull + ack P (completes; because removeOnComplete, P is NOT in completedJobs
 *      and its row is deleted).
 *   4. Drive the dependency processor (await scheduled flush + explicit trigger).
 *   5. ASSERT C is promoted (pull returns C, state leaves waiting-children).  <-- FAILS today.
 *
 * CONTROL: identical flow with removeOnComplete:false -> C IS promoted (passes),
 * proving the orphaning is caused specifically by removeOnComplete skipping
 * completedJobs.add().
 *
 * This RED test asserts the CORRECT post-fix behavior (child must be promoted
 * regardless of the parent's removeOnComplete flag).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { processPendingDependencies } from '../src/application/dependencyProcessor';
import type { JobId } from '../src/domain/types/job';

/**
 * Deterministically flush the (event-driven, microtask-scheduled) dependency
 * processor. We both await ticks so the scheduled microtask flush runs AND call
 * processPendingDependencies directly via the internal BackgroundContext, so the
 * result does not depend on timing.
 */
async function flushDependencies(qm: QueueManager): Promise<void> {
  const qmAny = qm as any;
  // Let the queueMicrotask-scheduled flush (scheduleDependencyFlush) run.
  await Promise.resolve();
  await Bun.sleep(0);
  await Promise.resolve();
  // Explicit direct drive, matching the direct-call repro pattern in bug-75 test.
  const bgCtx = qmAny.contextFactory.getBackgroundContext();
  await processPendingDependencies(bgCtx);
}

describe('Stability: removeOnComplete parent orphans dependent child', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('child is promoted when parent (removeOnComplete:true) completes', async () => {
    // 1. Parent with removeOnComplete:true
    const parent = await qm.push('orphan-q', {
      data: { role: 'parent' },
      removeOnComplete: true,
    });
    expect(parent).not.toBeNull();

    // 2. Child depending on parent -> waitingDeps
    const child = await qm.push('orphan-q', {
      data: { role: 'child' },
      dependsOn: [parent.id],
    });
    expect(child).not.toBeNull();

    // Child must start out blocked (not pullable while parent is pending).
    const childStateBefore = await qm.getJobState(child.id);
    expect(childStateBefore).toBe('waiting-children');

    // 3. Pull + ack the parent (completes). removeOnComplete => parent NOT added
    //    to completedJobs and its row deleted; onJobCompleted still fires.
    const pulledParent = await qm.pull('orphan-q', 0);
    expect(pulledParent).not.toBeNull();
    expect(pulledParent!.id).toBe(parent.id);
    await qm.ack(parent.id, { ok: true });

    // 4. Drive the dependency processor deterministically.
    await flushDependencies(qm);

    // 5. CORRECT post-fix behavior: child must now be runnable.
    //    Today this FAILS — the child stays in waitingDeps forever because
    //    completedJobs.has(parent.id) is false.
    const childStateAfter = await qm.getJobState(child.id);
    expect(childStateAfter).not.toBe('waiting-children');

    const pulledChild = await qm.pull('orphan-q', 0);
    expect(pulledChild).not.toBeNull();
    expect(pulledChild!.id).toBe(child.id);
  });

  test('child pushed AFTER a removeOnComplete parent already completed starts ready (late-dependent ordering)', async () => {
    // Skeptic-identified ordering: the dependent is added AFTER the
    // removeOnComplete parent has already completed. This exercises the
    // push-time readiness check (insertJobToShard), not the live dependency
    // processor. With an eager prune of depCompletions this would orphan the
    // late child; depCompletions must retain the completion for the whole
    // bounded window (same guarantee completedJobs gives a normal parent).
    const parent = await qm.push('orphan-late', {
      data: { role: 'parent' },
      removeOnComplete: true,
    });
    expect(parent).not.toBeNull();

    // Complete the parent BEFORE the child exists.
    const pulledParent = await qm.pull('orphan-late', 0);
    expect(pulledParent!.id).toBe(parent.id);
    await qm.ack(parent.id, { ok: true });
    await flushDependencies(qm); // would prune depCompletions if the eager prune existed

    // Now push the child that depends on the already-completed parent.
    const child = await qm.push('orphan-late', {
      data: { role: 'child' },
      dependsOn: [parent.id],
    });
    expect(child).not.toBeNull();

    // CORRECT behavior: the parent already completed, so the child must NOT be
    // parked in waiting-children — it should be immediately runnable.
    expect(await qm.getJobState(child.id)).not.toBe('waiting-children');
    const pulledChild = await qm.pull('orphan-late', 0);
    expect(pulledChild).not.toBeNull();
    expect(pulledChild!.id).toBe(child.id);
  });

  test('CONTROL: child is promoted when parent has removeOnComplete:false', async () => {
    // Identical flow, only difference is removeOnComplete:false on the parent.
    const parent = await qm.push('orphan-q-ctrl', {
      data: { role: 'parent' },
      removeOnComplete: false,
    });
    const child = await qm.push('orphan-q-ctrl', {
      data: { role: 'child' },
      dependsOn: [parent.id],
    });

    expect(await qm.getJobState(child.id)).toBe('waiting-children');

    const pulledParent = await qm.pull('orphan-q-ctrl', 0);
    expect(pulledParent!.id).toBe(parent.id);
    await qm.ack(parent.id, { ok: true });

    await flushDependencies(qm);

    // With removeOnComplete:false the parent IS added to completedJobs, so the
    // child gets promoted. This control should PASS, isolating the bug to the
    // removeOnComplete code path.
    expect(await qm.getJobState(child.id)).not.toBe('waiting-children');
    const pulledChild = await qm.pull('orphan-q-ctrl', 0);
    expect(pulledChild).not.toBeNull();
    expect(pulledChild!.id).toBe(child.id);
  });
});
