/**
 * STABILITY BUG: moveToWaitingChildren() strands the job.
 *
 * Recipe: pull a job (state: active/processing), then call
 * QueueManager.moveToWaitingChildren(id). The job is now unreachable and
 * uncancellable — there is no success-drain path back to the queue.
 *
 * Root cause (two interacting defects):
 *
 * 1. moveToWaitingChildren  (src/application/operations/jobStateTransitions.ts:81-111)
 *    Deletes the job from the processing shard, stores it in
 *    `shard.waitingChildren`, and sets jobIndex to
 *    `{ type: 'queue', shardIdx, queueName }`. But it does NOT push the job
 *    into `shard.getQueue()` and does NOT call `incrementQueued`. The job
 *    lives ONLY in the `waitingChildren` map while jobIndex claims it is in
 *    the queue. There is no success-drain path: the children-success drain
 *    reads `waitingDeps`, never `waitingChildren`.
 *
 * 2. getJob  (src/application/operations/queryOperations.ts:44-52)
 *    For `type: 'queue'` it reads only
 *    `shard.getQueue().find(id) ?? shard.waitingDeps.get(id)` — it never
 *    consults `shard.waitingChildren`. So getJob returns null even though the
 *    job object still exists in memory.
 *
 *    getJobState  (queryOperations.ts:158-176) DOES consult
 *    `shard.waitingChildren` (line 167-168) and reports 'waiting-children'.
 *    => Query inconsistency: getJobState says 'waiting-children' but getJob
 *       says the job does not exist.
 *
 * 3. cancelJob  (src/application/operations/jobManagement.ts:30-62)
 *    For `type: 'queue'` it only does `shard.getQueue().remove(id)`. The job
 *    isn't in the queue (it's in waitingChildren), so remove() misses, the
 *    operation reports failure, and cancel() returns false.
 *    => The job is permanently stranded: not in the queue, not in processing,
 *       cannot be fetched, cannot be cancelled, and has no completion path.
 *
 * Both assertions below encode the CORRECT post-fix behavior, so they FAIL on
 * the current (buggy) code — that failure is the proof the bug is real.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('STABILITY: moveToWaitingChildren strands the job (query inconsistency + uncancellable)', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('getJob/getJobState agree AND the job is recoverable after moveToWaitingChildren', async () => {
    // 1. Push a job and pull it with a lock so it becomes active/processing.
    const pushed = await qm.push('parent-q', { data: { kind: 'parent' } });
    expect(pushed).not.toBeNull();
    const id = pushed.id;

    const { job, token } = await qm.pullWithLock('parent-q', 'worker-1', 0, 60_000);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();
    expect(job!.id).toBe(id);
    expect(await qm.getJobState(id)).toBe('active');

    // 2. Move the active job to waiting-children.
    const moved = await qm.moveToWaitingChildren(id);
    expect(moved).toBe(true);

    // The job is now reported as waiting-children by getJobState.
    const state = await qm.getJobState(id);
    expect(state).toBe('waiting-children');

    // ── Assertion 1: query consistency ────────────────────────────────────
    // getJobState says 'waiting-children', so the job still exists. getJob
    // MUST therefore return the job (non-null). Today getJob returns null
    // because it never looks in shard.waitingChildren → this FAILS (RED).
    const fetched = await qm.getJob(id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(id);

    // ── Assertion 2: the job is not stranded ─────────────────────────────
    // A waiting-children job must remain controllable: cancelling it must
    // succeed (return true) so it can be removed. Today cancel() returns
    // false because cancelJob only looks in the queue, not in
    // waitingChildren → this FAILS (RED), proving the job is uncancellable
    // and permanently stranded.
    const cancelled = await qm.cancel(id);
    expect(cancelled).toBe(true);
  });
});
