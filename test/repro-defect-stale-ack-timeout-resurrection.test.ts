/**
 * REPRO — slug: stale-ack-timeout-resurrection
 *
 * DEFECT: a job with a per-job `timeout` and `attempts > 1` can be
 * phantom-completed by a stale ACK from the timed-out worker, silently
 * overriding the timeout decision and SKIPPING the retry.
 *
 * Sequence:
 *   1. Worker pulls a job (processing, lock token T1, startedAt set).
 *   2. Processor runs past the per-job timeout.
 *   3. The timeout sweep (backgroundTasks.checkJobTimeouts, every jobTimeoutCheckMs)
 *      calls ctx.fail(jobId, 'Job timeout exceeded') WITHOUT a token -> the job is
 *      requeued for retry (attempts++). The T1 lock is NOT cleared (fail had no token).
 *   4. The still-hung worker finally returns and ACKs with T1. In queueManager.ack,
 *      verifyLock(T1) still passes (lock present) OR fails -> the recovery path
 *      (completeStallRetriedJob, Issue #33) COMPLETES the job — even though it was
 *      explicitly timed out and is mid-retry. Phantom completion: state becomes
 *      'completed', the retry never runs, result is the late one.
 *
 * Root cause: the ACK recovery path (queueManager.ts) cannot distinguish a
 * stall-retried job (complete-to-dedup is correct) from a TIMEOUT-failed job
 * (must NOT complete; retry must win) — isStallRetried() is true for both
 * (attempts > 0 in queue).
 *
 * FIX: the timeout sweep records the job id in `timedOutJobs`; the ACK recovery
 * path discards a stale ACK for such a job (graceful no-op) so the retry proceeds.
 * A legitimate ACK of the retry attempt carries a valid current token and bypasses
 * the stale-token recovery path entirely, so it still completes normally.
 *
 * This RED test asserts the CORRECT post-fix behavior: after a timeout requeue,
 * a stale ACK must NOT complete the job. RED today: state === 'completed'.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { checkJobTimeouts } from '../src/application/backgroundTasks';
import type { BackgroundContext } from '../src/application/types';
import { jobId, type JobId } from '../src/domain/types/job';

function bgCtx(qm: QueueManager): BackgroundContext {
  return (
    qm as unknown as { contextFactory: { getBackgroundContext(): BackgroundContext } }
  ).contextFactory.getBackgroundContext();
}

describe('Stability: stale ACK after timeout must not phantom-complete a retrying job', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('a late ACK from the timed-out worker is discarded; the job retries (not completed)', async () => {
    const QUEUE = 'timeout-q';

    // Job with a short timeout and 2 attempts (so a timeout requeues for retry,
    // not straight to DLQ).
    const pushed = await qm.push(QUEUE, { data: { n: 1 }, timeout: 40, attempts: 2 });
    expect(pushed).not.toBeNull();

    // Worker pulls with a lock (default behavior). startedAt is set now.
    const { job, token } = await qm.pullWithLock(QUEUE, 'worker-1', 0, 30000);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();
    expect(await qm.getJobState(job!.id)).toBe('active');

    // Processor runs past the timeout.
    await Bun.sleep(80);

    // Timeout sweep fires: marks the job timed-out + ctx.fail -> requeue for retry.
    await checkJobTimeouts(bgCtx(qm));

    // The job must now be requeued for retry (delayed by backoff / waiting),
    // NOT completed.
    const stateAfterTimeout = await qm.getJobState(job!.id);
    expect(stateAfterTimeout).not.toBe('completed');

    // The still-hung worker finally returns and ACKs with its (now stale) token.
    // Pre-fix: this phantom-completes the job, skipping the retry.
    await qm.ack(job!.id, { lateResult: true }, token!);

    // CORRECT post-fix behavior: the late ACK is discarded; the job is still
    // retrying (not completed). RED today: getJobState === 'completed'.
    const finalState = await qm.getJobState(job!.id);
    expect(finalState).not.toBe('completed');
  });

  test('reusing a custom id clears the stale timeout marker (closes the #33/#75 reuse hole)', async () => {
    // Skeptic-identified hole: the timeout marker is never cleared, so a recycled
    // custom id (idempotency key) carrying a stale marker would wrongly discard
    // the NEW job's legitimate stall-retry completion — reintroducing duplicate
    // execution. Custom ids ARE the internal JobId, so the marker must be cleared
    // when an id is recycled (the timeout sweep is the only writer and never
    // removes; clearing rides the existing custom-id reuse scrub in push.ts).
    const QUEUE = 'timeout-reuse-q';
    const CID = 'idemp-key-1';
    const timedOut = (
      qm as unknown as {
        timedOutJobs: {
          has(k: JobId): boolean;
          set(k: JobId, value: { jobId: JobId; startedAt: number; token?: string }): void;
        };
      }
    ).timedOutJobs;

    // Simulate the stale marker left behind by a prior job (with this custom id)
    // that timed out and reached a terminal state.
    timedOut.set(jobId(CID), { jobId: jobId(CID), startedAt: Date.now() });
    expect(timedOut.has(jobId(CID))).toBe(true);

    // Push a job that recycles the custom id — this goes through the reuse path
    // in handleCustomId.
    const j = await qm.push(QUEUE, { customId: CID, data: { v: 2 }, attempts: 3 });
    expect(j.id).toBe(jobId(CID));

    // CORRECT post-fix behavior: recycling the id clears the stale marker so the
    // new job's recovery path is not poisoned. RED without the reuse-clear: true.
    expect(timedOut.has(jobId(CID))).toBe(false);

    // And the recycled job behaves normally end-to-end (legit ack completes it).
    const { job, token } = await qm.pullWithLock(QUEUE, 'w', 0, 30000);
    expect(job!.id).toBe(jobId(CID));
    await qm.ack(job!.id, { ok: true }, token!);
    expect(await qm.getJobState(jobId(CID))).toBe('completed');
  });

  test('CONTROL: a legit ACK of a normally-pulled job (no timeout) still completes', async () => {
    const QUEUE = 'timeout-q-ctrl';
    const pushed = await qm.push(QUEUE, { data: { n: 2 }, attempts: 2 });
    const { job, token } = await qm.pullWithLock(QUEUE, 'worker-1', 0, 30000);
    expect(job!.id).toBe(pushed.id);

    // No timeout, no sweep — a normal ack must complete the job.
    await qm.ack(job!.id, { ok: true }, token!);
    expect(await qm.getJobState(job!.id)).toBe('completed');
  });
});
