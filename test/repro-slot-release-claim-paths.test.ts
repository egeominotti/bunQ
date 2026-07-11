/**
 * SUSPECTED BUG (skeptic review of the 2.8.31 pull fix): concurrency-slot leak
 * when an ACTIVE job is CLAIMED by a management op instead of finishing via
 * ack/fail/lock-expiry.
 *
 * At pull time, tryPullFromShard (src/application/operations/pull.ts:268)
 * calls shard.tryAcquireConcurrency(queue), incrementing the
 * ConcurrencyLimiter's `active` counter (src/domain/types/queue.ts:72). That
 * slot is only returned by shard.releaseConcurrency / releaseJobResources.
 *
 * Release-on-exit audit of the "claim an active job" paths:
 *   - moveActiveToWait        (jobStateTransitions.ts:40)  -> releases  ✔
 *   - moveToWaitingChildren   (jobStateTransitions.ts:108) -> releases  ✔
 *   - ack/fail                (ack.ts:93/230)              -> releases  ✔
 *   - lock expiry / stalls    (lockManager.ts:125/172/214) -> releases  ✔
 *   - moveJobToDelayed        (jobManagement.ts:227-280)   -> NO release ✘
 *   - discardJob (processing) (jobManagement.ts:299-306)   -> NO release ✘
 *
 * There is no lazy reconciliation elsewhere (cleanupTasks/backgroundTasks
 * never touch the ConcurrencyLimiter), so each leaked slot is permanent:
 * with setConcurrency(N), N claims wedge the queue forever.
 *
 * Repro (deterministic, no background timers):
 *   setConcurrency(q, 2); push 4; pull 2 (slots exhausted); claim BOTH active
 *   jobs via the suspect op. Now 0 jobs are processing and 2 are waiting, so
 *   the queue MUST be able to dispatch 2 more jobs. Pre-fix it dispatches 0.
 *
 * RateLimiter is different by design: tokens are never "released" by ANY path
 * (ack included) — they refill continuously at `limit` tokens/sec
 * (src/domain/types/queue.ts:50-56). A token consumed for a claimed job is
 * therefore not a permanent leak; the last test documents the self-heal.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';

/** Read the ConcurrencyLimiter's active count via internal poking. */
function getActiveConcurrency(qm: QueueManager, queue: string): number | null {
  const shard = qm.getShards()[shardIndex(queue)] as unknown as {
    limiterManager?: {
      concurrencyLimiters?: Map<string, { getActive(): number }>;
    };
  };
  const limiter = shard.limiterManager?.concurrencyLimiters?.get(queue);
  return limiter ? limiter.getActive() : null;
}

describe('claim paths must release the queue concurrency slot acquired at pull', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  async function setupTwoActiveTwoWaiting(queue: string) {
    qm.setConcurrency(queue, 2);
    for (let i = 0; i < 4; i++) {
      await qm.push(queue, { data: { i } });
    }
    const a = await qm.pull(queue, 0);
    const b = await qm.pull(queue, 0);
    expect(a).not.toBeNull();
    expect(b).not.toBeNull();
    // Slots exhausted: the gate correctly blocks a 3rd pull.
    expect(getActiveConcurrency(qm, queue)).toBe(2);
    expect(await qm.pull(queue, 0)).toBeNull();
    return [a!, b!] as const;
  }

  test('moveToDelayed on both active jobs frees both slots (queue not wedged)', async () => {
    const QUEUE = 'claim-delay';
    const [a, b] = await setupTwoActiveTwoWaiting(QUEUE);

    // Claim both active jobs: QueueManager.moveToDelayed routes processing
    // jobs to jobMgmt.moveJobToDelayed (queueManager.ts:1191).
    expect(await qm.moveToDelayed(a.id, 60_000)).toBe(true);
    expect(await qm.moveToDelayed(b.id, 60_000)).toBe(true);

    // Sanity: nothing is processing anymore; both jobs are back in the queue
    // as delayed, and 2 fresh jobs are still waiting.
    expect(await qm.getJobState(a.id)).toBe('delayed');
    expect(await qm.getJobState(b.id)).toBe('delayed');

    // ===== CORRECT post-fix behavior =====
    // Zero jobs are active, so both slots must be free again.
    expect(getActiveConcurrency(qm, QUEUE)).toBe(0); // FAILS NOW: stays 2 (leak)

    // With 2 jobs waiting, 0 processing, and limit 2, both must be pullable.
    const after1 = await qm.pull(QUEUE, 0);
    const after2 = await qm.pull(QUEUE, 0);
    expect(after1).not.toBeNull(); // FAILS NOW: leaked slots keep the gate full
    expect(after2).not.toBeNull();
  });

  test('discard on both active jobs frees both slots (queue not wedged)', async () => {
    const QUEUE = 'claim-discard';
    const [a, b] = await setupTwoActiveTwoWaiting(QUEUE);

    // Claim both active jobs to the DLQ: QueueManager.discard routes to
    // jobMgmt.discardJob, whose processing branch (jobManagement.ts:299-306)
    // deletes from processingShards.
    expect(await qm.discard(a.id)).toBe(true);
    expect(await qm.discard(b.id)).toBe(true);

    // ===== CORRECT post-fix behavior =====
    expect(getActiveConcurrency(qm, QUEUE)).toBe(0); // FAILS NOW: stays 2 (leak)

    const after1 = await qm.pull(QUEUE, 0);
    const after2 = await qm.pull(QUEUE, 0);
    expect(after1).not.toBeNull(); // FAILS NOW: leaked slots keep the gate full
    expect(after2).not.toBeNull();
  });

  test('CONTROL: moveActiveToWait releases the slots (proves the asymmetry)', async () => {
    const QUEUE = 'claim-wait-ctrl';
    const [a, b] = await setupTwoActiveTwoWaiting(QUEUE);

    // Sibling op with correct release-on-exit (jobStateTransitions.ts:40).
    expect(await qm.moveActiveToWait(a.id)).toBe(true);
    expect(await qm.moveActiveToWait(b.id)).toBe(true);

    // Slots came back; the queue keeps dispatching. GREEN today.
    expect(getActiveConcurrency(qm, QUEUE)).toBe(0);
    const after1 = await qm.pull(QUEUE, 0);
    const after2 = await qm.pull(QUEUE, 0);
    expect(after1).not.toBeNull();
    expect(after2).not.toBeNull();
  });

  test('BONUS: discard of a WAITING job releases its uniqueKey (parity with cancelJob)', async () => {
    const QUEUE = 'claim-discard-waiting-uk';
    // A waiting job holding a uniqueKey reservation (never pulled, so it holds
    // no concurrency slot and no group).
    const first = await qm.push(QUEUE, { data: { n: 1 }, uniqueKey: 'uk-discard' });
    expect(await qm.getJobState(first.id)).toBe('waiting');

    // Discard the WAITING job to the DLQ: the queue branch of discardJob
    // (jobManagement.ts:289-298). cancelJob, the sibling remove-from-queue
    // path, releases the uniqueKey (jobManagement.ts:41); and every fail-to-DLQ
    // path frees the reservation on DLQ entry (ack.ts:230, lockManager.ts:172).
    expect(await qm.discard(first.id)).toBe(true);

    // ===== CORRECT post-fix behavior =====
    // Re-adding the same uniqueKey must be ACCEPTED as a brand-new job, not
    // deduped against the DLQ'd job (whose jobIndex entry is type 'dlq', which
    // handleDeduplication treats as "still live", push.ts:188).
    const second = await qm.push(QUEUE, { data: { n: 2 }, uniqueKey: 'uk-discard' });
    expect(second.id).not.toBe(first.id); // FAILS NOW: dedup returns the DLQ'd id
    expect(await qm.getJobState(second.id)).toBe('waiting');

    // And the new job is actually dispatchable.
    const pulled = await qm.pull(QUEUE, 0);
    expect(pulled).not.toBeNull();
    expect(pulled?.id).toBe(second.id);
  });

  test('INFO: rate-limit tokens consumed for claimed jobs self-heal via refill (not a permanent leak)', async () => {
    const QUEUE = 'claim-rate';
    // Rate limit 2 tokens/sec, no concurrency limit.
    qm.setRateLimit(QUEUE, 2);
    for (let i = 0; i < 4; i++) {
      await qm.push(QUEUE, { data: { i } });
    }

    // Consume both tokens.
    const a = await qm.pull(QUEUE, 0);
    const b = await qm.pull(QUEUE, 0);
    expect(a).not.toBeNull();
    expect(b).not.toBeNull();
    // Bucket empty right now.
    expect(await qm.pull(QUEUE, 0)).toBeNull();

    // Claim both active jobs (the suspect path).
    expect(await qm.moveToDelayed(a!.id, 60_000)).toBe(true);
    expect(await qm.moveToDelayed(b!.id, 60_000)).toBe(true);

    // Tokens refill continuously at 2/sec regardless of any release call, so
    // within ~1s the queue can dispatch the waiting jobs again. Poll with a
    // 3s deadline (no blind sleep).
    let pulled: unknown = null;
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && pulled === null) {
      pulled = await qm.pull(QUEUE, 0);
      if (pulled === null) await Bun.sleep(50);
    }
    expect(pulled).not.toBeNull(); // GREEN: token bucket refilled (by design)
  });
});
