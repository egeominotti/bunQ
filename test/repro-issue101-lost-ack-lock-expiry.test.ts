/**
 * ISSUE #101: A successful job completion is silently LOST when the lock token
 * expired WHILE the job was being processed — the job stalls to `failed`
 * instead of `completed`.
 *
 * Prod scenario: a half-open TCP storm makes the worker rebuild on a fresh
 * connection (or `lockDuration` simply elapses without renewal). The handler
 * still finishes successfully, but the completion ACK carries the now-dead lock
 * token. The server rejects it (`Invalid or expired lock token`), the client
 * AckBatcher burns its transient retries against this PERMANENT error and drops
 * the completion. The job is never marked `completed`; it is re-pulled,
 * re-processed, and eventually lands in `failed` via stall — despite having been
 * processed correctly every time. (Same lock-expiry family as #97, distinct
 * symptom: there the handler hangs; here it completes and the ACK is lost.)
 *
 * Root cause (server-side): `QueueManager.ackBatchWithResults` verifies the lock
 * with `verifyLock`, which rejects an EXPIRED lock even when its token still
 * matches the presenting worker and the job is still in `processing` (nobody
 * re-leased it). `throwIfOwnershipConflict` then throws — treating "my own lock
 * just expired" the same as "someone else owns this job".
 *
 * CORRECT (post-fix) behavior: a GRACE WINDOW — if the lock entry still belongs
 * to this worker (token matches) and the job is still in `processing` (not
 * re-leased), the completion is accepted and the job is marked `completed`.
 * Token-match guarantees no double-completion: a genuine re-lease replaces the
 * token, so a stale worker's completion is still rejected.
 *
 * Deterministic repro (no background timers / no TCP / no socket drop): pull
 * with a short lock TTL that is never renewed, sleep past the TTL WITHOUT running
 * the lock-expiry sweep (the job stays in `processing` under our own expired
 * lock), then ack.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';
import { checkExpiredLocks } from '../src/application/lockManager';
import { checkStalledJobs } from '../src/application/stallDetection';

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

describe('Issue #101: a successful completion must not be lost on lock expiry', () => {
  const TEST_DB = `/tmp/bunq-issue101-${Date.now()}-${process.pid}.db`;
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

  test('expired-but-still-ours lock: completion is accepted, job ends completed', async () => {
    const QUEUE = 'lost-ack';

    const job = await qm.push(QUEUE, { data: { n: 1 }, maxAttempts: 50, durable: true });

    // Pull with a short lock TTL that is never renewed → mimics `lockDuration`
    // elapsing while the handler is still running.
    const pulled = await qm.pullWithLock(QUEUE, 'worker-1', 0, 50);
    expect(pulled.job).not.toBeNull();
    expect(pulled.token).not.toBeNull();
    expect(await qm.getJobState(job.id)).toBe('active');

    // The handler "finishes" only AFTER the lock has expired. We deliberately do
    // NOT run checkExpiredLocks(): the job is still in `processing` under our own
    // (now-expired) lock — nobody re-leased it. This is the prod race; the ack
    // carries a token the server now considers expired.
    await Bun.sleep(80);

    // The completion ack. PRE-FIX: throws "Invalid or expired lock token" and the
    // successful work is lost → job eventually stalls to `failed`.
    let threw: string | null = null;
    try {
      await qm.ackBatchWithResults([{ id: job.id, result: { ok: true }, token: pulled.token! }]);
    } catch (err) {
      threw = (err as Error).message;
    }

    // ===== CORRECT post-fix behavior: grace window accepts the completion =====
    expect(threw).toBeNull();
    expect(await qm.getJobState(job.id)).toBe('completed');
    expect(qm.getResult(job.id)).toEqual({ ok: true });
  });

  test('genuinely re-leased job: the stale worker\'s completion is rejected (no double-complete)', async () => {
    const QUEUE = 'lost-ack-conflict';
    const job = await qm.push(QUEUE, { data: { n: 2 }, maxAttempts: 50, durable: true });

    // Worker A pulls with a short TTL, then its lock expires.
    const a = await qm.pullWithLock(QUEUE, 'worker-A', 0, 50);
    expect(a.token).not.toBeNull();
    await Bun.sleep(80);

    // The lock-expiry sweep requeues the job (default maxStalls > 1 → retry, not
    // DLQ); worker B then re-leases it with a FRESH token.
    await checkExpiredLocks(getInternalLockContext(qm));
    const b = await qm.pullWithLock(QUEUE, 'worker-B', 0, 5000);
    expect(b.job).not.toBeNull();
    expect(b.token).not.toBeNull();
    expect(b.token).not.toBe(a.token);

    // Worker A's stale completion must be REJECTED — B now owns the job. A must
    // NOT be able to complete it (token no longer matches the active lock).
    let aThrew: string | null = null;
    try {
      await qm.ackBatchWithResults([{ id: job.id, result: { from: 'A' }, token: a.token! }]);
    } catch (err) {
      aThrew = (err as Error).message;
    }
    expect(aThrew).toContain('Invalid or expired lock token');
    expect(await qm.getJobState(job.id)).not.toBe('completed');

    // B's completion succeeds and wins.
    await qm.ackBatchWithResults([{ id: job.id, result: { from: 'B' }, token: b.token! }]);
    expect(await qm.getJobState(job.id)).toBe('completed');
    expect(qm.getResult(job.id)).toEqual({ from: 'B' });
  });

  // Regression for the BLOCKING vector found in skeptic review. The stall path
  // (stallDetection retry/moveToDlq) requeues a job WITHOUT deleting the lock —
  // by design: the lingering lock preserves the original owner's recovery path
  // and the Worker-level activeJobIds dedup prevents re-processing in a single
  // process. But if another worker re-pulls the job (createLock returns null →
  // null token, but the job's `startedAt` is reset to a newer time), worker A's
  // EXPIRED, lingering lock still token-matches. Without the re-lease guard the
  // grace window would accept A's late ack and DOUBLE-COMPLETE the job that B
  // now holds. The guard compares lock.createdAt vs job.startedAt and rejects A.
  test('stall-retried + re-pulled: expired lingering lock must NOT double-complete (re-lease guard)', async () => {
    const QUEUE = 'stall-regrace';
    // Fast, deterministic stall: tiny interval, no grace period, retry (not DLQ).
    qm.setStallConfig(QUEUE, {
      enabled: true,
      stallInterval: 30,
      maxStalls: 5,
      gracePeriod: 0,
    });

    const job = await qm.push(QUEUE, {
      data: { n: 1 },
      maxAttempts: 50,
      backoff: 0,
      durable: true,
    });

    // Worker A pulls with a SHORT lock TTL so its lock EXPIRES — this forces the
    // ack through the #101 grace path (verifyLock fails on expiry) rather than a
    // normal valid-lock ack.
    const a = await qm.pullWithLock(QUEUE, 'worker-A', 0, 50);
    expect(a.token).not.toBeNull();

    // Lock expires AND the job looks stalled (no heartbeat past stallInterval).
    await Bun.sleep(80);

    // Drive the two-phase stall detector → retryStalliedJob requeues the job.
    // The lingering (now-expired) lock is intentionally NOT deleted here.
    const bg = (qm as any).contextFactory.getBackgroundContext();
    checkStalledJobs(bg); // phase 2: mark candidate
    checkStalledJobs(bg); // phase 1: confirm → retry
    await Bun.sleep(40);

    let state = await qm.getJobState(job.id);
    if (state === 'delayed') {
      await qm.promote(job.id);
      state = await qm.getJobState(job.id);
    }
    expect(state).toBe('waiting');

    // Worker B re-leases the SAME job. createLock returns null (A's lock lingers)
    // → B gets a null token, but B now owns the *current* processing instance
    // (the job's startedAt is reset to B's pull time).
    const b = await qm.pullWithLock(QUEUE, 'worker-B', 0, 10000);
    expect(b.job).not.toBeNull();
    expect(String(b.job!.id)).toBe(String(job.id));

    // Worker A's late completion (its expired, lingering token) must be REJECTED:
    // the job was re-pulled, so A no longer owns the current instance. PRE-GUARD
    // the grace window accepted it and completed with A's result.
    let aThrew: string | null = null;
    try {
      await qm.ackBatchWithResults([{ id: job.id, result: { from: 'A' }, token: a.token! }]);
    } catch (err) {
      aThrew = (err as Error).message;
    }
    expect(aThrew).toContain('Invalid or expired lock token');
    // The job must not have been completed with A's stale result.
    const finalState = await qm.getJobState(job.id);
    if (finalState === 'completed') {
      expect(qm.getResult(job.id)).not.toEqual({ from: 'A' });
    }
  });
});
