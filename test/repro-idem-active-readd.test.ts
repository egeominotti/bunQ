/**
 * REPRODUCTION — customId/jobId re-add while the job is ACTIVE (mode: embedded)
 *
 * Run with:
 *   BUNQUEUE_EMBEDDED=1 bun test test/repro-idem-active-readd.test.ts
 *
 * This is the customId/jobId twin of the (fixed) uniqueKey bug #69. Bug #69 made
 * `handleDeduplication` (uniqueKey) idempotent while the prior job is active
 * (popped from the in-memory queue into processingShards). `handleCustomId`
 * (jobId) in src/application/operations/push.ts:58 was NOT given the same fix:
 *
 *   const existingJob = location?.type === 'queue'
 *     ? shard.getQueue(location.queueName).find(existing) : null;
 *   if (existingJob) return { skip: true, existingJob };  // only the queued case
 *   // ...else falls through to the id-REUSE path: createJob(reused id) -> insert
 *
 * When the prior job is ACTIVE, location.type !== 'queue' so existingJob is null,
 * and completedJobs.has(id) is false (active ≠ completed) so the row is NOT
 * evicted. The reuse path then re-inserts the SAME deterministic id whose row is
 * already on disk → "UNIQUE constraint failed: jobs.id":
 *   - durable:true  → the violation is thrown straight back to the add() caller.
 *   - durable:false → the write buffer drops the insert ("constraint violation,
 *     dropped") and swallows it — the idempotent no-op silently became a reject.
 *
 * Correct behavior (BullMQ parity): re-adding an existing jobId in any UNFINISHED
 * state (waiting / active / waiting-children) is an idempotent no-op that returns
 * the existing job. These tests assert that, so they go RED on current code and
 * GREEN once handleCustomId handles the active case.
 *
 * The tests use the low-level manager.pull() to make a job active deterministically
 * (no worker timing). They DO NOT touch src/.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { createJob, jobId } from '../src/domain/types/job';

const DB = join(tmpdir(), `bunq-repro-idem-active-${process.pid}.db`);
function cleanDb() {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${DB}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

describe('REPRO: re-add customId/jobId while job is ACTIVE must be idempotent', () => {
  beforeEach(async () => {
    await shutdownManager(); // reset process-singleton manager → fresh dataPath
    cleanDb();
  });

  afterEach(async () => {
    await shutdownManager();
    cleanDb();
  });

  test('durable re-add while active must NOT throw UNIQUE and must dedup to the same job', async () => {
    const queueName = 'idem-active-durable';
    const q = new Queue(queueName, { embedded: true, dataPath: DB });

    const j1 = await q.add('charge', { v: 1 }, { jobId: 'pay-X', durable: true });

    // Pull it: the job leaves the in-memory priority queue and becomes ACTIVE.
    const manager = getSharedManager();
    const pulled = await manager.pull(queueName);
    expect(pulled).not.toBeNull();
    expect(pulled?.id).toBe(j1.id);

    // Re-add the SAME jobId while j1 is active. Must be an idempotent no-op.
    let threw = '';
    let j2Id = '';
    try {
      const j2 = await q.add('charge', { v: 2 }, { jobId: 'pay-X', durable: true });
      j2Id = j2.id;
    } catch (e) {
      threw = String((e as Error).message);
    }

    expect(threw).toBe(''); // RED: "UNIQUE constraint failed: jobs.id"
    expect(j2Id).toBe(j1.id); // idempotent: same existing job, no duplicate

    const counts = manager.getQueueJobCounts(queueName);
    expect(counts.active).toBe(1); // still exactly the one active job
    expect(counts.waiting).toBe(0); // no duplicate re-queued
  });

  test('buffered (non-durable) re-add while active must dedup, not silently drop a colliding insert', async () => {
    const queueName = 'idem-active-buffered';
    const q = new Queue(queueName, { embedded: true, dataPath: DB });

    const j1 = await q.add('charge', { v: 1 }, { jobId: 'pay-Y' });
    const manager = getSharedManager();
    const pulled = await manager.pull(queueName);
    expect(pulled?.id).toBe(j1.id);

    // Re-add (buffered). On buggy code this reaches the reuse path and the write
    // buffer rejects the colliding insert; assert idempotent dedup semantics hold.
    const j2 = await q.add('charge', { v: 2 }, { jobId: 'pay-Y' });
    expect(j2.id).toBe(j1.id);

    const counts = manager.getQueueJobCounts(queueName);
    expect(counts.active).toBe(1);
    expect(counts.waiting).toBe(0);

    // The original active job must still be ackable (not corrupted by the collision).
    await manager.ack(j1.id);
    const after = manager.getQueueJobCounts(queueName);
    expect(after.completed).toBe(1);
  });
});

describe('REPRO: re-add customId after an ORPHAN disk row must reconcile, not collide', () => {
  beforeEach(async () => {
    await shutdownManager();
    cleanDb();
  });
  afterEach(async () => {
    await shutdownManager();
    cleanDb();
  });

  test('reuse path drops a stale on-disk row so the re-add does not throw UNIQUE', async () => {
    const queueName = 'orphan-q';
    const q = new Queue(queueName, { embedded: true, dataPath: DB });
    const manager = getSharedManager();

    // Simulate the orphan an obliterate()-vs-durable-insert race leaves behind: a
    // persisted `jobs` row whose id === jobId(customId) with NO in-memory tracking
    // (absent from customIdMap / jobIndex / the priority queue). Inject it straight
    // to disk, bypassing all in-memory state.
    const storage = (
      manager as unknown as { storage: { insertJobImmediate: (j: unknown) => void } }
    ).storage;
    expect(storage).toBeTruthy();
    storage.insertJobImmediate(
      createJob(jobId('pay-orphan'), queueName, { data: { stale: true } }, Date.now())
    );

    // Re-add the same custom id durably. handleCustomId finds no mapping → reuse
    // path. Pre-fix it createJob()+inserts the same id → UNIQUE collision on the
    // surviving orphan row; the fix deletes the stale row first.
    let threw = '';
    let id = '';
    try {
      const j = await q.add('charge', { fresh: true }, { jobId: 'pay-orphan', durable: true });
      id = j.id;
    } catch (e) {
      threw = String((e as Error).message);
    }

    expect(threw).toBe(''); // RED before fix: "UNIQUE constraint failed: jobs.id"
    expect(id).toBe('pay-orphan');

    const counts = manager.getQueueJobCounts(queueName);
    expect(counts.waiting).toBe(1); // exactly the fresh job, no stale ghost left behind
  });
});
