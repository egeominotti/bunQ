/**
 * ISSUE #100: Queue control-state (paused / rate-limit / concurrency) is never
 * persisted — it lives only in `LimiterManager`'s in-memory `queueState` Map, so
 * ANY server restart silently resets it. A queue an operator deliberately PAUSED
 * resumes itself and starts processing again after a restart, with no error and
 * no warning. Rate-limit and concurrency overrides reset to defaults the same way.
 *
 * The schema even declares a `queue_state` table for exactly this — but it was
 * dead: zero INSERT/UPDATE/SELECT against it anywhere.
 *
 * Root cause:
 *   1. LimiterManager.pause/resume/setRateLimit/setConcurrency mutate only the
 *      in-memory Map.
 *   2. backgroundTasks.recover() restores jobs + DLQ on boot but never loads
 *      queue_state → the Map starts empty → isPaused() returns false for every
 *      queue.
 *
 * CORRECT (post-fix) behavior: control operations write-through to the
 * `queue_state` table (UPSERT), and recover() loads it on boot — so a paused
 * queue is still paused, and its rate/concurrency caps are intact, after restart.
 *
 * Deterministic repro (no docker): construct a QueueManager on a real SQLite
 * file, set control-state + enqueue a durable job, shut it down, then construct a
 * FRESH QueueManager on the SAME data dir (its constructor runs recover()).
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';

describe('Issue #100: queue control-state must survive a server restart', () => {
  const TEST_DB = `/tmp/bunq-issue100-${Date.now()}-${process.pid}.db`;

  afterEach(() => {
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        rmSync(TEST_DB + suffix);
      } catch {
        /* ignore */
      }
    }
  });

  test('paused + rateLimit + concurrency are restored from SQLite on boot', async () => {
    const QUEUE = 'qstate';

    // ---- phase 1: configure control-state + a durable job, then "stop" ----
    const qm1 = new QueueManager({ dataPath: TEST_DB });
    const j = await qm1.push(QUEUE, { data: { n: 1 }, durable: true });
    qm1.pause(QUEUE);
    qm1.setRateLimit(QUEUE, 42);
    qm1.setConcurrency(QUEUE, 7);
    expect(qm1.isPaused(QUEUE)).toBe(true);
    expect(qm1.getQueueLimits(QUEUE)).toEqual({ rateLimit: 42, concurrencyLimit: 7 });
    qm1.shutdown();

    // ---- phase 2: fresh process on the same data dir → recover() runs ----
    const qm2 = new QueueManager({ dataPath: TEST_DB });
    try {
      // The job survived the restart (persistence control).
      const survivedState = await qm2.getJobState(j.id);
      expect(['waiting', 'paused']).toContain(survivedState);
      expect(qm2.listQueues()).toContain(QUEUE);

      // ===== CORRECT post-fix behavior =====
      // PRE-FIX: paused resets to false; limits reset to null.
      expect(qm2.isPaused(QUEUE)).toBe(true);
      expect(qm2.getQueueLimits(QUEUE)).toEqual({ rateLimit: 42, concurrencyLimit: 7 });
    } finally {
      qm2.shutdown();
    }
  });

  test('resume + clear* are also persisted (a resumed queue stays resumed)', async () => {
    const QUEUE = 'qstate-resume';

    const qm1 = new QueueManager({ dataPath: TEST_DB });
    qm1.pause(QUEUE);
    qm1.setRateLimit(QUEUE, 10);
    qm1.setConcurrency(QUEUE, 3);
    // Now undo all of it — the cleared/resumed state must persist too.
    qm1.resume(QUEUE);
    qm1.clearRateLimit(QUEUE);
    qm1.clearConcurrency(QUEUE);
    expect(qm1.isPaused(QUEUE)).toBe(false);
    qm1.shutdown();

    const qm2 = new QueueManager({ dataPath: TEST_DB });
    try {
      expect(qm2.isPaused(QUEUE)).toBe(false);
      expect(qm2.getQueueLimits(QUEUE)).toEqual({ rateLimit: null, concurrencyLimit: null });
    } finally {
      qm2.shutdown();
    }
  });

  test('obliterate clears the persisted control-state row', async () => {
    const QUEUE = 'qstate-obliterate';

    const qm1 = new QueueManager({ dataPath: TEST_DB });
    qm1.pause(QUEUE);
    qm1.shutdown();

    const qm2 = new QueueManager({ dataPath: TEST_DB });
    expect(qm2.isPaused(QUEUE)).toBe(true);
    qm2.obliterate(QUEUE);
    qm2.shutdown();

    // After obliterate the persisted row is gone → no stale pause resurrects.
    const qm3 = new QueueManager({ dataPath: TEST_DB });
    try {
      expect(qm3.isPaused(QUEUE)).toBe(false);
    } finally {
      qm3.shutdown();
    }
  });
});
