/**
 * Issue #96 - Worker exceeds the concurrency limit (check-then-await-then-increment race)
 *
 * The concurrency gate lives only in poll():
 *
 *   poll()       -> if (activeJobs >= concurrency) reschedule; else tryProcess()
 *   tryProcess() -> item = buffered ?? await doPullBatch(); if (item) startJob(item)
 *   startJob()   -> activeJobs++          // THE increment, separated from the gate
 *
 * Two things break the gate's guarantee:
 *   1. `await doPullBatch()` (a network round-trip in TCP mode) sits between the
 *      gate and the increment. Several finally->poll->tryProcess runs can all pass
 *      the gate while activeJobs is still low, all suspend at the pull await, and
 *      each calls startJob() on resume -> activeJobs climbs past `concurrency`.
 *   2. startJob() schedules tryProcess() via setImmediate, BYPASSING poll()'s gate
 *      entirely; tryProcess() then starts a buffered job with no concurrency check.
 *
 * Real-world trigger (from the report): concurrency=3, a DLQ retry produces a burst
 * of jobs that complete in ~0ms, and a slow network widens the pull window — the
 * user observed 10 jobs in flight against a limit of 3.
 *
 * We reproduce deterministically by modelling the slow PULL: doPullBatch is wrapped
 * with a fixed latency so multiple tryProcess() runs are suspended at the await at
 * once. The REAL pull logic still runs; tryProcess()/startJob() — where the bug
 * lives — are completely unmodified.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Issue #96 - Worker concurrency overshoot race', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('observed concurrency never exceeds the configured limit under a fast-completion burst with slow pulls', async () => {
    const CONCURRENCY = 3;
    const TOTAL_JOBS = 30;

    const queue = new Queue('issue96-race', { embedded: true });
    queue.obliterate();

    let active = 0;
    let maxActive = 0;
    let completed = 0;

    const worker = new Worker(
      'issue96-race',
      async () => {
        active++;
        if (active > maxActive) maxActive = active;
        // Hold the slot briefly so the initial batch completes together, producing
        // the burst of near-simultaneous finally->poll calls that triggers the race.
        await Bun.sleep(20);
        active--;
        completed++;
        return { ok: true };
      },
      {
        embedded: true,
        concurrency: CONCURRENCY,
        heartbeatInterval: 0,
      }
    );

    // Model a slow network round-trip on PULL (the issue's stated trigger). Keep the
    // REAL pull logic and only add latency, so multiple tryProcess() runs suspend at
    // the await simultaneously. tryProcess()/startJob() are unchanged.
    const internals = worker as unknown as {
      doPullBatch: (...args: unknown[]) => Promise<unknown>;
    };
    const realPull = internals.doPullBatch.bind(worker);
    internals.doPullBatch = async (...args: unknown[]) => {
      await Bun.sleep(25);
      return realPull(...args);
    };

    // Seed a burst of fast-completing jobs.
    for (let i = 0; i < TOTAL_JOBS; i++) {
      await queue.add('task', { i });
    }

    // Drain.
    const deadline = Date.now() + 10000;
    while (completed < TOTAL_JOBS && Date.now() < deadline) {
      await Bun.sleep(50);
    }

    await worker.close();
    queue.close();

    // Sanity: the fix must not lose or stall jobs — everything is processed.
    expect(completed).toBe(TOTAL_JOBS);

    // THE BUG: with the race, maxActive climbs past CONCURRENCY (user saw 10 vs 3).
    expect(maxActive).toBeLessThanOrEqual(CONCURRENCY);
  }, 25000);
});
