/**
 * Issue #98 - Worker over-PULLS (leases) jobs past `concurrency`.
 *
 * The Issue #96 fix added a re-check before startJob() so the number of jobs
 * EXECUTING never exceeds `concurrency`. But the number of jobs PULLED & LEASED
 * is decided earlier, in doPullBatch():
 *
 *   doPullBatch() -> slots = concurrency - activeJobs   // (A) read BEFORE await
 *                    batchSize = min(batchSize, slots, 1000)
 *                    return await pull(... batchSize ...) // (B) leases on broker
 *
 * `slots` (A) is read before the pull (B), and nothing reserves those slots
 * across the await. When several jobs finish in a burst, multiple
 * finally->poll->tryProcess runs each enter doPullBatch() concurrently, each
 * reads the SAME stale `activeJobs`, and each pulls a full batch. All pulled
 * jobs are registered into pulledJobIds (leased + heartbeated) and parked in the
 * local pendingJobs buffer. So with concurrency=3 the worker can hold 5-6 jobs
 * leased — 3 running + the rest buffered, kept alive by the heartbeat, inflating
 * the broker's `active` count and starving other workers — even though the #96
 * gate correctly caps EXECUTION at 3.
 *
 * `pulledJobIds.size` is exactly the leased count (active + buffered): on
 * completion the job is removed from it (worker.ts finally), and a buffered job
 * stays in it until started or requeued. The post-fix invariant is therefore
 * `pulledJobIds.size <= concurrency` at all times.
 *
 * Deterministic repro (mirrors issue96-concurrency-race.test.ts): wrap
 * doPullBatch with a fixed latency so several tryProcess() runs suspend at the
 * await simultaneously — the REAL pull logic runs unchanged. We sample
 * pulledJobIds.size densely and assert the max never exceeds concurrency.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Issue #98 - Worker over-pull / lease past concurrency', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('leased jobs (pulledJobIds) never exceed concurrency under a fast-completion burst with slow pulls', async () => {
    const CONCURRENCY = 3;
    const TOTAL_JOBS = 40;

    const queue = new Queue('issue98-overpull', { embedded: true });
    queue.obliterate();

    let completed = 0;

    const worker = new Worker(
      'issue98-overpull',
      async () => {
        // Hold the slot so the initial batch completes together, producing the
        // burst of near-simultaneous finally->poll calls that triggers the race.
        await Bun.sleep(20);
        completed++;
        return { ok: true };
      },
      {
        embedded: true,
        concurrency: CONCURRENCY,
        heartbeatInterval: 0,
      }
    );

    const internals = worker as unknown as {
      doPullBatch: (...args: unknown[]) => Promise<unknown>;
      pulledJobIds: Set<string>;
      activeJobs: number;
    };

    // Model a slow round-trip on PULL (the issue's stated trigger) so multiple
    // tryProcess() runs suspend at the await simultaneously. Only latency is
    // added; the real pull logic and tryProcess()/doPullBatch slot math run
    // unchanged. Sample the leased count at resume — the moment a freshly pulled
    // batch lands on top of whatever is already leased.
    const realPull = internals.doPullBatch.bind(worker);
    let maxLeased = 0;
    const sample = (): void => {
      if (internals.pulledJobIds.size > maxLeased) maxLeased = internals.pulledJobIds.size;
    };
    internals.doPullBatch = async (...args: unknown[]) => {
      await Bun.sleep(25);
      const res = await realPull(...args);
      sample();
      return res;
    };

    // Dense sampler covering the whole run, independent of the pull wrapper.
    const sampler = setInterval(sample, 1);

    for (let i = 0; i < TOTAL_JOBS; i++) {
      await queue.add('task', { i });
    }

    const deadline = Date.now() + 15000;
    while (completed < TOTAL_JOBS && Date.now() < deadline) {
      sample();
      await Bun.sleep(20);
    }

    clearInterval(sampler);
    await worker.close();
    queue.close();

    // Sanity: the fix must not lose or stall jobs — everything is processed.
    expect(completed).toBe(TOTAL_JOBS);

    // THE BUG: overlapping pulls lease more than `concurrency` jobs at once
    // (3 running + buffered), so pulledJobIds.size climbs to 5-6. Post-fix the
    // reservation caps the leased count at concurrency.
    expect(maxLeased).toBeLessThanOrEqual(CONCURRENCY);
  }, 30000);

  // Regression guard for the group pull-ahead exception in the #98 fix. A worker
  // whose buffer fills with jobs from a concurrency-limited group must still pull
  // ahead to find jobs from OTHER, runnable groups — the lease cap must not wedge
  // it on a group-blocked buffer. (If the cap counted blocked buffered jobs, the
  // unlimited group's jobs would starve behind the buffered limited-group jobs.)
  test('group-limited buffer does not starve other runnable groups (pull-ahead preserved)', async () => {
    const queue = new Queue('issue98-group-liveness', { embedded: true });
    queue.obliterate();

    let limitedPeak = 0;
    let limitedActive = 0;
    let totalPeak = 0;
    let totalActive = 0;
    let completed = 0;

    const worker = new Worker(
      'issue98-group-liveness',
      async (job) => {
        totalActive++;
        if (totalActive > totalPeak) totalPeak = totalActive;
        const limited = (job.data as { g: string }).g === 'limited';
        if (limited) {
          limitedActive++;
          if (limitedActive > limitedPeak) limitedPeak = limitedActive;
        }
        await Bun.sleep(30);
        if (limited) limitedActive--;
        totalActive--;
        completed++;
        return { ok: true };
      },
      {
        embedded: true,
        concurrency: 4,
        heartbeatInterval: 0,
        limiter: { groupKey: 'g', max: 1, duration: 0 },
      }
    );

    // 6 jobs of the limited group come FIRST in the queue, then 6 unlimited jobs.
    // Without pull-ahead the worker would lease 4 limited jobs, run them 1-at-a-
    // time, and the unlimited jobs would wait behind them.
    for (let i = 0; i < 6; i++) await queue.add('t', { g: 'limited', i });
    for (let i = 0; i < 6; i++) await queue.add('t', { g: 'free', i });

    const deadline = Date.now() + 15000;
    while (completed < 12 && Date.now() < deadline) {
      await Bun.sleep(20);
    }

    await worker.close();
    queue.close();

    // Liveness: every job completes (no deadlock / starvation hang).
    expect(completed).toBe(12);
    // The limited group never exceeds its max of 1.
    expect(limitedPeak).toBeLessThanOrEqual(1);
    // Pull-ahead preserved: unlimited-group jobs ran concurrently alongside the
    // single limited job, so total peak concurrency exceeded 1.
    expect(totalPeak).toBeGreaterThan(1);
  }, 30000);
});
