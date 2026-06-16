/**
 * Worker over-pulls jobs past `concurrency` (server `active` > concurrency)
 *
 * Follow-up to Issue #96. The #96 fix added a concurrency re-check before
 * startJob() — guarding EXECUTION — so the number of running processors is
 * correctly capped. But doPullBatch() still computes free slots from a stale
 * `activeJobs` across the pull `await`, with no reservation. Overlapping
 * tryProcess() runs (poll timer, every finally->poll, the setImmediate
 * self-feed) therefore each pull a batch against the same stale count and
 * buffer the surplus into `pendingJobs`. Those buffered jobs are leased
 * (locked + heartbeated) so the broker keeps counting them as `active`:
 * the user observed active=5/6 against concurrency=3.
 *
 * The leased set is `pulledJobIds` (running + buffered). This test asserts it
 * never exceeds `concurrency`, which directly mirrors the server `active`
 * count. We model a slow PULL (the report's trigger) so multiple tryProcess()
 * runs suspend at the pull await at once; the real pull logic is unchanged.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Worker over-pull — leased (pulled) jobs never exceed concurrency', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('pulledJobIds (running + buffered, == server active) stays <= concurrency under a fast-completion burst with slow pulls', async () => {
    const CONCURRENCY = 3;
    const TOTAL_JOBS = 40;

    const queue = new Queue('overpull-leased', { embedded: true });
    queue.obliterate();

    let completed = 0;

    const worker = new Worker(
      'overpull-leased',
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
    };

    // Model a slow network round-trip on PULL (the issue's stated trigger).
    // Keep the REAL pull logic and only add latency, so multiple tryProcess()
    // runs suspend at the await simultaneously.
    const realPull = internals.doPullBatch.bind(worker);
    internals.doPullBatch = async (...args: unknown[]) => {
      await Bun.sleep(25);
      return realPull(...args);
    };

    // Sample the leased set (== broker `active`) continuously while draining.
    let maxLeased = 0;
    const sampler = setInterval(() => {
      const leased = internals.pulledJobIds.size;
      if (leased > maxLeased) maxLeased = leased;
    }, 1);

    for (let i = 0; i < TOTAL_JOBS; i++) {
      await queue.add('task', { i });
    }

    const deadline = Date.now() + 15000;
    while (completed < TOTAL_JOBS && Date.now() < deadline) {
      await Bun.sleep(50);
    }
    clearInterval(sampler);

    await worker.close();
    queue.close();

    // Sanity: the fix must not lose or stall jobs.
    expect(completed).toBe(TOTAL_JOBS);

    // THE BUG: leased jobs (running + buffered, holding locks) overshoot the
    // limit even though running processors do not. This is the server active>3.
    expect(maxLeased).toBeLessThanOrEqual(CONCURRENCY);
  }, 30000);
});
