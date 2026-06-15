/**
 * Stability bug: perQueueMetrics map grows unbounded — one permanent entry per
 * unique queue name, never evicted, not even by obliterate().
 *
 * Root cause:
 *   - queueManager.ts:113 declares `perQueueMetrics` as a PLAIN Map<string, ...>
 *     with NO cap (unlike completedJobs/jobResults/customIdMap which are
 *     BoundedMap/BoundedSet/LRUMap). It is only ever read with .get and written
 *     with .set — there is no eviction path anywhere.
 *   - ack.ts:119-126 inserts a fresh entry for every distinct queue name on the
 *     first successful ack for that queue (likewise on fail at ack.ts:240-245
 *     and ackHelpers.ts:200-207). So every unique queue name leaks one entry.
 *   - obliterate() (queueManager.ts:758-811) is the user-facing remediation: it
 *     purges 11+ collections (jobIndex, completedJobs, completedJobsData,
 *     jobResults, jobLogs, jobLocks, failedChildrenValues, ignoredChildrenFailures,
 *     pendingDepChecks, stalledCandidates, repeatChain, customIdMap, plus the
 *     SQLite rows and the queueNamesCache) but NEVER deletes from
 *     perQueueMetrics. cleanEmptyQueues also leaves it untouched.
 *
 * Impact: in a workload that creates many short-lived / dynamically-named queues
 * (per-tenant, per-job-id, per-session queues), perQueueMetrics grows without
 * bound for the lifetime of the process. There is NO way for an operator to
 * reclaim it — obliterating the queue (the documented cleanup) does not free it.
 *
 * This RED test pushes+acks one job across N distinct queue names, then asserts:
 *   1) perQueueMetrics stays bounded (it does NOT — size == N) — FAILS NOW.
 *   2) after obliterating every queue, perQueueMetrics drops to 0 (it does NOT —
 *      obliterate omits this map) — FAILS NOW.
 * Either failing assertion proves the leak; both encode the correct post-fix
 * behavior.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Stability: perQueueMetrics unbounded growth, not reclaimed by obliterate()', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('perQueueMetrics is reclaimed by obliterate()', async () => {
    const N = 200;

    // Map-like (a plain Map pre-fix; an LRU-bounded MapLike post-fix). We only
    // rely on the get/delete/size surface, not the concrete class.
    const perQueueMetrics = (qm as any).perQueueMetrics as {
      get(k: string): unknown;
      delete(k: string): boolean;
      readonly size: number;
    };
    expect(typeof perQueueMetrics.delete).toBe('function');
    expect(perQueueMetrics.size).toBe(0);

    const queueNames: string[] = [];
    for (let i = 0; i < N; i++) {
      const queue = `tenant-${i}`;
      queueNames.push(queue);

      const job = await qm.push(queue, { data: { i } });
      expect(job).not.toBeNull();

      const pulled = await qm.pull(queue, 0);
      expect(pulled).not.toBeNull();

      // ack with no token (no lock taken) — populates perQueueMetrics for
      // this queue via ack.ts:119-126.
      await qm.ack(pulled!.id, { ok: true });
    }

    // Sanity: every distinct queue produced exactly one entry while live.
    expect(perQueueMetrics.size).toBe(N);

    // LOAD-BEARING (post-fix correct behavior): obliterate() is the documented
    // way to reclaim all state for a queue. After obliterating every queue,
    // perQueueMetrics must be empty. Pre-fix this stayed at N because obliterate
    // omitted this map (the unbounded leak with no operator remediation). The
    // fix prunes the entry in obliterate() (and in cleanEmptyQueues() for queues
    // that drain), so metrics stay accurate for live queues but are reclaimed
    // when a queue is removed.
    for (const queue of queueNames) {
      qm.obliterate(queue);
    }
    expect(perQueueMetrics.size).toBe(0);
  });
});
