/**
 * TemporalManager Tests
 * Covers temporal index (SkipList) and delayed job tracking (MinHeap)
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { TemporalManager } from '../src/domain/queue/temporalManager';
import type { JobId } from '../src/domain/types/job';

/** Helper to create branded JobId */
function jobId(id: string): JobId {
  return id as JobId;
}

describe('TemporalManager', () => {
  let tm: TemporalManager;

  beforeEach(() => {
    tm = new TemporalManager();
  });

  // ======================================================
  // Temporal Index Operations (addToIndex, removeFromIndex,
  // clearIndexForQueue, getOldJobs, cleanOrphaned)
  // ======================================================

  describe('addToIndex', () => {
    test('should add a single job to the temporal index', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      expect(tm.indexSize).toBe(1);
    });

    test('should add multiple jobs to the same queue', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');
      tm.addToIndex(3000, jobId('j3'), 'emails');
      expect(tm.indexSize).toBe(3);
    });

    test('should add jobs across different queues', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'payments');
      tm.addToIndex(3000, jobId('j3'), 'notifications');
      expect(tm.indexSize).toBe(3);
    });

    test('should not add duplicate jobIds with same createdAt (equality check)', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(1000, jobId('j1'), 'emails'); // same jobId, same createdAt
      expect(tm.indexSize).toBe(1);
    });

    test('should allow same jobId with different createdAt (no cross-time dedup)', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j1'), 'emails'); // same jobId, different createdAt
      // SkipList equality only checks within same comparator position
      expect(tm.indexSize).toBe(2);
    });

    test('should allow different jobIds with the same createdAt', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(1000, jobId('j2'), 'emails');
      expect(tm.indexSize).toBe(2);
    });
  });

  describe('removeFromIndex', () => {
    test('should remove an existing job from the index', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');
      tm.removeFromIndex(jobId('j1'));
      expect(tm.indexSize).toBe(1);
    });

    test('should handle removing a non-existent job gracefully', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.removeFromIndex(jobId('nonexistent'));
      expect(tm.indexSize).toBe(1);
    });

    test('should handle removing from an empty index', () => {
      tm.removeFromIndex(jobId('j1'));
      expect(tm.indexSize).toBe(0);
    });

    test('should only remove the specified job, not others', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');
      tm.addToIndex(3000, jobId('j3'), 'payments');
      tm.removeFromIndex(jobId('j2'));
      expect(tm.indexSize).toBe(2);
    });
  });

  describe('clearIndexForQueue', () => {
    test('should remove all entries for a specific queue', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');
      tm.addToIndex(3000, jobId('j3'), 'payments');
      tm.clearIndexForQueue('emails');
      expect(tm.indexSize).toBe(1);
    });

    test('should not affect other queues', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'payments');
      tm.addToIndex(3000, jobId('j3'), 'notifications');
      tm.clearIndexForQueue('emails');
      expect(tm.indexSize).toBe(2);
    });

    test('should handle clearing a non-existent queue gracefully', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.clearIndexForQueue('nonexistent');
      expect(tm.indexSize).toBe(1);
    });

    test('should handle clearing from an empty index', () => {
      tm.clearIndexForQueue('emails');
      expect(tm.indexSize).toBe(0);
    });

    test('should remove all entries when queue has many jobs', () => {
      for (let i = 0; i < 50; i++) {
        tm.addToIndex(1000 + i, jobId(`j${i}`), 'bulk-queue');
      }
      tm.addToIndex(9999, jobId('keep-me'), 'other-queue');
      tm.clearIndexForQueue('bulk-queue');
      expect(tm.indexSize).toBe(1);
    });

    test('should preserve another queue entry with the same job ID', () => {
      tm.addToIndex(1000, jobId('shared'), 'emails');
      tm.addToIndex(2000, jobId('shared'), 'payments');

      tm.clearIndexForQueue('emails');

      expect(tm.indexSize).toBe(1);
      expect(tm.getOldJobs('payments', -1e15, 10)).toEqual([
        { jobId: jobId('shared'), createdAt: 2000 },
      ]);
    });
  });

  describe('getOldJobs', () => {
    test('should return jobs older than threshold', () => {
      const now = Date.now();
      tm.addToIndex(now - 10000, jobId('old1'), 'emails');
      tm.addToIndex(now - 5000, jobId('old2'), 'emails');
      tm.addToIndex(now - 100, jobId('new1'), 'emails');

      // Get jobs older than 3000ms
      const oldJobs = tm.getOldJobs('emails', 3000, 100);
      expect(oldJobs.length).toBe(2);
      expect(oldJobs[0].jobId).toBe('old1');
      expect(oldJobs[1].jobId).toBe('old2');
    });

    test('should only return jobs for the specified queue', () => {
      const now = Date.now();
      tm.addToIndex(now - 10000, jobId('j1'), 'emails');
      tm.addToIndex(now - 10000, jobId('j2'), 'payments');

      const oldJobs = tm.getOldJobs('emails', 3000, 100);
      expect(oldJobs.length).toBe(1);
      expect(oldJobs[0].jobId).toBe('j1');
    });

    test('should respect the limit parameter', () => {
      const now = Date.now();
      for (let i = 0; i < 10; i++) {
        tm.addToIndex(now - 10000 + i, jobId(`j${i}`), 'emails');
      }

      const oldJobs = tm.getOldJobs('emails', 3000, 3);
      expect(oldJobs.length).toBe(3);
    });

    test('should return empty array when no jobs are old enough', () => {
      const now = Date.now();
      tm.addToIndex(now - 100, jobId('j1'), 'emails');
      tm.addToIndex(now - 50, jobId('j2'), 'emails');

      const oldJobs = tm.getOldJobs('emails', 3000, 100);
      expect(oldJobs.length).toBe(0);
    });

    test('should return empty array for empty index', () => {
      const oldJobs = tm.getOldJobs('emails', 3000, 100);
      expect(oldJobs.length).toBe(0);
    });

    test('should return empty array for non-existent queue', () => {
      const now = Date.now();
      tm.addToIndex(now - 10000, jobId('j1'), 'emails');

      const oldJobs = tm.getOldJobs('nonexistent', 3000, 100);
      expect(oldJobs.length).toBe(0);
    });

    test('should use job ID as deterministic tie-breaker', () => {
      const createdAt = Date.now() - 10000;
      tm.addToIndex(createdAt, jobId('j3'), 'emails');
      tm.addToIndex(createdAt, jobId('j1'), 'emails');
      tm.addToIndex(createdAt, jobId('j2'), 'emails');

      expect(tm.getOldJobs('emails', 3000, 100).map((job) => job.jobId)).toEqual([
        jobId('j1'),
        jobId('j2'),
        jobId('j3'),
      ]);
    });
  });

  describe('cleanOrphaned', () => {
    test('should remove entries for jobs not in the valid set', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');
      tm.addToIndex(3000, jobId('j3'), 'emails');

      const validIds = new Set<JobId>([jobId('j1'), jobId('j3')]);
      const removed = tm.cleanOrphaned(validIds);
      expect(removed).toBe(1);
      expect(tm.indexSize).toBe(2);
    });

    test('should return 0 when all jobs are valid', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');

      const validIds = new Set<JobId>([jobId('j1'), jobId('j2')]);
      const removed = tm.cleanOrphaned(validIds);
      expect(removed).toBe(0);
      expect(tm.indexSize).toBe(2);
    });

    test('should remove all entries when none are valid', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'emails');
      tm.addToIndex(3000, jobId('j3'), 'payments');

      const validIds = new Set<JobId>();
      const removed = tm.cleanOrphaned(validIds);
      expect(removed).toBe(3);
      expect(tm.indexSize).toBe(0);
    });

    test('should return 0 on empty index', () => {
      const validIds = new Set<JobId>([jobId('j1')]);
      const removed = tm.cleanOrphaned(validIds);
      expect(removed).toBe(0);
    });

    test('should handle mix of active and inactive jobs across queues', () => {
      tm.addToIndex(1000, jobId('e1'), 'emails');
      tm.addToIndex(2000, jobId('e2'), 'emails');
      tm.addToIndex(3000, jobId('p1'), 'payments');
      tm.addToIndex(4000, jobId('p2'), 'payments');
      tm.addToIndex(5000, jobId('n1'), 'notifications');

      // Only e1, p2 are still active
      const validIds = new Set<JobId>([jobId('e1'), jobId('p2')]);
      const removed = tm.cleanOrphaned(validIds);
      expect(removed).toBe(3);
      expect(tm.indexSize).toBe(2);
    });

    test('should remove every timestamp registered for an orphaned job ID', () => {
      tm.addToIndex(1000, jobId('orphan'), 'emails');
      tm.addToIndex(2000, jobId('orphan'), 'payments');
      tm.addToIndex(3000, jobId('valid'), 'emails');

      const removed = tm.cleanOrphaned(new Set([jobId('valid')]));

      expect(removed).toBe(2);
      expect(tm.indexSize).toBe(1);
      expect(tm.getOldJobs('payments', -1e15, 10)).toEqual([]);
    });
  });

  describe('indexSize', () => {
    test('should be 0 for new instance', () => {
      expect(tm.indexSize).toBe(0);
    });

    test('should reflect current number of entries', () => {
      tm.addToIndex(1000, jobId('j1'), 'q');
      expect(tm.indexSize).toBe(1);
      tm.addToIndex(2000, jobId('j2'), 'q');
      expect(tm.indexSize).toBe(2);
      tm.removeFromIndex(jobId('j1'));
      expect(tm.indexSize).toBe(1);
    });
  });

  // ======================================================
  // Delayed Job Operations (isDelayed, addDelayed,
  // removeDelayed, refreshDelayed, delayedCount)
  // ======================================================

  describe('isDelayed', () => {
    test('should return false for non-existent job', () => {
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
    });

    test('should return true after adding a delayed job', () => {
      tm.addDelayed(jobId('j1'), Date.now() + 5000);
      expect(tm.isDelayed(jobId('j1'))).toBe(true);
    });

    test('should return false after removing a delayed job', () => {
      tm.addDelayed(jobId('j1'), Date.now() + 5000);
      tm.removeDelayed(jobId('j1'));
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
    });
  });

  describe('addDelayed', () => {
    test('should add a job to delayed tracking', () => {
      tm.addDelayed(jobId('j1'), 5000);
      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('j1'))).toBe(true);
    });

    test('should track multiple delayed jobs', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j2'), 6000);
      tm.addDelayed(jobId('j3'), 7000);
      expect(tm.delayedCount).toBe(3);
    });

    test('should update runAt if same jobId is added again', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j1'), 8000);
      // delayedJobIds is a Set so the count stays at 1
      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('j1'))).toBe(true);
    });

    test('should reflect in getSizes', () => {
      tm.addDelayed(jobId('j1'), 5000);
      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(1);
      expect(sizes.delayedHeap).toBe(1);
      expect(sizes.delayedRunAt).toBe(1);
    });

    test('should compact stale entries created by repeated rescheduling', () => {
      const runAt = Date.now() + 365 * 24 * 60 * 60 * 1000;
      for (let i = 0; i < 2_000; i++) {
        tm.addDelayed(jobId('rescheduled'), runAt + i);
      }

      expect(tm.delayedCount).toBe(1);
      expect(tm.getSizes().delayedHeap).toBeLessThan(1_024);
    });
  });

  describe('removeDelayed', () => {
    test('should remove a delayed job and return true', () => {
      tm.addDelayed(jobId('j1'), 5000);
      const result = tm.removeDelayed(jobId('j1'));
      expect(result).toBe(true);
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
      expect(tm.delayedCount).toBe(0);
    });

    test('should return false for non-existent job', () => {
      const result = tm.removeDelayed(jobId('nonexistent'));
      expect(result).toBe(false);
    });

    test('should not affect heap size (lazy removal)', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j2'), 6000);
      tm.removeDelayed(jobId('j1'));

      // Heap still has 2 entries (lazy removal); delayedJobIds has 1
      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(1);
      expect(sizes.delayedHeap).toBe(2); // heap is not cleaned immediately
      expect(sizes.delayedRunAt).toBe(1);
    });

    test('should only remove the specified job', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j2'), 6000);
      tm.addDelayed(jobId('j3'), 7000);
      tm.removeDelayed(jobId('j2'));
      expect(tm.delayedCount).toBe(2);
      expect(tm.isDelayed(jobId('j1'))).toBe(true);
      expect(tm.isDelayed(jobId('j2'))).toBe(false);
      expect(tm.isDelayed(jobId('j3'))).toBe(true);
    });

    test('should compact stale future entries when no delayed jobs remain', () => {
      const runAt = Date.now() + 365 * 24 * 60 * 60 * 1000;
      for (let i = 0; i < 2_000; i++) {
        const id = jobId(`future-${i}`);
        tm.addDelayed(id, runAt);
        tm.removeDelayed(id);
      }

      expect(tm.delayedCount).toBe(0);
      expect(tm.getSizes().delayedHeap).toBe(0);
    });

    test('should bound stale future entries while delayed jobs remain live', () => {
      const runAt = Date.now() + 365 * 24 * 60 * 60 * 1000;
      tm.addDelayed(jobId('live'), runAt + 1);

      for (let i = 0; i < 2_000; i++) {
        const id = jobId(`stale-${i}`);
        tm.addDelayed(id, runAt);
        tm.removeDelayed(id);
      }

      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('live'))).toBe(true);
      expect(tm.getSizes().delayedHeap).toBeLessThan(1_024);
    });
  });

  describe('refreshDelayed', () => {
    test('should promote jobs whose runAt is in the past', () => {
      const now = Date.now();
      tm.addDelayed(jobId('j1'), now - 1000); // already past
      tm.addDelayed(jobId('j2'), now - 500); // already past

      const count = tm.refreshDelayed(now);
      expect(count).toBe(2);
      expect(tm.delayedCount).toBe(0);
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
      expect(tm.isDelayed(jobId('j2'))).toBe(false);
    });

    test('should not promote jobs whose runAt is in the future', () => {
      const now = Date.now();
      tm.addDelayed(jobId('j1'), now + 10000);
      tm.addDelayed(jobId('j2'), now + 20000);

      const count = tm.refreshDelayed(now);
      expect(count).toBe(0);
      expect(tm.delayedCount).toBe(2);
    });

    test('should promote only jobs that are due', () => {
      const now = Date.now();
      tm.addDelayed(jobId('j1'), now - 1000); // past
      tm.addDelayed(jobId('j2'), now + 5000); // future
      tm.addDelayed(jobId('j3'), now - 500); // past

      const count = tm.refreshDelayed(now);
      expect(count).toBe(2);
      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
      expect(tm.isDelayed(jobId('j2'))).toBe(true);
      expect(tm.isDelayed(jobId('j3'))).toBe(false);
    });

    test('should return 0 when no delayed jobs exist', () => {
      const count = tm.refreshDelayed(Date.now());
      expect(count).toBe(0);
    });

    test('should promote job whose runAt equals now exactly', () => {
      const now = 5000;
      tm.addDelayed(jobId('j1'), 5000); // runAt === now

      const count = tm.refreshDelayed(now);
      expect(count).toBe(1);
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
    });

    test('should skip stale entries (job removed before refresh)', () => {
      const now = Date.now();
      tm.addDelayed(jobId('j1'), now - 1000);
      tm.addDelayed(jobId('j2'), now - 500);

      // Remove j1 before refresh (lazy removal)
      tm.removeDelayed(jobId('j1'));

      const count = tm.refreshDelayed(now);
      // j1 is skipped (stale), j2 is promoted
      expect(count).toBe(1);
      expect(tm.delayedCount).toBe(0);
    });

    test('should skip stale entries when runAt was updated', () => {
      const now = Date.now();

      // Add j1 with runAt in the past
      tm.addDelayed(jobId('j1'), now - 1000);

      // Update j1 to a future runAt (adds new heap entry, updates delayedRunAt map)
      tm.addDelayed(jobId('j1'), now + 10000);

      const count = tm.refreshDelayed(now);
      // The old heap entry (now - 1000) is popped but its runAt doesn't match
      // the current runAt in delayedRunAt, so it's skipped as stale
      expect(count).toBe(0);
      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('j1'))).toBe(true);
    });

    test('should handle many delayed jobs efficiently', () => {
      const now = Date.now();
      for (let i = 0; i < 100; i++) {
        tm.addDelayed(jobId(`j${i}`), now - (100 - i)); // all in the past
      }

      const count = tm.refreshDelayed(now);
      expect(count).toBe(100);
      expect(tm.delayedCount).toBe(0);
    });

    test('should correctly handle interleaved add/remove/refresh', () => {
      const now = 10000;

      tm.addDelayed(jobId('j1'), 5000); // due
      tm.addDelayed(jobId('j2'), 8000); // due
      tm.addDelayed(jobId('j3'), 15000); // not due

      // Remove j2 before refresh
      tm.removeDelayed(jobId('j2'));

      const count = tm.refreshDelayed(now);
      // j1 promoted, j2 stale (removed), j3 not due
      expect(count).toBe(1);
      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('j3'))).toBe(true);
    });
  });

  describe('delayedCount', () => {
    test('should be 0 for new instance', () => {
      expect(tm.delayedCount).toBe(0);
    });

    test('should track additions and removals', () => {
      tm.addDelayed(jobId('j1'), 5000);
      expect(tm.delayedCount).toBe(1);

      tm.addDelayed(jobId('j2'), 6000);
      expect(tm.delayedCount).toBe(2);

      tm.removeDelayed(jobId('j1'));
      expect(tm.delayedCount).toBe(1);
    });

    test('should decrease when jobs are promoted via refreshDelayed', () => {
      const now = Date.now();
      tm.addDelayed(jobId('j1'), now - 1000);
      tm.addDelayed(jobId('j2'), now - 500);
      expect(tm.delayedCount).toBe(2);

      tm.refreshDelayed(now);
      expect(tm.delayedCount).toBe(0);
    });
  });

  // ======================================================
  // Reset Operations (clearDelayed, clear)
  // ======================================================

  describe('clearDelayed', () => {
    test('should clear all delayed tracking', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j2'), 6000);
      tm.clearDelayed();

      expect(tm.delayedCount).toBe(0);
      expect(tm.isDelayed(jobId('j1'))).toBe(false);
      expect(tm.isDelayed(jobId('j2'))).toBe(false);

      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(0);
      expect(sizes.delayedHeap).toBe(0);
      expect(sizes.delayedRunAt).toBe(0);
    });

    test('should not affect the temporal index', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addDelayed(jobId('j2'), 5000);
      tm.clearDelayed();

      expect(tm.indexSize).toBe(1);
      expect(tm.delayedCount).toBe(0);
    });

    test('should be safe to call on empty state', () => {
      tm.clearDelayed();
      expect(tm.delayedCount).toBe(0);
    });
  });

  describe('clear', () => {
    test('should clear both temporal index and delayed tracking', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'payments');
      tm.addDelayed(jobId('j3'), 5000);
      tm.addDelayed(jobId('j4'), 6000);

      tm.clear();

      expect(tm.indexSize).toBe(0);
      expect(tm.delayedCount).toBe(0);
      expect(tm.isDelayed(jobId('j3'))).toBe(false);
    });

    test('should be safe to call on empty state', () => {
      tm.clear();
      expect(tm.indexSize).toBe(0);
      expect(tm.delayedCount).toBe(0);
    });

    test('should allow reuse after clearing', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addDelayed(jobId('j2'), 5000);
      tm.clear();

      // Add new data after clear
      tm.addToIndex(3000, jobId('j3'), 'payments');
      tm.addDelayed(jobId('j4'), 8000);

      expect(tm.indexSize).toBe(1);
      expect(tm.delayedCount).toBe(1);
      expect(tm.isDelayed(jobId('j4'))).toBe(true);
    });
  });

  // ======================================================
  // getSizes (debug info)
  // ======================================================

  describe('getSizes', () => {
    test('should return all zeros for new instance', () => {
      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(0);
      expect(sizes.delayedHeap).toBe(0);
      expect(sizes.delayedRunAt).toBe(0);
      expect(sizes.temporalIndex).toBe(0);
    });

    test('should reflect temporal index size', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.addToIndex(2000, jobId('j2'), 'payments');

      const sizes = tm.getSizes();
      expect(sizes.temporalIndex).toBe(2);
    });

    test('should reflect delayed tracking sizes', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j2'), 6000);

      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(2);
      expect(sizes.delayedHeap).toBe(2);
      expect(sizes.delayedRunAt).toBe(2);
    });

    test('should show divergence between heap and jobIds after lazy removal', () => {
      tm.addDelayed(jobId('j1'), 5000);
      tm.addDelayed(jobId('j2'), 6000);
      tm.addDelayed(jobId('j3'), 7000);

      tm.removeDelayed(jobId('j2'));

      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(2); // j1, j3
      expect(sizes.delayedHeap).toBe(3); // j1, j2, j3 still in heap
      expect(sizes.delayedRunAt).toBe(2); // j1, j3
    });

    test('should converge after refreshDelayed cleans stale entries', () => {
      tm.addDelayed(jobId('j1'), 1000);
      tm.addDelayed(jobId('j2'), 2000);
      tm.addDelayed(jobId('j3'), 3000);

      tm.removeDelayed(jobId('j2'));

      // Refresh at now=2500: promotes j1, skips stale j2
      tm.refreshDelayed(2500);

      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(1); // only j3
      expect(sizes.delayedRunAt).toBe(1); // only j3
      // Heap had j1 popped and j2 stale popped, j3 remains
      expect(sizes.delayedHeap).toBe(1);
    });

    test('should show both temporal and delayed data', () => {
      tm.addToIndex(1000, jobId('idx1'), 'q1');
      tm.addToIndex(2000, jobId('idx2'), 'q1');
      tm.addToIndex(3000, jobId('idx3'), 'q2');
      tm.addDelayed(jobId('del1'), 5000);

      const sizes = tm.getSizes();
      expect(sizes.temporalIndex).toBe(3);
      expect(sizes.delayedJobIds).toBe(1);
      expect(sizes.delayedHeap).toBe(1);
      expect(sizes.delayedRunAt).toBe(1);
    });
  });

  // ======================================================
  // Multiple queues isolation
  // ======================================================

  describe('multiple queues isolation', () => {
    test('clearIndexForQueue should only affect target queue', () => {
      tm.addToIndex(1000, jobId('e1'), 'emails');
      tm.addToIndex(2000, jobId('e2'), 'emails');
      tm.addToIndex(3000, jobId('p1'), 'payments');
      tm.addToIndex(4000, jobId('p2'), 'payments');
      tm.addToIndex(5000, jobId('n1'), 'notifications');

      tm.clearIndexForQueue('payments');

      expect(tm.indexSize).toBe(3); // e1, e2, n1
    });

    test('getOldJobs should filter by queue name', () => {
      const now = Date.now();
      tm.addToIndex(now - 10000, jobId('e1'), 'emails');
      tm.addToIndex(now - 10000, jobId('p1'), 'payments');
      tm.addToIndex(now - 10000, jobId('n1'), 'notifications');

      const emailJobs = tm.getOldJobs('emails', 5000, 100);
      expect(emailJobs.length).toBe(1);
      expect(emailJobs[0].jobId).toBe('e1');

      const paymentJobs = tm.getOldJobs('payments', 5000, 100);
      expect(paymentJobs.length).toBe(1);
      expect(paymentJobs[0].jobId).toBe('p1');
    });

    test('cleanOrphaned works across all queues at once', () => {
      tm.addToIndex(1000, jobId('e1'), 'emails');
      tm.addToIndex(2000, jobId('p1'), 'payments');
      tm.addToIndex(3000, jobId('n1'), 'notifications');

      // Only e1 is valid
      const validIds = new Set<JobId>([jobId('e1')]);
      const removed = tm.cleanOrphaned(validIds);
      expect(removed).toBe(2);
      expect(tm.indexSize).toBe(1);
    });
  });

  // ======================================================
  // Edge cases
  // ======================================================

  describe('edge cases', () => {
    test('operations on completely empty TemporalManager', () => {
      expect(tm.indexSize).toBe(0);
      expect(tm.delayedCount).toBe(0);
      expect(tm.isDelayed(jobId('anything'))).toBe(false);
      expect(tm.removeDelayed(jobId('anything'))).toBe(false);
      expect(tm.refreshDelayed(Date.now())).toBe(0);
      expect(tm.getOldJobs('q', 5000, 100)).toEqual([]);
      expect(tm.cleanOrphaned(new Set())).toBe(0);

      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(0);
      expect(sizes.delayedHeap).toBe(0);
      expect(sizes.delayedRunAt).toBe(0);
      expect(sizes.temporalIndex).toBe(0);
    });

    test('adding delayed job with runAt = 0', () => {
      tm.addDelayed(jobId('j1'), 0);
      expect(tm.isDelayed(jobId('j1'))).toBe(true);
      expect(tm.delayedCount).toBe(1);

      // Any non-negative now should promote it
      const count = tm.refreshDelayed(0);
      expect(count).toBe(1);
      expect(tm.delayedCount).toBe(0);
    });

    test('refreshDelayed with now = 0 should not promote future jobs', () => {
      tm.addDelayed(jobId('j1'), 1000);
      const count = tm.refreshDelayed(0);
      expect(count).toBe(0);
      expect(tm.delayedCount).toBe(1);
    });

    test('double remove of delayed job', () => {
      tm.addDelayed(jobId('j1'), 5000);
      expect(tm.removeDelayed(jobId('j1'))).toBe(true);
      expect(tm.removeDelayed(jobId('j1'))).toBe(false);
    });

    test('add to index then clear all then re-add', () => {
      tm.addToIndex(1000, jobId('j1'), 'q');
      tm.clear();
      tm.addToIndex(2000, jobId('j2'), 'q');
      expect(tm.indexSize).toBe(1);
    });

    test('multiple addDelayed for same jobId creates stale heap entries', () => {
      // Add the same jobId 3 times with different runAt values
      tm.addDelayed(jobId('j1'), 1000);
      tm.addDelayed(jobId('j1'), 2000);
      tm.addDelayed(jobId('j1'), 3000);

      // Only 1 delayed job, but 3 heap entries
      const sizes = tm.getSizes();
      expect(sizes.delayedJobIds).toBe(1);
      expect(sizes.delayedHeap).toBe(3);
      expect(sizes.delayedRunAt).toBe(1);

      // Refresh at 2500: pops 1000 (stale), pops 2000 (stale), stops at 3000 (future)
      const count = tm.refreshDelayed(2500);
      expect(count).toBe(0);
      expect(tm.delayedCount).toBe(1);
    });

    test('refreshDelayed processes stale entries in correct order', () => {
      // Create scenario: add j1 at 1000, update to 5000, add j2 at 2000
      tm.addDelayed(jobId('j1'), 1000);
      tm.addDelayed(jobId('j1'), 5000); // updates runAt for j1
      tm.addDelayed(jobId('j2'), 2000);

      // At now=3000: heap pops 1000 (j1, stale), then 2000 (j2, valid), stops at 5000
      const count = tm.refreshDelayed(3000);
      expect(count).toBe(1); // only j2 promoted
      expect(tm.isDelayed(jobId('j1'))).toBe(true); // still delayed (runAt=5000)
      expect(tm.isDelayed(jobId('j2'))).toBe(false); // promoted
    });

    test('large number of entries in temporal index', () => {
      for (let i = 0; i < 500; i++) {
        tm.addToIndex(i, jobId(`j${i}`), `q${i % 5}`);
      }
      expect(tm.indexSize).toBe(500);

      // Clear one queue - should remove 100 entries (every 5th)
      tm.clearIndexForQueue('q0');
      expect(tm.indexSize).toBe(400);
    });

    test('removeFromIndex after clearIndexForQueue is safe', () => {
      tm.addToIndex(1000, jobId('j1'), 'emails');
      tm.clearIndexForQueue('emails');
      // Removing from already empty queue
      tm.removeFromIndex(jobId('j1'));
      expect(tm.indexSize).toBe(0);
    });
  });
});
