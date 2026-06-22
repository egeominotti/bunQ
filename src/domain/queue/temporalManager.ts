/**
 * TemporalManager - Manages temporal index and delayed job tracking
 *
 * Provides:
 * - Temporal index (SkipList) for efficient cleanQueue range queries
 * - Delayed heap (MinHeap) for O(k) delayed job refresh
 */

import type { JobId } from '../types/job';
import { SkipList } from '../../shared/skipList';
import { MinHeap } from '../../shared/minHeap';

/** Entry in the temporal index */
export interface TemporalEntry {
  createdAt: number;
  jobId: JobId;
  queue: string;
}

/** Entry in the delayed heap */
interface DelayedEntry {
  jobId: JobId;
  runAt: number;
}

/**
 * Manages temporal indexing for efficient job cleanup
 * and delayed job tracking for O(k) refresh
 */
export class TemporalManager {
  /**
   * Temporal index: Skip List for O(log n) insert/delete
   * Ordered by createdAt for efficient cleanQueue range queries
   * Uses equality check on jobId to prevent duplicate entries for the same job
   */
  // Total-order comparator: createdAt first, then jobId as a tie-break. Without
  // the tie-break, a batch of jobs sharing one createdAt (the addBulk case —
  // `now` is captured once) makes every node compare-equal, which (a) turns
  // SkipList.insert's duplicate-check scan into O(n) per insert => O(n²) for the
  // batch, and (b) makes SkipList.delete remove the WRONG same-createdAt node
  // (it stops at the first compare-equal node). A total order fixes both: the
  // dedup scan and delete both resolve to the exact (createdAt, jobId) node in
  // O(log n). jobId is a string (UUIDv7 by default, or any custom id), so its
  // lexicographic comparison is a valid total order in every case. Equality is
  // still by jobId (now reached only for a true duplicate).
  private readonly temporalIndex = new SkipList<TemporalEntry>(
    (a, b) => a.createdAt - b.createdAt || (a.jobId < b.jobId ? -1 : a.jobId > b.jobId ? 1 : 0),
    16,
    0.5,
    (a, b) => a.jobId === b.jobId
  );

  /** Set of delayed job IDs for tracking when they become ready */
  private readonly delayedJobIds = new Set<JobId>();

  /**
   * Min-heap of delayed jobs ordered by runAt for O(k) refresh
   * Instead of O(n × queues) iteration
   */
  private readonly delayedHeap = new MinHeap<DelayedEntry>((a, b) => a.runAt - b.runAt);

  /** Map from jobId to current runAt for stale detection in delayedHeap */
  private readonly delayedRunAt = new Map<JobId, number>();

  // ============ Temporal Index Operations ============

  /** Add job to temporal index - O(log n) */
  addToIndex(createdAt: number, jobId: JobId, queue: string): void {
    this.temporalIndex.insert({ createdAt, jobId, queue });
  }

  /**
   * Get old jobs from temporal index - O(log n + k)
   * Returns jobs older than threshold, up to limit
   */
  getOldJobs(
    queue: string,
    thresholdMs: number,
    limit: number
  ): Array<{ jobId: JobId; createdAt: number }> {
    const now = Date.now();
    const threshold = now - thresholdMs;
    const result: Array<{ jobId: JobId; createdAt: number }> = [];

    for (const entry of this.temporalIndex.values()) {
      if (entry.createdAt > threshold) break;
      if (entry.queue === queue) {
        result.push({ jobId: entry.jobId, createdAt: entry.createdAt });
        if (result.length >= limit) break;
      }
    }

    return result;
  }

  /** Remove job from temporal index */
  removeFromIndex(jobId: JobId): void {
    this.temporalIndex.deleteWhere((e) => e.jobId === jobId);
  }

  /** Clear temporal index for a queue */
  clearIndexForQueue(queue: string): void {
    this.temporalIndex.removeAll((e) => e.queue === queue);
  }

  /**
   * Clean orphaned temporal index entries.
   * Removes entries for jobs that no longer exist.
   */
  cleanOrphaned(validJobIds: Set<JobId>): number {
    if (this.temporalIndex.size === 0) return 0;

    const beforeSize = this.temporalIndex.size;
    this.temporalIndex.removeAll((e) => !validJobIds.has(e.jobId));
    return beforeSize - this.temporalIndex.size;
  }

  /** Get temporal index size */
  get indexSize(): number {
    return this.temporalIndex.size;
  }

  // ============ Delayed Job Operations ============

  /** Check if job is delayed */
  isDelayed(jobId: JobId): boolean {
    return this.delayedJobIds.has(jobId);
  }

  /** Add a delayed job */
  addDelayed(jobId: JobId, runAt: number): void {
    this.delayedJobIds.add(jobId);
    this.delayedHeap.push({ jobId, runAt });
    this.delayedRunAt.set(jobId, runAt);
  }

  /** Remove a delayed job (lazy removal from heap) */
  removeDelayed(jobId: JobId): boolean {
    if (this.delayedJobIds.has(jobId)) {
      this.delayedJobIds.delete(jobId);
      this.delayedRunAt.delete(jobId);
      return true;
    }
    return false;
  }

  /**
   * Refresh delayed jobs that have become ready
   * O(k) where k = jobs that became ready
   * Returns number of jobs that became ready
   */
  refreshDelayed(now: number): number {
    let count = 0;

    while (!this.delayedHeap.isEmpty) {
      const top = this.delayedHeap.peek();
      if (!top || top.runAt > now) break;

      this.delayedHeap.pop();

      // Check if stale (job was removed or runAt changed)
      const currentRunAt = this.delayedRunAt.get(top.jobId);
      if (currentRunAt === undefined) continue;
      if (currentRunAt !== top.runAt) continue;

      // Job is ready - remove from delayed tracking
      this.delayedJobIds.delete(top.jobId);
      this.delayedRunAt.delete(top.jobId);
      count++;
    }

    return count;
  }

  /** Get delayed job count */
  get delayedCount(): number {
    return this.delayedJobIds.size;
  }

  // ============ Reset Operations ============

  /** Clear all delayed tracking */
  clearDelayed(): void {
    this.delayedJobIds.clear();
    this.delayedHeap.clear();
    this.delayedRunAt.clear();
  }

  /** Clear all data */
  clear(): void {
    this.clearDelayed();
    // Note: SkipList doesn't have a clear method, so we recreate by removing all
    this.temporalIndex.removeAll(() => true);
  }

  // ============ Debug Info ============

  /** Get internal structure sizes for memory debugging */
  getSizes(): {
    delayedJobIds: number;
    delayedHeap: number;
    delayedRunAt: number;
    temporalIndex: number;
  } {
    return {
      delayedJobIds: this.delayedJobIds.size,
      delayedHeap: this.delayedHeap.size,
      delayedRunAt: this.delayedRunAt.size,
      temporalIndex: this.temporalIndex.size,
    };
  }
}
