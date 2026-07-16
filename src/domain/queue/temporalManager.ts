/**
 * TemporalManager - Manages temporal index and delayed job tracking
 *
 * Provides:
 * - Temporal index (SkipList) for efficient cleanQueue range queries
 * - Delayed heap (MinHeap) for O(k) delayed job refresh
 */

import { MinHeap } from '../../shared/minHeap';
import type { JobId } from '../types/job';
import { TemporalIndex } from './temporalIndex';

export type { TemporalEntry } from './temporalIndex';

/** Entry in the delayed heap */
interface DelayedEntry {
  jobId: JobId;
  runAt: number;
}

const DELAYED_COMPACTION_MIN_STALE = 256;

/**
 * Manages temporal indexing for efficient job cleanup
 * and delayed job tracking for O(k) refresh
 */
export class TemporalManager {
  private readonly temporalIndex = new TemporalIndex();

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
    this.temporalIndex.add(createdAt, jobId, queue);
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
    return this.temporalIndex.getOldJobs(queue, threshold, limit);
  }

  /** Remove job from temporal index */
  removeFromIndex(jobId: JobId): void {
    this.temporalIndex.remove(jobId);
  }

  /** Clear temporal index for a queue */
  clearIndexForQueue(queue: string): void {
    this.temporalIndex.clearQueue(queue);
  }

  /**
   * Clean orphaned temporal index entries.
   * Removes entries for jobs that no longer exist.
   */
  cleanOrphaned(validJobIds: Set<JobId>): number {
    return this.temporalIndex.cleanOrphaned(validJobIds);
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
    this.maybeCompactDelayedHeap();
  }

  /** Remove a delayed job (lazy removal from heap) */
  removeDelayed(jobId: JobId): boolean {
    if (!this.delayedJobIds.delete(jobId)) return false;
    this.delayedRunAt.delete(jobId);
    this.maybeCompactDelayedHeap();
    return true;
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

    this.maybeCompactDelayedHeap();

    return count;
  }

  private maybeCompactDelayedHeap(): void {
    const liveCount = this.delayedRunAt.size;
    if (liveCount === 0) {
      this.delayedHeap.clear();
      return;
    }

    const staleCount = this.delayedHeap.size - liveCount;
    if (staleCount < DELAYED_COMPACTION_MIN_STALE || staleCount < liveCount) return;

    this.delayedHeap.buildFrom(
      Array.from(this.delayedRunAt, ([jobId, runAt]) => ({ jobId, runAt }))
    );
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
    this.temporalIndex.clear();
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
