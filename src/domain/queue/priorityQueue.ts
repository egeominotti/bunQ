/**
 * Indexed Priority Queue
 * Combines binary heap for O(log n) priority operations
 * with Map for O(1) lookups by job ID
 */

import type { Job, JobId } from '../types/job';
import type { HeapEntry, IndexedJob } from '../types/priorityQueue';
import { comparePriorityEntries } from './priorityQueueOrder';

/**
 * Indexed Priority Queue implementation with 4-ary heap
 * 4-ary provides better cache locality than binary heap
 * O(log₄ n) push, pop, update
 * O(1) find, has
 */
export class IndexedPriorityQueue {
  /** Branching factor - 4 provides optimal cache performance */
  private static readonly D = 4;
  private heap: HeapEntry[] = [];
  private readonly index: Map<JobId, IndexedJob> = new Map();
  // Use BigInt to prevent overflow at extreme throughput
  private generation = 0n;

  /** Get current size */
  get size(): number {
    return this.index.size;
  }

  /** Check if empty */
  get isEmpty(): boolean {
    return this.index.size === 0;
  }

  /** Push a job into the queue */
  push(job: Job): void {
    const gen = this.generation++;

    // Store in index
    this.index.set(job.id, { job, generation: gen });

    // Add to heap
    const entry: HeapEntry = {
      jobId: job.id,
      priority: job.priority,
      runAt: job.runAt,
      lifo: job.lifo,
      generation: gen,
    };
    this.heap.push(entry);
    this.bubbleUp(this.heap.length - 1);
  }

  /** Pop the highest priority job */
  pop(): Job | null {
    while (this.heap.length > 0) {
      const entry = this.heap[0];
      const indexed = this.index.get(entry.jobId);

      // Skip stale entries (generation mismatch = updated or removed)
      if (indexed?.generation !== entry.generation) {
        this.removeTop();
        continue;
      }

      // Remove from both structures
      this.removeTop();
      this.index.delete(entry.jobId);
      return indexed.job;
    }
    return null;
  }

  /** Peek at the highest priority job without removing */
  peek(): Job | null {
    while (this.heap.length > 0) {
      const entry = this.heap[0];
      const indexed = this.index.get(entry.jobId);

      // Skip stale entries
      if (indexed?.generation !== entry.generation) {
        this.removeTop();
        continue;
      }

      return indexed.job;
    }
    return null;
  }

  /** Find a job by ID - O(1) */
  find(jobId: JobId): Job | null {
    return this.index.get(jobId)?.job ?? null;
  }

  /** Check if job exists - O(1) */
  has(jobId: JobId): boolean {
    return this.index.has(jobId);
  }

  /** Remove a job by ID - O(1) for index, heap cleans lazily */
  remove(jobId: JobId): Job | null {
    const indexed = this.index.get(jobId);
    if (!indexed) return null;

    this.index.delete(jobId);
    // Heap entry becomes stale, will be skipped on pop
    return indexed.job;
  }

  /** Update job priority - O(log n). Optionally update the LIFO tie-break flag. */
  updatePriority(jobId: JobId, newPriority: number, newLifo?: boolean): boolean {
    const indexed = this.index.get(jobId);
    if (!indexed) return false;

    const lifo = newLifo ?? indexed.job.lifo;

    // Create new job with updated priority (immutable pattern)
    const updatedJob: Job = {
      ...indexed.job,
      priority: newPriority,
      lifo,
    };

    // Create new heap entry with new generation
    const gen = this.generation++;

    // Update index with new job and generation
    this.index.set(jobId, { job: updatedJob, generation: gen });

    const entry: HeapEntry = {
      jobId: updatedJob.id,
      priority: newPriority,
      runAt: updatedJob.runAt,
      lifo,
      generation: gen,
    };
    this.heap.push(entry);
    this.bubbleUp(this.heap.length - 1);

    return true;
  }

  /** Update job runAt (for delay changes) - O(log n) */
  updateRunAt(jobId: JobId, newRunAt: number): boolean {
    const indexed = this.index.get(jobId);
    if (!indexed) return false;

    // Create new job with updated runAt (immutable pattern)
    const updatedJob: Job = {
      ...indexed.job,
      runAt: newRunAt,
    };

    // Create new heap entry with new generation
    const gen = this.generation++;

    // Update index with new job and generation
    this.index.set(jobId, { job: updatedJob, generation: gen });

    const entry: HeapEntry = {
      jobId: updatedJob.id,
      priority: updatedJob.priority,
      runAt: newRunAt,
      lifo: updatedJob.lifo,
      generation: gen,
    };
    this.heap.push(entry);
    this.bubbleUp(this.heap.length - 1);

    return true;
  }

  /** Get all jobs (for iteration) */
  values(): Job[] {
    return Array.from(this.index.values()).map((v) => v.job);
  }

  /** Clear the queue */
  clear(): void {
    this.heap = [];
    this.index.clear();
    this.generation = 0n;
  }

  /**
   * Get the ratio of stale entries in the heap
   * Used to decide when to compact
   */
  getStaleRatio(): number {
    if (this.heap.length === 0) return 0;
    return 1 - this.index.size / this.heap.length;
  }

  /**
   * Compact the heap by removing stale entries
   * Call when stale ratio exceeds threshold (e.g., 20%)
   * O(n) operation but reclaims memory and improves performance
   */
  compact(): void {
    if (this.heap.length === 0) return;

    // Filter out stale entries
    const validEntries: HeapEntry[] = [];
    for (const entry of this.heap) {
      const indexed = this.index.get(entry.jobId);
      if (indexed?.generation === entry.generation) {
        validEntries.push(entry);
      }
    }

    // Rebuild heap - O(n) heapify
    this.heap = validEntries;
    this.heapify();
  }

  /** Rebuild heap property from arbitrary array - O(n) */
  private heapify(): void {
    const D = IndexedPriorityQueue.D;
    // Start from last non-leaf node and bubble down
    // In D-ary heap, last non-leaf is at floor((n-2)/D)
    for (let i = Math.floor((this.heap.length - 2) / D); i >= 0; i--) {
      this.bubbleDown(i);
    }
  }

  /** Check if compaction is needed (stale ratio > threshold) */
  needsCompaction(threshold: number = 0.2): boolean {
    return this.getStaleRatio() > threshold;
  }

  // ============ Heap Operations ============

  private removeTop(): void {
    if (this.heap.length <= 1) {
      this.heap.pop();
      return;
    }
    this.heap[0] = this.heap.pop()!;
    this.bubbleDown(0);
  }

  /** 4-ary bubbleUp: parent at floor((idx-1)/D) */
  private bubbleUp(idx: number): void {
    const D = IndexedPriorityQueue.D;
    while (idx > 0) {
      const parentIdx = Math.floor((idx - 1) / D);
      if (comparePriorityEntries(this.heap[idx], this.heap[parentIdx]) >= 0) {
        break;
      }
      this.swap(idx, parentIdx);
      idx = parentIdx;
    }
  }

  /** 4-ary bubbleDown: children at D*idx+1 through D*idx+D */
  private bubbleDown(idx: number): void {
    const D = IndexedPriorityQueue.D;
    const length = this.heap.length;
    const heap = this.heap;

    while (true) {
      const firstChild = D * idx + 1;
      if (firstChild >= length) break;

      // Find minimum among up to D children (cache-friendly sequential access)
      let smallest = idx;
      const lastChild = Math.min(firstChild + D, length);

      for (let c = firstChild; c < lastChild; c++) {
        if (comparePriorityEntries(heap[c], heap[smallest]) < 0) {
          smallest = c;
        }
      }

      if (smallest === idx) break;
      this.swap(idx, smallest);
      idx = smallest;
    }
  }

  private swap(i: number, j: number): void {
    const temp = this.heap[i];
    this.heap[i] = this.heap[j];
    this.heap[j] = temp;
  }
}
