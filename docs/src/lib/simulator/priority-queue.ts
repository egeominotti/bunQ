/**
 * 4-ary MinHeap Priority Queue
 * Same implementation as bunqueue for accurate simulation
 */

import type { Job } from './types';

interface HeapEntry {
  jobId: string;
  priority: number;
  runAt: number;
  generation: bigint;
}

export class PriorityQueue {
  private heap: HeapEntry[] = [];
  private index: Map<string, { job: Job; generation: bigint }> = new Map();
  private currentGen: bigint = 0n;

  get size(): number {
    return this.index.size;
  }

  push(job: Job): void {
    const gen = ++this.currentGen;
    this.index.set(job.id, { job, generation: gen });

    const entry: HeapEntry = {
      jobId: job.id,
      priority: -job.priority, // Negate for max-heap behavior
      runAt: job.runAt,
      generation: gen,
    };

    this.heap.push(entry);
    this.bubbleUp(this.heap.length - 1);
  }

  pop(): Job | undefined {
    while (this.heap.length > 0) {
      const entry = this.heap[0];
      const indexed = this.index.get(entry.jobId);

      // Skip stale entries
      if (!indexed || indexed.generation !== entry.generation) {
        this.removeTop();
        continue;
      }

      // Skip delayed jobs not yet ready
      if (entry.runAt > Date.now()) {
        return undefined;
      }

      this.removeTop();
      const job = indexed.job;
      this.index.delete(entry.jobId);
      return job;
    }
    return undefined;
  }

  peek(): Job | undefined {
    // Skip stale entries at top of heap
    while (this.heap.length > 0) {
      const entry = this.heap[0];
      const indexed = this.index.get(entry.jobId);

      if (indexed && indexed.generation === entry.generation) {
        // Valid entry found - check if ready
        if (entry.runAt <= Date.now()) {
          return indexed.job;
        }
        return undefined; // Top entry not ready yet
      }

      // Stale entry - remove it
      this.removeTop();
    }
    return undefined;
  }

  remove(jobId: string): Job | undefined {
    const indexed = this.index.get(jobId);
    if (!indexed) return undefined;

    // Delete from index - heap entry becomes stale (lazy deletion)
    // Generation mismatch will cause it to be skipped on pop()
    this.index.delete(jobId);

    // Compact heap if too many stale entries (>20%)
    const staleRatio = (this.heap.length - this.index.size) / Math.max(1, this.heap.length);
    if (staleRatio > 0.2 && this.heap.length > 100) {
      this.compact();
    }

    return indexed.job;
  }

  private compact(): void {
    // Rebuild heap with only valid entries
    const validEntries = this.heap.filter(entry => {
      const indexed = this.index.get(entry.jobId);
      return indexed && indexed.generation === entry.generation;
    });
    this.heap = validEntries;
    // Re-heapify
    for (let i = Math.floor(this.heap.length / 4); i >= 0; i--) {
      this.bubbleDown(i);
    }
  }

  has(jobId: string): boolean {
    return this.index.has(jobId);
  }

  get(jobId: string): Job | undefined {
    return this.index.get(jobId)?.job;
  }

  getAll(): Job[] {
    return Array.from(this.index.values()).map(v => v.job);
  }

  getReady(): Job[] {
    const now = Date.now();
    return this.getAll()
      .filter(j => j.runAt <= now)
      .sort((a, b) => b.priority - a.priority || a.runAt - b.runAt);
  }

  getDelayed(): Job[] {
    const now = Date.now();
    return this.getAll()
      .filter(j => j.runAt > now)
      .sort((a, b) => a.runAt - b.runAt);
  }

  private removeTop(): void {
    if (this.heap.length <= 1) {
      this.heap.pop();
      return;
    }
    this.heap[0] = this.heap.pop()!;
    this.bubbleDown(0);
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIdx = Math.floor((index - 1) / 4);
      if (this.compare(index, parentIdx) >= 0) break;
      this.swap(index, parentIdx);
      index = parentIdx;
    }
  }

  private bubbleDown(index: number): void {
    while (true) {
      let smallest = index;
      const firstChild = index * 4 + 1;

      for (let i = 0; i < 4 && firstChild + i < this.heap.length; i++) {
        if (this.compare(firstChild + i, smallest) < 0) {
          smallest = firstChild + i;
        }
      }

      if (smallest === index) break;
      this.swap(index, smallest);
      index = smallest;
    }
  }

  private compare(i: number, j: number): number {
    const a = this.heap[i];
    const b = this.heap[j];
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.runAt - b.runAt;
  }

  private swap(i: number, j: number): void {
    [this.heap[i], this.heap[j]] = [this.heap[j], this.heap[i]];
  }
}
