/**
 * Priority Queue Tests
 */

import { describe, test, expect } from 'bun:test';
import { IndexedPriorityQueue } from '../src/domain/queue/priorityQueue';
import { createJob, jobId } from '../src/domain/types/job';

describe('IndexedPriorityQueue', () => {
  function makeJob(id: number, priority = 0, runAt = Date.now()) {
    const job = createJob(jobId(`test-job-${id}`), 'test', { data: { id } }, runAt);
    (job as { priority: number }).priority = priority;
    return job;
  }

  test('should start empty', () => {
    const queue = new IndexedPriorityQueue();
    expect(queue.size).toBe(0);
    expect(queue.isEmpty).toBe(true);
    expect(queue.pop()).toBeNull();
  });

  test('should push and pop single job', () => {
    const queue = new IndexedPriorityQueue();
    const job = makeJob(1);

    queue.push(job);
    expect(queue.size).toBe(1);
    expect(queue.isEmpty).toBe(false);

    const popped = queue.pop();
    expect(popped).not.toBeNull();
    expect(popped?.id).toBe(job.id);
    expect(queue.size).toBe(0);
  });

  test('should respect priority order (higher first)', () => {
    const queue = new IndexedPriorityQueue();
    const low = makeJob(1, 1);
    const high = makeJob(2, 10);
    const medium = makeJob(3, 5);

    queue.push(low);
    queue.push(high);
    queue.push(medium);

    expect(queue.pop()?.id).toBe(high.id);
    expect(queue.pop()?.id).toBe(medium.id);
    expect(queue.pop()?.id).toBe(low.id);
  });

  test('should find job by id', () => {
    const queue = new IndexedPriorityQueue();
    const job1 = makeJob(1);
    const job2 = makeJob(2);

    queue.push(job1);
    queue.push(job2);

    expect(queue.find(job1.id)?.id).toBe(job1.id);
    expect(queue.find(job2.id)?.id).toBe(job2.id);
    expect(queue.find(jobId('test-job-999'))).toBeNull();
  });

  test('should remove job by id', () => {
    const queue = new IndexedPriorityQueue();
    const job1 = makeJob(1, 10);
    const job2 = makeJob(2, 5);

    queue.push(job1);
    queue.push(job2);

    const removed = queue.remove(job1.id);
    expect(removed?.id).toBe(job1.id);
    expect(queue.size).toBe(1);
  });

  test('should update priority', () => {
    const queue = new IndexedPriorityQueue();
    const job1 = makeJob(1, 1);
    const job2 = makeJob(2, 10);

    queue.push(job1);
    queue.push(job2);

    queue.updatePriority(job1.id, 100);

    expect(queue.pop()?.id).toBe(job1.id);
  });

  test('should clear all jobs', () => {
    const queue = new IndexedPriorityQueue();
    queue.push(makeJob(1));
    queue.push(makeJob(2));

    queue.clear();
    expect(queue.size).toBe(0);
  });

  describe('compaction', () => {
    test('should report stale ratio after removals', () => {
      const queue = new IndexedPriorityQueue();
      queue.push(makeJob(1));
      queue.push(makeJob(2));
      queue.push(makeJob(3));
      queue.push(makeJob(4));

      // Initially no stale entries
      expect(queue.getStaleRatio()).toBe(0);

      // Remove 2 jobs - creates stale heap entries
      queue.remove(makeJob(1).id);
      queue.remove(makeJob(2).id);

      // Now 2/4 heap entries are stale = 50%
      expect(queue.getStaleRatio()).toBe(0.5);
    });

    test('should detect when compaction is needed', () => {
      const queue = new IndexedPriorityQueue();
      queue.push(makeJob(1));
      queue.push(makeJob(2));
      queue.push(makeJob(3));
      queue.push(makeJob(4));
      queue.push(makeJob(5));

      // No compaction needed initially
      expect(queue.needsCompaction(0.2)).toBe(false);

      // Remove 2 of 5 = 40% stale
      queue.remove(makeJob(1).id);
      queue.remove(makeJob(2).id);

      expect(queue.needsCompaction(0.2)).toBe(true);
      expect(queue.needsCompaction(0.5)).toBe(false);
    });

    test('should compact heap and remove stale entries', () => {
      const queue = new IndexedPriorityQueue();
      queue.push(makeJob(1, 1));
      queue.push(makeJob(2, 2));
      queue.push(makeJob(3, 3));
      queue.push(makeJob(4, 4));

      // Remove jobs to create stale entries
      queue.remove(makeJob(1).id);
      queue.remove(makeJob(3).id);

      expect(queue.getStaleRatio()).toBe(0.5);

      // Compact
      queue.compact();

      // Stale ratio should be 0 after compaction
      expect(queue.getStaleRatio()).toBe(0);
      expect(queue.size).toBe(2);

      // Queue should still work correctly
      expect(queue.pop()?.id).toBe(makeJob(4).id); // priority 4 first
      expect(queue.pop()?.id).toBe(makeJob(2).id); // priority 2 second
    });

    test('should maintain heap property after compaction', () => {
      const queue = new IndexedPriorityQueue();

      // Push many jobs
      for (let i = 0; i < 10; i++) {
        queue.push(makeJob(i, i));
      }

      // Remove alternating jobs
      for (let i = 0; i < 10; i += 2) {
        queue.remove(makeJob(i).id);
      }

      // Compact
      queue.compact();

      // Pop should still give correct priority order
      const priorities: number[] = [];
      while (!queue.isEmpty) {
        const job = queue.pop();
        if (job) priorities.push(job.priority);
      }

      // Should be descending order (higher priority first)
      expect(priorities).toEqual([9, 7, 5, 3, 1]);
    });

    test('should handle compaction of empty queue', () => {
      const queue = new IndexedPriorityQueue();
      queue.compact(); // Should not throw
      expect(queue.size).toBe(0);
    });

    test('should handle stale ratio for empty queue', () => {
      const queue = new IndexedPriorityQueue();
      expect(queue.getStaleRatio()).toBe(0);
    });

    test('should create stale entries on priority update', () => {
      const queue = new IndexedPriorityQueue();
      queue.push(makeJob(1, 1));
      queue.push(makeJob(2, 2));

      // Update priority creates new heap entry, old one becomes stale
      queue.updatePriority(makeJob(1).id, 10);

      // 1 stale entry out of 3 heap entries
      expect(queue.getStaleRatio()).toBeCloseTo(1 / 3, 2);
    });
  });
});
