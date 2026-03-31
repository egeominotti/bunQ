/**
 * Bug reproductions for GET /queues/:q/counts endpoint
 *
 * Bug 1: completed returns global count instead of per-queue
 * Bug 2: failed always 0 (user expectation: totalFailed per-queue counter)
 * Bug 3: Workers missing queue/concurrency fields
 * Bug 4: /queues/:q/workers not implemented
 * Bug 5: Per-queue totalCompleted/totalFailed counters missing
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Bug: getQueueJobCounts completed/failed per-queue', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============================================================
  // Bug 1: completed is global instead of per-queue
  // ============================================================
  describe('Bug 1: completed should be per-queue, not global', () => {
    test('completed count on queue-b should be 0 when only queue-a has completed jobs', async () => {
      // Setup: push to two separate queues
      await qm.push('queue-a', { data: { msg: 'a1' } });
      await qm.push('queue-b', { data: { msg: 'b1' } });

      // Pull and ack only queue-a's job
      const pulled = await qm.pull('queue-a');
      expect(pulled).not.toBeNull();
      await qm.ack(pulled!.id, { result: 'done' });

      // Check counts
      const countsA = qm.getQueueJobCounts('queue-a');
      const countsB = qm.getQueueJobCounts('queue-b');

      // queue-a should have 1 completed
      expect(countsA.completed).toBe(1);
      // BUG: queue-b currently returns 1 (global count) instead of 0
      expect(countsB.completed).toBe(0);
    });

    test('completed count should be accurate across 3 queues with different completion counts', async () => {
      // Complete 3 in queue-a, 1 in queue-b, 0 in queue-c
      for (let i = 0; i < 3; i++) {
        await qm.push('queue-a', { data: { idx: i } });
      }
      await qm.push('queue-b', { data: { msg: 'b' } });
      await qm.push('queue-c', { data: { msg: 'c' } });

      // Complete all 3 from queue-a
      for (let i = 0; i < 3; i++) {
        const p = await qm.pull('queue-a');
        await qm.ack(p!.id);
      }
      // Complete 1 from queue-b
      const pb = await qm.pull('queue-b');
      await qm.ack(pb!.id);
      // queue-c: no pulls, no completions

      const countsA = qm.getQueueJobCounts('queue-a');
      const countsB = qm.getQueueJobCounts('queue-b');
      const countsC = qm.getQueueJobCounts('queue-c');

      // BUG: currently all three return 4 (global total)
      expect(countsA.completed).toBe(3);
      expect(countsB.completed).toBe(1);
      expect(countsC.completed).toBe(0);
    });
  });

  // ============================================================
  // Bug 2: failed (DLQ) per-queue isolation
  // ============================================================
  describe('Bug 2: failed count per-queue isolation', () => {
    test('failed count should only reflect DLQ entries for that specific queue', async () => {
      // maxAttempts=1 so first fail goes directly to DLQ
      await qm.push('queue-a', { data: { msg: 'fail-a' }, maxAttempts: 1 });
      await qm.push('queue-b', { data: { msg: 'ok-b' } });

      // Fail only queue-a's job
      const pulledA = await qm.pull('queue-a');
      await qm.fail(pulledA!.id, 'intentional failure');

      const countsA = qm.getQueueJobCounts('queue-a');
      const countsB = qm.getQueueJobCounts('queue-b');

      expect(countsA.failed).toBe(1);
      expect(countsB.failed).toBe(0);
    });

    test('job with default maxAttempts=3 goes to delayed on first fail, not DLQ', async () => {
      // Default maxAttempts=3, first fail → retry (delayed), not DLQ
      await qm.push('queue-a', { data: { msg: 'retry-me' } });
      const pulled = await qm.pull('queue-a');
      await qm.fail(pulled!.id, 'first failure');

      const counts = qm.getQueueJobCounts('queue-a');
      // Job is in delayed (retry), not DLQ
      expect(counts.failed).toBe(0);
      expect(counts.delayed).toBe(1);
    });
  });

  // ============================================================
  // Bug 5: per-queue totalCompleted/totalFailed counters
  // ============================================================
  describe('Bug 5: per-queue totalCompleted and totalFailed counters', () => {
    test('counts should include totalCompleted per-queue', async () => {
      // Complete jobs in different queues
      await qm.push('queue-a', { data: { msg: 'a1' } });
      await qm.push('queue-a', { data: { msg: 'a2' } });
      await qm.push('queue-b', { data: { msg: 'b1' } });

      // Complete 2 in queue-a, 1 in queue-b
      for (let i = 0; i < 2; i++) {
        const p = await qm.pull('queue-a');
        await qm.ack(p!.id);
      }
      const pb = await qm.pull('queue-b');
      await qm.ack(pb!.id);

      const countsA = qm.getQueueJobCounts('queue-a');
      const countsB = qm.getQueueJobCounts('queue-b');

      // totalCompleted should be per-queue cumulative
      expect((countsA as any).totalCompleted).toBe(2);
      expect((countsB as any).totalCompleted).toBe(1);
    });

    test('counts should include totalFailed per-queue', async () => {
      // Fail jobs in different queues (maxAttempts=1 for immediate DLQ)
      await qm.push('queue-a', { data: { msg: 'f1' }, maxAttempts: 1 });
      await qm.push('queue-a', { data: { msg: 'f2' }, maxAttempts: 1 });
      await qm.push('queue-b', { data: { msg: 'f3' }, maxAttempts: 1 });

      // Fail 2 in queue-a
      for (let i = 0; i < 2; i++) {
        const p = await qm.pull('queue-a');
        await qm.fail(p!.id, 'failure');
      }
      // Fail 1 in queue-b
      const pb = await qm.pull('queue-b');
      await qm.fail(pb!.id, 'failure');

      const countsA = qm.getQueueJobCounts('queue-a');
      const countsB = qm.getQueueJobCounts('queue-b');

      expect((countsA as any).totalFailed).toBe(2);
      expect((countsB as any).totalFailed).toBe(1);
    });
  });
});
