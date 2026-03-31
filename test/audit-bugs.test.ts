/**
 * Audit Bug Tests - Reproduce and verify fixes for:
 * 1. finalizeBatchAck skips repeat job scheduling for batches > 4
 * 2. finalizeBatchAck missing throughput tracking
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { type JobId } from '../src/domain/types/job';

describe('Audit Bugs - Batch Ack Repeat & Throughput', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============ Repeat scheduling in batch ack ============

  describe('ackJobBatch should schedule repeat jobs (batch > 4)', () => {
    test('single ackJob schedules repeat job correctly', async () => {
      // Baseline: single ack with repeat config works
      await qm.push('repeat-q', {
        data: { msg: 'repeating' },
        repeat: { every: 1000, limit: 3 },
      });
      const pulled = await qm.pull('repeat-q');
      expect(pulled).not.toBeNull();

      await qm.ack(pulled!.id);

      const stats = qm.getStats();
      expect(stats.completed).toBe(1);
      // The repeat mechanism should have requeued a new job
      expect(stats.waiting + stats.delayed).toBeGreaterThanOrEqual(1);
    });

    test('ackJobBatch (>4 jobs) schedules repeat jobs', async () => {
      // Bug: batches > 4 use finalizeBatchAck which skips onRepeat
      const ids: JobId[] = [];
      for (let i = 0; i < 6; i++) {
        await qm.push('repeat-q', {
          data: { msg: `repeat-${i}` },
          repeat: { every: 1000, limit: 3 },
        });
      }
      for (let i = 0; i < 6; i++) {
        const pulled = await qm.pull('repeat-q');
        expect(pulled).not.toBeNull();
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      // Allow fire-and-forget repeat pushes to complete
      await new Promise((r) => setTimeout(r, 50));

      const stats = qm.getStats();
      expect(stats.completed).toBe(6);
      // All 6 repeat jobs should have been rescheduled
      expect(stats.waiting + stats.delayed).toBeGreaterThanOrEqual(6);
    });

    test('ackJobBatch (>4 jobs) respects repeat limit', async () => {
      // Jobs at their repeat limit should NOT be rescheduled
      const ids: JobId[] = [];
      for (let i = 0; i < 5; i++) {
        await qm.push('repeat-q', {
          data: { msg: `at-limit-${i}` },
          repeat: { every: 1000, limit: 1, count: 1 },
        });
      }
      for (let i = 0; i < 5; i++) {
        const pulled = await qm.pull('repeat-q');
        expect(pulled).not.toBeNull();
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      const stats = qm.getStats();
      expect(stats.completed).toBe(5);
      // Jobs at their limit should NOT be rescheduled
      expect(stats.waiting + stats.delayed).toBe(0);
    });

    test('ackJobBatch (>4 jobs) with no repeat config does not reschedule', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 6; i++) {
        await qm.push('normal-q', {
          data: { msg: `normal-${i}` },
        });
      }
      for (let i = 0; i < 6; i++) {
        const pulled = await qm.pull('normal-q');
        expect(pulled).not.toBeNull();
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      const stats = qm.getStats();
      expect(stats.completed).toBe(6);
      expect(stats.waiting + stats.delayed).toBe(0);
    });
  });

  // ============ Repeat scheduling in batch ack with results ============

  describe('ackJobBatchWithResults should schedule repeat jobs (batch > 4)', () => {
    test('ackJobBatchWithResults (>4 jobs) schedules repeat jobs', async () => {
      const items: Array<{ id: JobId; result: unknown }> = [];
      for (let i = 0; i < 6; i++) {
        await qm.push('repeat-q', {
          data: { msg: `repeat-result-${i}` },
          repeat: { every: 1000, limit: 3 },
        });
      }
      for (let i = 0; i < 6; i++) {
        const pulled = await qm.pull('repeat-q');
        expect(pulled).not.toBeNull();
        items.push({ id: pulled!.id, result: { value: i } });
      }

      await qm.ackBatchWithResults(items);

      // Allow fire-and-forget repeat pushes to complete
      await new Promise((r) => setTimeout(r, 50));

      const stats = qm.getStats();
      expect(stats.completed).toBe(6);
      // All 6 repeat jobs should have been rescheduled
      expect(stats.waiting + stats.delayed).toBeGreaterThanOrEqual(6);
    });

    test('ackJobBatchWithResults (>4 jobs) respects repeat limit', async () => {
      const items: Array<{ id: JobId; result: unknown }> = [];
      for (let i = 0; i < 5; i++) {
        await qm.push('repeat-q', {
          data: { msg: `at-limit-${i}` },
          repeat: { every: 1000, limit: 1, count: 1 },
        });
      }
      for (let i = 0; i < 5; i++) {
        const pulled = await qm.pull('repeat-q');
        expect(pulled).not.toBeNull();
        items.push({ id: pulled!.id, result: { done: true } });
      }

      await qm.ackBatchWithResults(items);

      const stats = qm.getStats();
      expect(stats.completed).toBe(5);
      expect(stats.waiting + stats.delayed).toBe(0);
    });
  });

  // ============ Throughput tracking in batch ack ============

  describe('finalizeBatchAck should track throughput', () => {
    test('ackJobBatch (>4 jobs) updates totalCompleted counter', async () => {
      const ids: JobId[] = [];
      for (let i = 0; i < 6; i++) {
        await qm.push('throughput-q', { data: { id: i } });
      }
      for (let i = 0; i < 6; i++) {
        const pulled = await qm.pull('throughput-q');
        ids.push(pulled!.id);
      }

      await qm.ackBatch(ids);

      const stats = qm.getStats();
      expect(Number(stats.totalCompleted)).toBe(6);
    });

    test('ackJobBatchWithResults (>4 jobs) updates totalCompleted counter', async () => {
      const items: Array<{ id: JobId; result: unknown }> = [];
      for (let i = 0; i < 6; i++) {
        await qm.push('throughput-q', { data: { id: i } });
      }
      for (let i = 0; i < 6; i++) {
        const pulled = await qm.pull('throughput-q');
        items.push({ id: pulled!.id, result: { v: i } });
      }

      await qm.ackBatchWithResults(items);

      const stats = qm.getStats();
      expect(Number(stats.totalCompleted)).toBe(6);
    });
  });
});
