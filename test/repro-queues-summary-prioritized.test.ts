import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { QueueManager } from '../src/application/queueManager';

describe('QueueManager.getQueuesSummary prioritized counts', () => {
  test('includes prioritized jobs instead of dropping them from the per-state summary', async () => {
    const manager = new QueueManager();
    const queue = `queues-summary-prioritized-${randomUUID()}`;

    try {
      await manager.push(queue, { data: { kind: 'waiting' }, priority: 0 });
      await manager.push(queue, { data: { kind: 'prioritized' }, priority: 10 });

      expect(manager.getQueueJobCounts(queue)).toMatchObject({
        waiting: 1,
        prioritized: 1,
      });

      const summary = manager.getQueuesSummary().find((entry) => entry.name === queue);
      expect(summary?.counts).toEqual({
        waiting: 1,
        prioritized: 1,
        active: 0,
        completed: 0,
        failed: 0,
        delayed: 0,
      });
    } finally {
      manager.shutdown();
    }
  });
});
