import { describe, expect, test } from 'bun:test';
import type { QueueManager } from '../src/application/queueManager';
import type { QueueJobCounts } from '../src/application/queueStatsAggregator';
import { QueueCountsScheduler } from '../src/infrastructure/server/queueCountsScheduler';

const counts: QueueJobCounts = {
  waiting: 1,
  prioritized: 0,
  delayed: 0,
  active: 0,
  completed: 0,
  failed: 0,
  'waiting-children': 0,
  totalCompleted: 0,
  totalFailed: 0,
};

describe('QueueCountsScheduler', () => {
  test('coalesces repeated events and aggregates all pending queues once', async () => {
    const batches: string[][] = [];
    const emitted: string[] = [];
    const manager = {
      getQueueJobCountsBatch(queueNames: Iterable<string>) {
        const queues = Array.from(queueNames);
        batches.push(queues);
        return new Map(queues.map((queue) => [queue, counts]));
      },
    } as QueueManager;
    const scheduler = new QueueCountsScheduler(manager, (queue) => emitted.push(queue));

    for (let i = 0; i < 1_000; i++) scheduler.schedule('queue-a');
    scheduler.schedule('queue-b');
    await Bun.sleep(30);

    expect(batches).toEqual([['queue-a', 'queue-b']]);
    expect(emitted).toEqual(['queue-a', 'queue-b']);
    scheduler.stop();
  });
});
