import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { PostgresCounts } from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-dashboard-projection-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL dashboard projection', () => {
  test.skipIf(!postgresUrl)('aggregates all queue counts with one snapshot pass', async () => {
    const value = namespace('single-pass');
    const manager = new PostgresQueueManager({
      postgres: { url: postgresUrl!, namespace: value, brokerId: 'dashboard-projection-broker' },
    });

    try {
      await manager.waitUntilReady();
      const queues = ['dashboard-a', 'dashboard-b', 'dashboard-c', 'dashboard-d'];
      for (const queue of queues) {
        await manager.pushBatch(
          queue,
          Array.from({ length: 5 }, (_, index) => ({ data: { queue, index } }))
        );
      }

      const state = manager as unknown as {
        postgresSnapshot: {
          counts(queue?: string): PostgresCounts;
          countsByQueue(): ReadonlyMap<string, PostgresCounts>;
        };
      };
      const originalCounts = state.postgresSnapshot.counts.bind(state.postgresSnapshot);
      const originalCountsByQueue = state.postgresSnapshot.countsByQueue.bind(
        state.postgresSnapshot
      );
      let individualScans = 0;
      let aggregateScans = 0;
      state.postgresSnapshot.counts = (queue?: string) => {
        individualScans++;
        return originalCounts(queue);
      };
      state.postgresSnapshot.countsByQueue = () => {
        aggregateScans++;
        return originalCountsByQueue();
      };

      const stats = manager.getPerQueueStats();
      const summary = manager.getQueuesSummary();

      expect(individualScans).toBe(0);
      expect(aggregateScans).toBe(2);
      expect([...stats.keys()]).toEqual(queues);
      expect([...stats.values()].every(({ waiting }) => waiting === 5)).toBe(true);
      expect(summary.map(({ name }) => name)).toEqual(queues);
      expect(summary.every(({ counts }) => counts.waiting === 5)).toBe(true);
    } finally {
      await manager.shutdownPostgres();
    }
  });
});
