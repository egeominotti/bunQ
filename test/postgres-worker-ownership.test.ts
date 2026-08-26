import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { cleanupPostgresNamespace, postgresManagerStore } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-worker-owner-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL worker ownership fencing', () => {
  test.skipIf(!postgresUrl)(
    'rejects a stale unregister after another broker takes ownership',
    async () => {
      const value = namespace('unregister');
      const first = manager(value, 'worker-owner-a');
      const second = manager(value, 'worker-owner-b');
      const workerId = `worker-${crypto.randomUUID()}`;

      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await first.registerWorkerDurable('old', ['queue'], 1, {
          workerId,
          clientId: 'old-client',
        });
        await second.registerWorkerDurable('new', ['queue'], 1, {
          workerId,
          clientId: 'new-client',
        });
        expect((await second.listWorkersDurable()).map((worker) => worker.clientId)).toEqual([
          'new-client',
        ]);

        expect(await first.unregisterWorkerDurable(workerId)).toBe(false);
        expect(await second.listWorkersDurable()).toMatchObject([
          { id: workerId, name: 'new', clientId: 'new-client' },
        ]);
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'rejects a stale heartbeat after another broker takes ownership',
    async () => {
      const value = namespace('heartbeat');
      const first = manager(value, 'heartbeat-owner-a');
      const second = manager(value, 'heartbeat-owner-b');
      const workerId = `worker-${crypto.randomUUID()}`;

      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await first.registerWorkerDurable('old', ['queue'], 1, {
          workerId,
          clientId: 'old-client',
        });
        await second.registerWorkerDurable('new', ['queue'], 1, {
          workerId,
          clientId: 'new-client',
        });

        expect(await first.heartbeatWorkerDurable(workerId, { processed: 99 }, 'old-client')).toBe(
          false
        );
        const [authoritative] = await postgresManagerStore(second).listWorkers();
        expect(authoritative).toMatchObject({
          id: workerId,
          name: 'new',
          clientId: 'new-client',
          processedJobs: 0,
        });
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );
});
