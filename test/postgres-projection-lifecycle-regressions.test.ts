import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import {
  cleanupPostgresNamespace,
  eventually,
  pausePostgresEventStream,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-projection-lifecycle-${label}-${Date.now()}-${crypto.randomUUID()}`;
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

describe('PostgreSQL projection lifecycle regressions', () => {
  test.skipIf(!postgresUrl)(
    'does not reject a committed push when its local projection refresh fails',
    async () => {
      const value = namespace('push-refresh');
      const broker = manager(value, 'push-refresh');
      try {
        await broker.waitUntilReady();
        const store = postgresManagerStore(broker);
        const loadJobProjections = store.loadJobProjections.bind(store);
        let injected = false;
        store.loadJobProjections = async (requests) => {
          if (!injected) {
            injected = true;
            throw new Error('injected post-commit projection failure');
          }
          return await loadJobProjections(requests);
        };

        const job = await broker.push('projection', { data: { request: 'once' } });
        store.loadJobProjections = loadJobProjections;
        const rows = await store.list('projection', { limit: 100, asc: true });

        expect(rows.map((row) => row.job.id)).toEqual([job.id]);
        expect(
          await eventually(() => broker.getJobs('projection').some(({ id }) => id === job.id))
        ).toBe(true);
        expect(await eventually(() => store.health().ok)).toBe(true);
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not reject a committed acknowledgement when projection refresh fails',
    async () => {
      const value = namespace('ack-refresh');
      const broker = manager(value, 'ack-refresh');
      try {
        await broker.waitUntilReady();
        const job = await broker.push('projection', { data: {} });
        const claim = await broker.pullWithLock('projection', 'worker', 0, 60_000);
        const store = postgresManagerStore(broker);
        const loadJobProjections = store.loadJobProjections.bind(store);
        let injected = false;
        store.loadJobProjections = async (requests) => {
          if (!injected) {
            injected = true;
            throw new Error('injected ACK projection failure');
          }
          return await loadJobProjections(requests);
        };

        await expect(broker.ack(job.id, { done: true }, claim.token!)).resolves.toBeUndefined();
        store.loadJobProjections = loadJobProjections;
        expect((await store.getJob(job.id))?.state).toBe('completed');
        expect(await eventually(() => broker.getCompletedJobs().has(job.id))).toBe(true);
        expect(await eventually(() => store.health().ok)).toBe(true);
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps a local claim token when earlier journal events catch up',
    async () => {
      const value = namespace('claim-event-fence');
      const broker = manager(value, 'claim-event-fence');
      try {
        await broker.waitUntilReady();
        const stream = await pausePostgresEventStream(broker);
        const job = await broker.push('projection', { data: {} });
        const claim = await broker.pullWithLock('projection', 'worker', 0, 60_000);

        expect(broker.verifyLock(job.id, claim.token!)).toBe(true);
        await stream.drain();
        expect(broker.verifyLock(job.id, claim.token!)).toBe(true);
        await expect(broker.ack(job.id, { done: true })).resolves.toBeUndefined();
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );
});
