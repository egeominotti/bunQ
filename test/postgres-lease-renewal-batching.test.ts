import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import {
  cleanupPostgresNamespace,
  deferred,
  pausePostgresEventStream,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-lease-batch-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL lease renewal batching', () => {
  test.skipIf(!postgresUrl)(
    'renews one heartbeat batch without scalar renewals or projection reloads',
    async () => {
      const value = namespace('set-based');
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'lease-batch-broker',
          poolSize: 4,
          pollIntervalMs: 60_000,
          leaseDurationMs: 60_000,
        },
      });

      try {
        await manager.waitUntilReady();
        const queue = 'lease-batch';
        const ids = await manager.pushBatch(
          queue,
          Array.from({ length: 8 }, (_, index) => ({ data: { index } }))
        );
        const claimed = await manager.pullBatchWithLock(queue, ids.length, 'batch-worker');
        expect(claimed.jobs.map(({ id }) => id)).toEqual(ids);
        expect(claimed.tokens).toHaveLength(ids.length);

        await pausePostgresEventStream(manager);
        await Bun.sleep(50);
        const store = postgresManagerStore(manager);
        const originalRenew = store.renew.bind(store);
        const originalLoad = store.loadJobProjections.bind(store);
        let scalarRenewals = 0;
        let projectionLoads = 0;
        store.renew = async (...args) => {
          scalarRenewals++;
          return await originalRenew(...args);
        };
        store.loadJobProjections = async (...args) => {
          projectionLoads++;
          return await originalLoad(...args);
        };

        const renewed = await manager.heartbeatBatchDurable(
          claimed.jobs.map(({ id }) => id),
          claimed.tokens
        );

        expect(renewed).toBe(ids.length);
        expect(scalarRenewals).toBe(0);
        expect(projectionLoads).toBe(0);

        const rows = await store.context.sql<
          Array<{ id: string; lease_renewals: number; state: string }>
        >`
          SELECT id, lease_renewals, state
          FROM bunqueue_jobs
          WHERE namespace = ${value}
            AND id = ANY(${store.context.sql.array(ids.map(String), 'TEXT')})
          ORDER BY id
        `;
        expect(rows).toHaveLength(ids.length);
        expect(
          rows.every(({ state, lease_renewals }) => state === 'active' && lease_renewals === 1)
        ).toBe(true);
        for (const { id } of rows) {
          expect(manager.getLockInfo(id as JobId)?.renewalCount).toBe(1);
        }
      } finally {
        await manager.shutdownPostgres();
      }
    },
    15_000
  );

  test.skipIf(!postgresUrl)(
    'renews valid leases and repairs failed projections with one coalesced load',
    async () => {
      const value = namespace('mixed-fencing');
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'lease-mixed-broker',
          poolSize: 4,
          pollIntervalMs: 60_000,
          leaseDurationMs: 60_000,
        },
      });

      try {
        await manager.waitUntilReady();
        const queue = 'lease-mixed';
        const ids = await manager.pushBatch(
          queue,
          Array.from({ length: 4 }, (_, index) => ({ data: { index } }))
        );
        const claimed = await manager.pullBatchWithLock(queue, ids.length, 'mixed-worker');
        await pausePostgresEventStream(manager);
        await Bun.sleep(50);

        const store = postgresManagerStore(manager);
        const originalRenewMany = store.renewMany.bind(store);
        const originalLoad = store.loadJobProjections.bind(store);
        let renewalBatches = 0;
        const projectionBatches: number[] = [];
        store.renewMany = async (...args) => {
          renewalBatches++;
          return await originalRenewMany(...args);
        };
        store.loadJobProjections = async (...args) => {
          projectionBatches.push(args[0].length);
          return await originalLoad(...args);
        };
        const tokens = claimed.tokens.map((token, index) =>
          index % 2 === 0 ? token : `stale-${index}`
        );

        const renewed = await manager.heartbeatBatchDurable(
          claimed.jobs.map(({ id }) => id),
          tokens
        );

        expect(renewed).toBe(2);
        expect(renewalBatches).toBe(1);
        expect(projectionBatches).toEqual([2]);
        for (let index = 0; index < ids.length; index++) {
          expect(manager.getLockInfo(ids[index])?.renewalCount).toBe(index % 2 === 0 ? 1 : 0);
        }
      } finally {
        await manager.shutdownPostgres();
      }
    },
    15_000
  );

  test.skipIf(!postgresUrl)(
    'does not resurrect a completed projection when a committed renewal returns late',
    async () => {
      const value = namespace('terminal-race');
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'lease-terminal-race-broker',
          poolSize: 4,
          pollIntervalMs: 10,
          leaseDurationMs: 60_000,
        },
      });

      try {
        await manager.waitUntilReady();
        const queue = 'lease-terminal-race';
        const { id } = await manager.push(queue, { race: true });
        const claim = await manager.pullWithLock(queue, 'race-worker');
        expect(claim.job?.id).toBe(id);
        await pausePostgresEventStream(manager);
        await Bun.sleep(50);

        const store = postgresManagerStore(manager);
        const originalRenewMany = store.renewMany.bind(store);
        const renewalCommitted = deferred<undefined>();
        const releaseRenewal = deferred<undefined>();
        store.renewMany = async (...args) => {
          const rows = await originalRenewMany(...args);
          renewalCommitted.resolve(undefined);
          await releaseRenewal.promise;
          return rows;
        };

        const heartbeat = manager.heartbeatDurable(id, claim.token);
        try {
          await renewalCommitted.promise;
          await manager.ackBatchWithResults([
            { id, token: claim.token, result: { completed: true } },
          ]);
          expect(manager.getJobIndex().get(id)?.type).toBe('completed');
        } finally {
          releaseRenewal.resolve(undefined);
        }
        expect(await heartbeat).toBe(true);
        expect(manager.getJobIndex().get(id)?.type).toBe('completed');
        expect(manager.getLockInfo(id)).toBeNull();
      } finally {
        await manager.shutdownPostgres();
      }
    },
    15_000
  );

  test.skipIf(!postgresUrl)(
    'does not recreate a removed job when a committed renewal returns late',
    async () => {
      const value = namespace('removed-race');
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'lease-removed-race-broker',
          poolSize: 4,
          pollIntervalMs: 10,
          leaseDurationMs: 60_000,
        },
      });

      try {
        await manager.waitUntilReady();
        const queue = 'lease-removed-race';
        const { id } = await manager.push(queue, { race: true });
        const claim = await manager.pullWithLock(queue, 'race-worker');
        await pausePostgresEventStream(manager);
        await Bun.sleep(50);

        const store = postgresManagerStore(manager);
        const originalRenewMany = store.renewMany.bind(store);
        const renewalCommitted = deferred<undefined>();
        const releaseRenewal = deferred<undefined>();
        store.renewMany = async (...args) => {
          const rows = await originalRenewMany(...args);
          renewalCommitted.resolve(undefined);
          await releaseRenewal.promise;
          return rows;
        };

        const heartbeat = manager.heartbeatDurable(id, claim.token);
        try {
          await renewalCommitted.promise;
          await manager.ackBatchWithResults([
            {
              id,
              token: claim.token,
              result: { removed: true },
              removeOnComplete: true,
            },
          ]);
          expect(manager.getJobIndex().has(id)).toBe(false);
        } finally {
          releaseRenewal.resolve(undefined);
        }
        expect(await heartbeat).toBe(true);
        expect(manager.getJobIndex().has(id)).toBe(false);
        expect(manager.getJobs(queue, { state: 'active' })).toEqual([]);
      } finally {
        await manager.shutdownPostgres();
      }
    },
    15_000
  );
});
