import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { PostgresStartupEventBuffer } from '../src/application/postgres-queue-manager/startupEventBuffer';
import { releaseClientJobsWithRetry } from '../src/infrastructure/server/tcp/clientRelease';
import {
  cleanupPostgresNamespace,
  deferred,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-async-lifecycle-${label}-${Date.now()}-${crypto.randomUUID()}`;
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

describe('PostgreSQL asynchronous lifecycle regressions', () => {
  test.skipIf(!postgresUrl)(
    'retries every client lease after a transient release failure',
    async () => {
      const value = namespace('client-release');
      const broker = manager(value, 'client-release');
      try {
        await broker.waitUntilReady();
        const ids = await broker.pushBatch('release', [
          { data: { index: 1 } },
          { data: { index: 2 } },
        ]);
        await broker.pullBatchWithLock('release', 2, 'worker', 0, 60_000);
        for (const id of ids) broker.registerClientJob('client', id);

        const store = postgresManagerStore(broker);
        const releaseClientLease = store.releaseClientLease.bind(store);
        let attempts = 0;
        store.releaseClientLease = async (id, token) => {
          attempts++;
          if (attempts === 1) throw new Error('injected transient release failure');
          return await releaseClientLease(id, token);
        };

        expect(await releaseClientJobsWithRetry(broker, 'client', 3)).toBe(2);
        store.releaseClientLease = releaseClientLease;
        expect(await store.list('release', { states: ['active'], limit: 100 })).toHaveLength(0);
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'drains in-flight maintenance before store close resolves',
    async () => {
      const value = namespace('maintenance-drain');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'maintenance-drain',
        pollIntervalMs: 25,
      });
      const entered = deferred<undefined>();
      const release = deferred<undefined>();
      let finished = false;
      try {
        await store.initialize();
        const runtime = store as unknown as {
          runMaintenance(operation: () => Promise<unknown>, subsystem: string): Promise<void>;
        };
        const maintenance = runtime.runMaintenance(async () => {
          entered.resolve(undefined);
          await release.promise;
          finished = true;
        }, 'blocked');
        await entered.promise;

        let closed = false;
        const closing = store.close().then(() => {
          closed = true;
        });
        await Bun.sleep(50);
        expect(closed).toBe(false);
        release.resolve(undefined);
        await Promise.all([maintenance, closing]);
        expect(finished).toBe(true);
      } finally {
        release.resolve(undefined);
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)('serializes maintenance executions for one subsystem', async () => {
    const value = namespace('maintenance-single-flight');
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace: value,
      brokerId: 'maintenance-single-flight',
    });
    const firstEntered = deferred<undefined>();
    const releaseFirst = deferred<undefined>();
    let active = 0;
    let maximumActive = 0;
    try {
      await store.initialize();
      const runtime = store as unknown as {
        runMaintenance(operation: () => Promise<unknown>, subsystem: string): Promise<void>;
      };
      const operation = async () => {
        active++;
        maximumActive = Math.max(maximumActive, active);
        if (maximumActive === 1) {
          firstEntered.resolve(undefined);
          await releaseFirst.promise;
        }
        active--;
      };
      const first = runtime.runMaintenance(operation, 'shared');
      await firstEntered.promise;
      const second = runtime.runMaintenance(operation, 'shared');
      await Bun.sleep(25);
      expect(maximumActive).toBe(1);
      releaseFirst.resolve(undefined);
      await Promise.all([first, second]);
      expect(maximumActive).toBe(1);
    } finally {
      releaseFirst.resolve(undefined);
      await store.close();
    }
  });

  test.skipIf(!postgresUrl)(
    'lets shutdown cancel a queue refresh under continuous churn',
    async () => {
      const value = namespace('refresh-shutdown');
      const broker = manager(value, 'refresh-shutdown');
      try {
        await broker.waitUntilReady();
        await broker.push('hot', { data: {} });
        const internals = broker as unknown as {
          postgresStore: PostgresQueueStore;
          queueEventVersions: Map<string, number>;
          refreshQueue(queue: string): Promise<void>;
          runPostgresOperation<T>(operation: () => Promise<T>): Promise<T>;
        };
        const loadQueueReadModel = internals.postgresStore.loadQueueReadModel.bind(
          internals.postgresStore
        );
        let attempts = 0;
        internals.postgresStore.loadQueueReadModel = async (queue) => {
          const snapshot = await loadQueueReadModel(queue);
          attempts++;
          internals.queueEventVersions.set(
            'hot',
            (internals.queueEventVersions.get('hot') ?? 0) + 1
          );
          return snapshot;
        };
        const refresh = internals.runPostgresOperation(() => internals.refreshQueue('hot'));
        while (attempts < 1) await Bun.sleep(5);

        const shutdown = broker.shutdownPostgres();
        const closedUnderChurn = await Promise.race([
          shutdown.then(() => true),
          Bun.sleep(500).then(() => false),
        ]);
        internals.postgresStore.loadQueueReadModel = loadQueueReadModel;
        await refresh;
        await shutdown;
        expect(closedUnderChurn).toBe(true);
      } finally {
        await broker.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'hydrates while its startup event buffer keeps overflowing',
    async () => {
      const value = namespace('startup-overflow');
      const producer = manager(value, 'startup-overflow-producer');
      let joining: PostgresQueueManager | null = null;
      let inject = true;
      try {
        await producer.waitUntilReady();
        joining = manager(value, 'startup-overflow-joining');
        const internals = joining as unknown as {
          snapshotEventBuffer: PostgresStartupEventBuffer;
        };
        internals.snapshotEventBuffer = new PostgresStartupEventBuffer(1);
        const store = postgresManagerStore(joining);
        const loadSnapshot = store.loadManagerSnapshot.bind(store);
        store.loadManagerSnapshot = async () => {
          if (inject) {
            await producer.pushBatch('hot', [{ data: {} }, { data: {} }]);
            await (store.events as unknown as { drain(): Promise<void> }).drain();
          }
          return await loadSnapshot();
        };

        const becameReady = await Promise.race([
          joining.waitUntilReady().then(() => true),
          Bun.sleep(750).then(() => false),
        ]);
        inject = false;
        expect(becameReady).toBe(true);
      } finally {
        inject = false;
        await Promise.allSettled([producer.shutdownPostgres(), joining?.shutdownPostgres()]);
      }
    }
  );
});
