import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  cleanupPostgresNamespace,
  deferred,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-shutdown-admission-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function store(value: string, brokerId: string): PostgresQueueStore {
  return new PostgresQueueStore({
    url: postgresUrl!,
    namespace: value,
    brokerId,
    pollIntervalMs: 25,
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
}, 30_000);

describe('PostgreSQL shutdown admission boundaries', () => {
  test.skipIf(!postgresUrl)(
    'drains a claim committed before shutdown and returns the delivery once',
    async () => {
      const value = namespace('claim');
      const broker = manager(value, 'claim-manager');
      const verifier = store(value, 'claim-verifier');
      const claimCommitted = deferred<undefined>();
      const releaseClaim = deferred<undefined>();
      let shutdown: Promise<void> | null = null;

      try {
        await Promise.all([broker.waitUntilReady(), verifier.initialize()]);
        const job = await broker.push('claim-shutdown', { data: { delivery: 'once' } });
        const brokerStore = postgresManagerStore(broker);
        const claim = brokerStore.claim.bind(brokerStore);
        brokerStore.claim = async (queue, count, owner, leaseDurationMs) => {
          const claimed = await claim(queue, count, owner, leaseDurationMs);
          claimCommitted.resolve(undefined);
          await releaseClaim.promise;
          return claimed;
        };

        const delivery = broker.pull('claim-shutdown');
        void delivery.catch(() => undefined);
        await claimCommitted.promise;
        expect((await verifier.getJob(job.id))?.state).toBe('active');

        const internals = broker as unknown as { operations: { active: number } };
        expect(internals.operations.active).toBe(1);
        shutdown = broker.shutdownPostgres();
        releaseClaim.resolve(undefined);

        expect((await delivery)?.id).toBe(job.id);
        await shutdown;
        const [row] = await verifier.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_jobs
          WHERE namespace = ${value} AND id = ${String(job.id)}
        `;
        expect(row.count).toBe(1);
      } finally {
        releaseClaim.resolve(undefined);
        await Promise.allSettled([shutdown ?? broker.shutdownPostgres(), verifier.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not let an empty long poll hold shutdown open',
    async () => {
      const value = namespace('long-poll');
      const broker = manager(value, 'long-poll-manager');
      const waitEntered = deferred<undefined>();
      const releaseWait = deferred<undefined>();
      let shutdown: Promise<void> | null = null;

      try {
        await broker.waitUntilReady();
        const brokerStore = postgresManagerStore(broker);
        brokerStore.waitForWork = async () => {
          waitEntered.resolve(undefined);
          await releaseWait.promise;
        };

        const delivery = broker.pull('empty-long-poll', 60_000);
        void delivery.catch(() => undefined);
        await waitEntered.promise;

        shutdown = broker.shutdownPostgres();
        await expect(shutdown).resolves.toBeUndefined();
        releaseWait.resolve(undefined);
        await expect(delivery).rejects.toThrow('PostgreSQL queue manager is shutting down');
      } finally {
        releaseWait.resolve(undefined);
        await Promise.allSettled([shutdown ?? broker.shutdownPostgres()]);
      }
    },
    10_000
  );

  test.skipIf(!postgresUrl)('waits for manager hydration before closing PostgreSQL', async () => {
    const value = namespace('hydration');
    const broker = manager(value, 'hydration-manager');
    const hydrationEntered = deferred<undefined>();
    const releaseHydration = deferred<undefined>();
    const brokerStore = postgresManagerStore(broker);
    const loadManagerSnapshot = brokerStore.loadManagerSnapshot.bind(brokerStore);
    brokerStore.loadManagerSnapshot = async () => {
      hydrationEntered.resolve(undefined);
      await releaseHydration.promise;
      return await loadManagerSnapshot();
    };
    let shutdown: Promise<void> | null = null;
    let shutdownComplete = false;

    try {
      await hydrationEntered.promise;
      shutdown = broker.shutdownPostgres().then(() => {
        shutdownComplete = true;
      });
      await Promise.resolve();
      expect(shutdownComplete).toBe(false);

      releaseHydration.resolve(undefined);
      await expect(broker.waitUntilReady()).resolves.toBeUndefined();
      await expect(shutdown).resolves.toBeUndefined();
      expect(shutdownComplete).toBe(true);
    } finally {
      releaseHydration.resolve(undefined);
      await Promise.allSettled([shutdown ?? broker.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)(
    'lets an admitted relationship operation finish nested mutations during shutdown',
    async () => {
      const value = namespace('nested');
      const broker = manager(value, 'nested-manager');
      const verifier = store(value, 'nested-verifier');
      const removalCommitted = deferred<undefined>();
      const releaseRemoval = deferred<undefined>();
      let shutdown: Promise<void> | null = null;

      try {
        await Promise.all([broker.waitUntilReady(), verifier.initialize()]);
        const parent = await broker.push('nested-parent', { data: { parent: true } });
        const children = await Promise.all([
          broker.push('nested-child', { data: { child: 1 } }),
          broker.push('nested-child', { data: { child: 2 } }),
        ]);
        for (const child of children) await broker.updateJobParent(child.id, parent.id);

        const brokerStore = postgresManagerStore(broker);
        const removeUnprocessedChildren = brokerStore.removeUnprocessedChildren.bind(brokerStore);
        brokerStore.removeUnprocessedChildren = async (parentId) => {
          const removed = await removeUnprocessedChildren(parentId);
          removalCommitted.resolve(undefined);
          await releaseRemoval.promise;
          return removed;
        };

        const removal = broker.removeUnprocessedChildren(parent.id);
        void removal.catch(() => undefined);
        await removalCommitted.promise;
        shutdown = broker.shutdownPostgres();
        releaseRemoval.resolve(undefined);

        await expect(removal).resolves.toBeUndefined();
        await expect(shutdown).resolves.toBeUndefined();
        for (const child of children) await expect(verifier.getJob(child.id)).resolves.toBeNull();
        const storedParent = await verifier.getJob(parent.id);
        expect(storedParent?.state).toBe('waiting');
        expect(storedParent?.job.childrenIds).toEqual([]);
        expect(storedParent?.job.dependsOn).toEqual([]);
        await expect(verifier.removeUnprocessedChildren(parent.id)).resolves.toEqual([]);
      } finally {
        releaseRemoval.resolve(undefined);
        await Promise.allSettled([shutdown ?? broker.shutdownPostgres(), verifier.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'rejects late synchronous work but keeps disconnect cleanup non-throwing',
    async () => {
      const value = namespace('sync');
      const broker = manager(value, 'sync-manager');
      const verifier = store(value, 'sync-verifier');

      try {
        await Promise.all([broker.waitUntilReady(), verifier.initialize()]);
        broker.registerWorker('cleanup-worker', ['sync'], 1, {
          workerId: 'cleanup-worker',
          clientId: 'cleanup-client',
        });
        await broker.flushPostgresWrites();
        await broker.shutdownPostgres();

        expect(() => broker.pause('late-sync-work')).toThrow(
          'PostgreSQL queue manager is shutting down'
        );
        expect(broker.isPaused('late-sync-work')).toBe(false);
        expect(() => broker.unregisterWorkersByClientId('cleanup-client')).not.toThrow();
        expect(broker.unregisterWorkersByClientId('cleanup-client')).toBe(0);
        expect(() => broker.forceReleaseClientJobs('cleanup-client')).not.toThrow();
        expect(await verifier.listWorkers()).toEqual([]);
      } finally {
        await Promise.allSettled([broker.shutdownPostgres(), verifier.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'rejects every asynchronous surface before it can touch a closed pool',
    async () => {
      const value = namespace('late-async');
      const broker = manager(value, 'late-async-manager');
      await broker.waitUntilReady();
      await broker.shutdownPostgres();
      const id = jobId('late-operation');
      const operations: Array<() => Promise<unknown>> = [
        () => broker.push('late', { data: {} }),
        () => broker.pushBatch('late', [{ data: {} }]),
        () => broker.pushFlow({ jobs: [{ id, queue: 'late', input: { data: {} } }] }),
        () => broker.pull('late'),
        () => broker.extendLock(id, 'late-token', 1_000),
        () => broker.updateJobParent(id, jobId('late-parent')),
        () => broker.retryDlqDurable('late'),
        () => broker.pauseDurable('late', true),
        () => broker.registerWorkerDurable('late-worker', ['late']),
        () => broker.cancel(id),
        () => broker.getJob(id),
        () => broker.readCloudSnapshotDurable(),
        () => broker.ack(id, undefined, 'late-token'),
      ];

      for (const operation of operations) {
        await expect(operation()).rejects.toThrow('PostgreSQL queue manager is shutting down');
      }
      await expect(broker.flushPostgresWrites()).resolves.toBeUndefined();
    }
  );
});
