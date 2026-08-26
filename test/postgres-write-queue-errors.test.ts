import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { PostgresDeferredWriteQueue } from '../src/application/postgres-queue-manager/deferredWrites';
import {
  cleanupPostgresNamespace,
  deferred,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

afterAll(async () => {
  if (!postgresUrl) return;
  for (const namespace of namespaces) await cleanupPostgresNamespace(postgresUrl, namespace);
});

describe('PostgreSQL deferred write errors', () => {
  test('closes admission atomically and drains writes accepted earlier', async () => {
    const writes = new PostgresDeferredWriteQueue();
    const entered = deferred<undefined>();
    const release = deferred<undefined>();
    writes.enqueue(async () => {
      entered.resolve(undefined);
      await release.promise;
    });
    await entered.promise;

    const drain = writes.closeAndDrain();
    expect(() => writes.enqueue(async () => undefined)).toThrow(
      'PostgreSQL queue manager is shutting down'
    );

    release.resolve(undefined);
    await expect(drain).resolves.toEqual([]);
    await expect(writes.flush()).resolves.toBeUndefined();
  });

  test('reports one checkpoint failure to every concurrent drain observer', async () => {
    const writes = new PostgresDeferredWriteQueue();
    writes.enqueue(async () => {
      throw new Error('shared checkpoint failure');
    });

    const outcomes = await Promise.allSettled([writes.flush(), writes.flush()]);
    expect(outcomes.map((outcome) => outcome.status)).toEqual(['rejected', 'rejected']);
    expect(
      outcomes.map((outcome) =>
        outcome.status === 'rejected' ? String(outcome.reason) : 'unexpected success'
      )
    ).toEqual(['Error: shared checkpoint failure', 'Error: shared checkpoint failure']);
    await expect(writes.flush()).resolves.toBeUndefined();
  });

  test.skipIf(!postgresUrl)(
    'does not forget an earlier failure after a later write succeeds',
    async () => {
      const namespace = `test-write-errors-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(namespace);
      const manager = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace, brokerId: 'write-errors' },
      });

      try {
        await manager.waitUntilReady();
        const store = postgresManagerStore(manager);
        const originalSaveWorker = store.saveWorker;
        let calls = 0;
        store.saveWorker = async (worker) => {
          calls++;
          if (calls === 1) throw new Error('synthetic first write failure');
          await originalSaveWorker(worker);
        };

        manager.registerWorker('first', ['queue'], 1, { workerId: 'first-worker' });
        manager.registerWorker('second', ['queue'], 1, { workerId: 'second-worker' });

        await expect(manager.flushPostgresWrites()).rejects.toThrow(
          'synthetic first write failure'
        );
        expect(calls).toBe(2);
        expect((await store.listWorkers()).map((worker) => worker.id)).toEqual(['second-worker']);
        await expect(manager.flushPostgresWrites()).resolves.toBeUndefined();
      } finally {
        await manager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)('reports multiple deferred failures in write order', async () => {
    const namespace = `test-write-order-${Date.now()}-${crypto.randomUUID()}`;
    namespaces.push(namespace);
    const manager = new PostgresQueueManager({
      postgres: { url: postgresUrl!, namespace, brokerId: 'write-order' },
    });

    try {
      await manager.waitUntilReady();
      const store = postgresManagerStore(manager);
      const originalSaveWorker = store.saveWorker;
      let calls = 0;
      store.saveWorker = async (worker) => {
        calls++;
        if (calls <= 2) throw new Error(`synthetic ordered failure ${calls}`);
        await originalSaveWorker(worker);
      };

      manager.registerWorker('first', ['queue'], 1, { workerId: 'first-worker' });
      manager.registerWorker('second', ['queue'], 1, { workerId: 'second-worker' });
      manager.registerWorker('third', ['queue'], 1, { workerId: 'third-worker' });

      let failure: unknown;
      try {
        await manager.flushPostgresWrites();
      } catch (error) {
        failure = error;
      }
      expect(failure).toBeInstanceOf(AggregateError);
      expect(
        (failure as AggregateError).errors.map((error) =>
          error instanceof Error ? error.message : String(error)
        )
      ).toEqual(['synthetic ordered failure 1', 'synthetic ordered failure 2']);
      expect(calls).toBe(3);
      expect((await store.listWorkers()).map((worker) => worker.id)).toEqual(['third-worker']);
      await expect(manager.flushPostgresWrites()).resolves.toBeUndefined();
    } finally {
      await manager.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)(
    'drains and closes before reporting a deferred write failure',
    async () => {
      const namespace = `test-write-shutdown-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(namespace);
      const manager = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace, brokerId: 'write-shutdown' },
      });
      let closed = false;

      try {
        await manager.waitUntilReady();
        const store = postgresManagerStore(manager);
        const originalSaveWorker = store.saveWorker;
        const originalClose = store.close.bind(store);
        let calls = 0;
        let secondSaved = false;
        store.saveWorker = async (worker) => {
          calls++;
          if (calls === 1) throw new Error('synthetic shutdown write failure');
          await originalSaveWorker(worker);
          secondSaved = true;
        };
        store.close = async () => {
          try {
            await originalClose();
          } finally {
            closed = true;
          }
        };

        manager.registerWorker('first', ['queue'], 1, { workerId: 'first-worker' });
        manager.registerWorker('second', ['queue'], 1, { workerId: 'second-worker' });

        await expect(manager.shutdownPostgres()).rejects.toThrow(
          'synthetic shutdown write failure'
        );
        expect(calls).toBe(2);
        expect(secondSaved).toBe(true);
        expect(closed).toBe(true);
        await expect(manager.shutdownPostgres()).resolves.toBeUndefined();
      } finally {
        await Promise.allSettled([manager.shutdownPostgres()]);
      }
    }
  );
});
