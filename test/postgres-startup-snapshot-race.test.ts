import { afterAll, describe, expect, test } from 'bun:test';
import { jobId } from '../src/domain/types/job';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { PostgresStartupEventBuffer } from '../src/application/postgres-queue-manager/startupEventBuffer';
import type { PostgresStoreEvent } from '../src/infrastructure/persistence/postgres';
import {
  cleanupPostgresNamespace,
  deferred,
  eventually,
  postgresManagerSnapshotHas,
  postgresManagerStore,
  postgresWaitingIds,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function storeEvent(id: number): PostgresStoreEvent {
  return {
    id,
    queue: 'startup-buffer',
    type: 'removed',
    jobId: jobId(String(id)),
    occurredAt: id,
    job: null,
    state: null,
  };
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const namespace of namespaces) await cleanupPostgresNamespace(postgresUrl, namespace);
});

describe('PostgreSQL startup snapshot consistency', () => {
  test('caps event capture and explicitly resets after an overflow', () => {
    const buffer = new PostgresStartupEventBuffer(2);
    buffer.capture(storeEvent(1));
    buffer.capture(storeEvent(2));
    expect(buffer.overflowed).toBe(false);
    expect(buffer.take()?.map(({ id }) => id)).toEqual([1, 2]);

    buffer.capture(storeEvent(3));
    buffer.capture(storeEvent(4));
    buffer.capture(storeEvent(5));
    expect(buffer.overflowed).toBe(true);
    expect(buffer.take()).toBeNull();

    buffer.reset();
    buffer.capture(storeEvent(6));
    expect(buffer.overflowed).toBe(false);
    expect(buffer.take()?.map(({ id }) => id)).toEqual([6]);
  });

  test.skipIf(!postgresUrl)(
    'replays an event observed while the initial snapshot is loading',
    async () => {
      const namespace = `test-startup-snapshot-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(namespace);
      const queue = 'startup-snapshot';
      const active = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace, brokerId: 'active', pollIntervalMs: 25 },
      });
      let remote: PostgresQueueManager | null = null;
      const rowsCaptured = deferred<number>();
      const releaseSnapshot = deferred<undefined>();

      try {
        await active.waitUntilReady();
        remote = new PostgresQueueManager({
          postgres: { url: postgresUrl!, namespace, brokerId: 'remote', pollIntervalMs: 25 },
        });
        const store = postgresManagerStore(remote);
        const originalLoadSnapshot = store.loadManagerSnapshot;
        store.loadManagerSnapshot = async () => {
          const snapshot = await originalLoadSnapshot();
          rowsCaptured.resolve(snapshot.rows.length);
          await releaseSnapshot.promise;
          return snapshot;
        };

        expect(await rowsCaptured.promise).toBe(0);
        const job = await active.push(queue, { data: { marker: 'during-hydration' } });
        expect(await eventually(() => postgresManagerSnapshotHas(remote!, job.id))).toBe(true);

        releaseSnapshot.resolve(undefined);
        await remote.waitUntilReady();
        expect(await postgresManagerStore(active).getJob(job.id)).not.toBeNull();
        expect(
          await eventually(() => postgresWaitingIds(remote!, queue).includes(String(job.id)))
        ).toBe(true);
      } finally {
        releaseSnapshot.resolve(undefined);
        await Promise.allSettled([
          active.shutdownPostgres(),
          ...(remote ? [remote.shutdownPostgres()] : []),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'bounds events observed during a slow snapshot and retries from durable state on overflow',
    async () => {
      const namespace = `test-startup-overflow-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(namespace);
      const queue = 'startup-overflow';
      const active = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace, brokerId: 'overflow-active', pollIntervalMs: 25 },
      });
      let remote: PostgresQueueManager | null = null;
      const rowsCaptured = deferred<number>();
      const releaseSnapshot = deferred<undefined>();
      let loadCalls = 0;

      try {
        await active.waitUntilReady();
        remote = new PostgresQueueManager({
          postgres: {
            url: postgresUrl!,
            namespace,
            brokerId: 'overflow-remote',
            pollIntervalMs: 25,
          },
        });
        const store = postgresManagerStore(remote);
        const originalLoadSnapshot = store.loadManagerSnapshot;
        store.loadManagerSnapshot = async () => {
          loadCalls++;
          const snapshot = await originalLoadSnapshot();
          if (loadCalls === 1) {
            rowsCaptured.resolve(snapshot.rows.length);
            await releaseSnapshot.promise;
          }
          return snapshot;
        };

        expect(await rowsCaptured.promise).toBe(0);
        const ids = await active.pushBatch(
          queue,
          Array.from({ length: 300 }, (_, index) => ({ data: { index } }))
        );
        expect(
          await eventually(() => {
            const state = remote as unknown as {
              snapshotEventBuffer: { overflowed: boolean } | null;
            };
            return state.snapshotEventBuffer?.overflowed === true;
          })
        ).toBe(true);

        releaseSnapshot.resolve(undefined);
        await remote.waitUntilReady();
        expect(loadCalls).toBeGreaterThanOrEqual(2);
        expect(
          remote
            .getJobs(queue, { state: 'waiting', end: ids.length })
            .map((job) => String(job.id))
            .sort()
        ).toEqual(ids.map(String).sort());
      } finally {
        releaseSnapshot.resolve(undefined);
        await Promise.allSettled([
          active.shutdownPostgres(),
          ...(remote ? [remote.shutdownPostgres()] : []),
        ]);
      }
    }
  );
});
