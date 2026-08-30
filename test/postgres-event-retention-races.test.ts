import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import { recordPostgresEvent } from '../src/infrastructure/persistence/postgres/context';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace, deferred, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-event-retention-race-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function store(value: string, brokerId: string): PostgresQueueStore {
  return new PostgresQueueStore({
    url: postgresUrl!,
    namespace: value,
    brokerId,
    poolSize: 4,
    maxQueueEvents: 2,
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL event-retention races', () => {
  test.skipIf(!postgresUrl)(
    'converges retention without blocking on transient lock contention',
    async () => {
      const value = namespace('retry-after-timeout');
      const queue = 'retry-after-timeout';
      const queueStore = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'retention-retry',
        poolSize: 4,
        maxQueueEvents: 2,
        pollIntervalMs: 60_000,
        lockTimeoutMs: 100,
      });
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const lockAcquired = deferred<undefined>();
      const releaseLock = deferred<undefined>();
      let blockingTransaction: Promise<void> | null = null;
      const retainedEventCount = async () => {
        const [row] = await queueStore.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_events
          WHERE namespace = ${value} AND queue = ${queue}
        `;
        return row.count;
      };

      try {
        await queueStore.initialize();
        for (let index = 0; index < 2; index++) {
          await queueStore.insert(
            createJob(jobId(`retention-seed-${index}`), queue, { data: { index } })
          );
        }
        expect(await retainedEventCount()).toBe(2);

        const lockName = postgresAdvisoryLockName('event-retention', value, queue);
        blockingTransaction = blocker.begin(async (tx) => {
          await tx`SELECT pg_advisory_xact_lock(hashtextextended(${lockName}, 0))`;
          lockAcquired.resolve(undefined);
          await releaseLock.promise;
        });
        await lockAcquired.promise;

        await queueStore.insert(
          createJob(jobId('retention-contended'), queue, { data: { contended: true } })
        );
        expect(await retainedEventCount()).toBe(3);
        const eventStream = queueStore.events as unknown as {
          retentionRetryTimer: ReturnType<typeof setTimeout> | null;
        };
        expect(await eventually(() => Boolean(eventStream.retentionRetryTimer))).toBe(true);
        expect(queueStore.health()).toMatchObject({ ok: true, error: null, since: null });

        releaseLock.resolve(undefined);
        await blockingTransaction;
        blockingTransaction = null;

        expect(await eventually(async () => (await retainedEventCount()) === 2)).toBe(true);
        expect(await eventually(() => queueStore.health().ok)).toBe(true);
      } finally {
        releaseLock.resolve(undefined);
        await blockingTransaction?.catch(() => undefined);
        await Promise.allSettled([queueStore.close(), blocker.close({ timeout: 5 })]);
      }
    },
    10_000
  );

  test.skipIf(!postgresUrl)(
    'never waits on inverse inline queue-retention lock orders',
    async () => {
      const value = namespace('inverse-queues');
      const stores = [store(value, 'inverse-a'), store(value, 'inverse-b')];
      let sequence = 0;
      const emit = (queueStore: PostgresQueueStore, queue: string) =>
        queueStore.context.sql.begin((tx) =>
          recordPostgresEvent(tx, queueStore.context, {
            queue,
            type: 'updated',
            jobId: jobId(`cross-${++sequence}`),
            occurredAt: Date.now(),
          })
        );
      try {
        await Promise.all(stores.map((queueStore) => queueStore.initialize()));
        for (const queue of ['q1', 'q2']) {
          await emit(stores[0], queue);
          await emit(stores[0], queue);
        }

        let arrivals = 0;
        const release = Promise.withResolvers<undefined>();
        const rendezvous = async () => {
          arrivals++;
          if (arrivals === 2) release.resolve(undefined);
          await release.promise;
        };
        const run = (queueStore: PostgresQueueStore, first: string, second: string) =>
          queueStore.context.sql.begin(async (tx) => {
            await recordPostgresEvent(tx, queueStore.context, {
              queue: first,
              type: 'updated',
              jobId: jobId(`cross-${++sequence}`),
              occurredAt: Date.now(),
            });
            await rendezvous();
            await recordPostgresEvent(tx, queueStore.context, {
              queue: second,
              type: 'updated',
              jobId: jobId(`cross-${++sequence}`),
              occurredAt: Date.now(),
            });
          });

        await expect(
          Promise.all([run(stores[0], 'q1', 'q2'), run(stores[1], 'q2', 'q1')])
        ).resolves.toEqual([undefined, undefined]);
        expect(
          await eventually(async () => {
            const rows = await stores[0].context.sql<{ queue: string; count: number }[]>`
              SELECT queue, COUNT(*)::int AS count
              FROM bunqueue_events
              WHERE namespace = ${value} AND queue IN ('q1', 'q2')
              GROUP BY queue
              ORDER BY queue
            `;
            return rows.length === 2 && rows.every(({ count }) => count === 2);
          })
        ).toBe(true);
      } finally {
        await Promise.allSettled(stores.map((queueStore) => queueStore.close()));
      }
    },
    10_000
  );

  test.skipIf(!postgresUrl)(
    'serializes manual trimming with queue obliteration',
    async () => {
      const value = namespace('trim-obliterate');
      const trimmer = store(value, 'trim-broker');
      const destroyer = store(value, 'obliterate-broker');
      try {
        await Promise.all([trimmer.initialize(), destroyer.initialize()]);
        for (let iteration = 0; iteration < 20; iteration++) {
          const queue = `trim-obliterate-${iteration}`;
          await trimmer.insertMany(
            Array.from({ length: 8 }, (_, index) =>
              createJob(jobId(`trim-${iteration}-${index}`), queue, { data: {} })
            )
          );
          await expect(
            Promise.all([trimmer.trimQueueEvents(queue, 0), destroyer.obliterate(queue)])
          ).resolves.toHaveLength(2);
          expect(await destroyer.getCounts(queue)).toMatchObject({
            waiting: 0,
            active: 0,
            completed: 0,
          });
        }
      } finally {
        await Promise.allSettled([trimmer.close(), destroyer.close()]);
      }
    },
    20_000
  );
});
