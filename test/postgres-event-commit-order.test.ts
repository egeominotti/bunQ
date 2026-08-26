import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, jobId, type JobId } from '../src/domain/types/job';
import {
  loadPostgresEventPruneWatermarks,
  recordPostgresEventPruneWatermarks,
} from '../src/infrastructure/persistence/postgres/eventPruneWatermarks';
import { prunePostgresEventCommits } from '../src/infrastructure/persistence/postgres/eventJournal';
import {
  cleanupPostgresNamespace,
  deferred,
  eventually,
  insertPostgresPushedEvent,
  insertPostgresWaitingJob,
  notifyPostgresEvent,
  pausePostgresEventStream,
  postgresEventStream,
  postgresManagerStore,
  postgresRaceContext,
  postgresWaitingIds,
  trimAndNotifyPostgresEvent,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-event-order-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function exactWaiting(
  manager: PostgresQueueManager,
  queue: string,
  expectedIds: readonly JobId[]
): boolean {
  return postgresWaitingIds(manager, queue).join() === expectedIds.map(String).sort().join();
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL event commit ordering', () => {
  test.skipIf(!postgresUrl)(
    'invalidates a queue when an older source event commits after a newer one',
    async () => {
      const value = namespace('pruned');
      const queue = 'commit-order-pruned';
      const remote = new PostgresQueueManager({
        maxQueueEvents: 1,
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'remote', pollIntervalMs: 25 },
      });
      const sqlA = new SQL(postgresUrl!, { max: 1 });
      const sqlB = new SQL(postgresUrl!, { max: 1 });
      const releaseA = deferred<undefined>();
      const allocatedA = deferred<number>();
      let transactionA: Promise<number> | null = null;

      try {
        await remote.waitUntilReady();
        const seed = await remote.push(queue, { data: { marker: 'seed' } });
        const contextA = postgresRaceContext(remote, sqlA);
        const contextB = postgresRaceContext(remote, sqlB);
        const now = Date.now();
        const jobA = createJob(
          jobId(`A-${crypto.randomUUID()}`),
          queue,
          { data: { marker: 'A' } },
          now + 1
        );
        const jobB = createJob(
          jobId(`B-${crypto.randomUUID()}`),
          queue,
          { data: { marker: 'B' } },
          now + 2
        );

        transactionA = sqlA
          .begin(async (tx) => {
            await insertPostgresWaitingJob(tx, value, jobA);
            const eventId = await insertPostgresPushedEvent(tx, value, jobA);
            allocatedA.resolve(eventId);
            await releaseA.promise;
            return await trimAndNotifyPostgresEvent(tx, contextA, queue, eventId);
          })
          .catch((error) => {
            allocatedA.reject(error);
            throw error;
          });
        const eventA = await allocatedA.promise;
        const eventB = await sqlB.begin(async (tx) => {
          await insertPostgresWaitingJob(tx, value, jobB);
          const eventId = await insertPostgresPushedEvent(tx, value, jobB);
          const prunedThrough = await trimAndNotifyPostgresEvent(tx, contextB, queue, eventId);
          return { eventId, prunedThrough };
        });
        expect(eventB.eventId).toBeGreaterThan(eventA);
        expect(
          await eventually(() => postgresWaitingIds(remote, queue).includes(String(jobB.id)))
        ).toBe(true);

        releaseA.resolve(undefined);
        const prunedByA = await transactionA;
        transactionA = null;
        expect(prunedByA).toBe(eventA);
        await postgresEventStream(remote).drain();

        expect(
          await eventually(() => exactWaiting(remote, queue, [seed.id, jobA.id, jobB.id]))
        ).toBe(true);
      } finally {
        releaseA.resolve(undefined);
        await transactionA?.catch(() => undefined);
        await Promise.allSettled([
          remote.shutdownPostgres(),
          sqlA.close({ timeout: 5 }),
          sqlB.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'catches an unpruned older event after its notification is missed',
    async () => {
      const value = namespace('unpruned');
      const queue = 'commit-order-unpruned';
      const remote = new PostgresQueueManager({
        maxQueueEvents: 100,
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'remote', pollIntervalMs: 25 },
      });
      const sqlA = new SQL(postgresUrl!, { max: 1 });
      const sqlB = new SQL(postgresUrl!, { max: 1 });
      const releaseA = deferred<undefined>();
      const allocatedA = deferred<number>();
      let transactionA: Promise<void> | null = null;

      try {
        await remote.waitUntilReady();
        const seed = await remote.push(queue, { data: { marker: 'seed' } });
        const contextA = postgresRaceContext(remote, sqlA);
        const contextB = postgresRaceContext(remote, sqlB);
        const now = Date.now();
        const jobA = createJob(
          jobId(`A-${crypto.randomUUID()}`),
          queue,
          { data: { marker: 'A' } },
          now + 1
        );
        const jobB = createJob(
          jobId(`B-${crypto.randomUUID()}`),
          queue,
          { data: { marker: 'B' } },
          now + 2
        );

        transactionA = sqlA
          .begin(async (tx) => {
            await insertPostgresWaitingJob(tx, value, jobA);
            const eventId = await insertPostgresPushedEvent(tx, value, jobA);
            allocatedA.resolve(eventId);
            await releaseA.promise;
            await notifyPostgresEvent(tx, contextA, queue, eventId);
          })
          .catch((error) => {
            allocatedA.reject(error);
            throw error;
          });
        const eventA = await allocatedA.promise;
        const eventB = await sqlB.begin(async (tx) => {
          await insertPostgresWaitingJob(tx, value, jobB);
          const eventId = await insertPostgresPushedEvent(tx, value, jobB);
          await notifyPostgresEvent(tx, contextB, queue, eventId);
          return eventId;
        });
        expect(eventB).toBeGreaterThan(eventA);
        expect(
          await eventually(() => postgresWaitingIds(remote, queue).includes(String(jobB.id)))
        ).toBe(true);

        const stream = await pausePostgresEventStream(remote);
        releaseA.resolve(undefined);
        await transactionA;
        transactionA = null;
        await stream.drain();

        expect(
          await eventually(() => exactWaiting(remote, queue, [seed.id, jobA.id, jobB.id]))
        ).toBe(true);
      } finally {
        releaseA.resolve(undefined);
        await transactionA?.catch(() => undefined);
        await Promise.allSettled([
          remote.shutdownPostgres(),
          sqlA.close({ timeout: 5 }),
          sqlB.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps the pruned commit frontier monotonic across newer physical IDs',
    async () => {
      const value = namespace('prune-frontier');
      const manager = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'prune-frontier' },
      });
      try {
        await manager.waitUntilReady();
        const context = postgresManagerStore(manager).context;
        const [sequence] = await context.sql<{ last_value: number | string | bigint }[]>`
          SELECT last_value FROM bunqueue_event_commit_seq
        `;
        const commitSequenceFloor = Number(sequence.last_value);
        await context.sql.begin((tx) =>
          recordPostgresEventPruneWatermarks(tx, context, [
            { queue: 'frontier', sourceEventId: 100, prunedThrough: 50, prunedCommitSeq: 9 },
          ])
        );
        await context.sql.begin((tx) =>
          recordPostgresEventPruneWatermarks(tx, context, [
            { queue: 'frontier', sourceEventId: 101, prunedThrough: 51, prunedCommitSeq: 3 },
          ])
        );

        const [watermark] = await loadPostgresEventPruneWatermarks(context);
        expect(watermark.prunedCommitSeq).toBe(9);
        expect(watermark.commitSeq).toBeGreaterThan(commitSequenceFloor);
      } finally {
        await manager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'collects only commit envelopes with no journal references',
    async () => {
      const value = namespace('commit-gc');
      const manager = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'commit-gc' },
      });
      try {
        await manager.waitUntilReady();
        const pushed = await manager.push('commit-gc', { data: { marker: 'retained' } });
        const context = postgresManagerStore(manager).context;
        const [event] = await context.sql<{ transaction_id: number | string | bigint }[]>`
        SELECT transaction_id FROM bunqueue_events
        WHERE namespace = ${value} AND job_id = ${String(pushed.id)}
      `;

        expect(await prunePostgresEventCommits(context)).toBe(0);
        await context.sql`
        DELETE FROM bunqueue_events
        WHERE namespace = ${value} AND transaction_id = ${event.transaction_id}
      `;
        expect(await prunePostgresEventCommits(context)).toBe(1);
        const [remaining] = await context.sql<{ count: number | string | bigint }[]>`
        SELECT COUNT(*) AS count FROM bunqueue_event_commits
        WHERE namespace = ${value} AND transaction_id = ${event.transaction_id}
      `;
        expect(Number(remaining.count)).toBe(0);
      } finally {
        await manager.shutdownPostgres();
      }
    }
  );
});
