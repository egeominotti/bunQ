import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import fc from 'fast-check';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, jobId, type JobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  createPostgresFastCheckScope,
  postgresFastCheckParameters,
  postgresTestUrl,
} from './support/postgres-fast-check';
import {
  deferred,
  insertPostgresPushedEvent,
  insertPostgresWaitingJob,
  notifyPostgresEvent,
  postgresRaceContext,
} from './support/postgres-event-race';

const scope = createPostgresFastCheckScope('events');

interface PausableEventStream {
  subscription: { unlisten(): Promise<void> } | null;
  pollTimer: ReturnType<typeof setInterval> | null;
  drain(): Promise<void>;
}

function eventStream(value: PostgresQueueManager): PausableEventStream {
  const store = (value as unknown as { postgresStore: PostgresQueueStore }).postgresStore;
  return store.events as unknown as PausableEventStream;
}

async function pauseNotifications(value: PostgresQueueManager): Promise<PausableEventStream> {
  const stream = eventStream(value);
  if (stream.pollTimer) clearInterval(stream.pollTimer);
  stream.pollTimer = null;
  await stream.subscription?.unlisten();
  stream.subscription = null;
  return stream;
}

async function eventually(condition: () => boolean | Promise<boolean>, timeoutMs = 3_000) {
  const deadline = Date.now() + timeoutMs;
  do {
    if (await condition()) return true;
    await Bun.sleep(20);
  } while (Date.now() < deadline);
  return false;
}

function exactState(
  manager: PostgresQueueManager,
  queue: string,
  state: string,
  ids: readonly JobId[]
): boolean {
  const actual = manager
    .getJobs(queue, { state })
    .map((job) => String(job.id))
    .sort();
  return actual.join() === ids.map(String).sort().join();
}

async function waitForState(
  manager: PostgresQueueManager,
  queue: string,
  state: string,
  ids: readonly JobId[]
): Promise<void> {
  expect(await eventually(() => exactState(manager, queue, state, ids))).toBe(true);
}

function managers(namespace: string, retention: number) {
  const active = new PostgresQueueManager({
    maxQueueEvents: retention,
    postgres: {
      url: postgresTestUrl!,
      namespace,
      brokerId: 'events-active',
      pollIntervalMs: 25,
    },
  });
  const remote = new PostgresQueueManager({
    maxQueueEvents: retention,
    postgres: {
      url: postgresTestUrl!,
      namespace,
      brokerId: 'events-remote',
      pollIntervalMs: 25,
    },
  });
  return { active, remote };
}

afterAll(() => scope.cleanup(), { timeout: 30_000 });

describe('PostgreSQL fast-check event convergence', () => {
  test.skipIf(!postgresTestUrl)(
    'converges admission, claim, and completion across generated event windows',
    async () => {
      let run = 0;
      const scenario = fc.integer({ min: 1, max: 8 }).chain((retention) =>
        fc.record({
          batchSize: fc.integer({ min: 1, max: retention * 3 }),
          missed: fc.boolean(),
          retention: fc.constant(retention),
        })
      );
      await fc.assert(
        fc.asyncProperty(scenario, async ({ batchSize, missed, retention }) => {
          const namespace = scope.namespace(`batch-${run}`);
          const queue = `batch-${run++}`;
          const { active, remote } = managers(namespace, retention);
          try {
            await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
            const paused = missed ? await pauseNotifications(remote) : null;
            const ids = await active.pushBatch(
              queue,
              Array.from({ length: batchSize }, (_, index) => ({ data: { index } }))
            );
            await paused?.drain();
            await waitForState(remote, queue, 'waiting', ids);

            const claims = await active.pullBatchWithLock(
              queue,
              batchSize,
              'event-worker',
              0,
              60_000
            );
            await paused?.drain();
            await waitForState(remote, queue, 'active', ids);

            await active.ackBatchWithResults(
              claims.jobs.map((job, index) => ({
                id: job.id,
                token: claims.tokens[index],
                result: { index },
              }))
            );
            await paused?.drain();
            await waitForState(remote, queue, 'completed', ids);
            expect(ids.map((id) => remote.getResult(id))).toEqual(
              ids.map((_, index) => ({ index }))
            );
          } finally {
            await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
          }
        }),
        postgresFastCheckParameters(25)
      );
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'converges after more sequential commits than the retained event window',
    async () => {
      let run = 0;
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 8 }), async (retention) => {
          const namespace = scope.namespace(`sequential-${run}`);
          const queue = `sequential-${run++}`;
          const { active, remote } = managers(namespace, retention);
          try {
            await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
            const paused = await pauseNotifications(remote);
            const ids: JobId[] = [];
            for (let index = 0; index < retention + 3; index++) {
              ids.push((await active.push(queue, { data: { index } })).id);
            }
            await paused.drain();
            await waitForState(remote, queue, 'waiting', ids);
          } finally {
            await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
          }
        }),
        postgresFastCheckParameters(12)
      );
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'replays generated transaction orders independently of physical event IDs',
    async () => {
      let run = 0;
      const commitKeys = fc.uniqueArray(fc.integer({ min: -10_000, max: 10_000 }), {
        minLength: 2,
        maxLength: 6,
      });
      await fc.assert(
        fc.asyncProperty(commitKeys, async (keys) => {
          const namespace = scope.namespace(`commit-order-${run}`);
          const queue = `commit-order-${run++}`;
          const remote = new PostgresQueueManager({
            maxQueueEvents: 100,
            postgres: {
              url: postgresTestUrl!,
              namespace,
              brokerId: 'events-commit-order',
              pollIntervalMs: 25,
            },
          });
          const connections = keys.map(() => new SQL(postgresTestUrl!, { max: 1 }));
          const releases = keys.map(() => deferred<undefined>());
          const allocations = keys.map(() => deferred<number>());
          const transactions: Promise<void>[] = [];
          const ids: JobId[] = [];

          try {
            await remote.waitUntilReady();
            const stream = await pauseNotifications(remote);
            await stream.drain();
            for (let index = 0; index < keys.length; index++) {
              const job = createJob(
                jobId(`commit-order-${index}-${crypto.randomUUID()}`),
                queue,
                { data: { index } },
                Date.now() + index
              );
              ids.push(job.id);
              const context = postgresRaceContext(remote, connections[index]);
              const transaction = connections[index]
                .begin(async (tx) => {
                  await insertPostgresWaitingJob(tx, namespace, job);
                  const eventId = await insertPostgresPushedEvent(tx, namespace, job);
                  allocations[index].resolve(eventId);
                  await notifyPostgresEvent(tx, context, queue, eventId);
                  await releases[index].promise;
                })
                .catch((error) => {
                  allocations[index].reject(error);
                  throw error;
                });
              transactions.push(transaction);
              await allocations[index].promise;
            }

            const order = keys
              .map((key, index) => ({ key, index }))
              .sort((left, right) => left.key - right.key)
              .map(({ index }) => index);
            if (order.every((index, position) => index === position)) order.reverse();
            for (const index of order) {
              releases[index].resolve(undefined);
              await transactions[index];
              await stream.drain();
            }
            await waitForState(remote, queue, 'waiting', ids);
          } finally {
            for (const release of releases) release.resolve(undefined);
            await Promise.allSettled(transactions);
            await Promise.allSettled([
              remote.shutdownPostgres(),
              ...connections.map((sql) => sql.close({ timeout: 5 })),
            ]);
          }
        }),
        postgresFastCheckParameters(12)
      );
    },
    130_000
  );
});
