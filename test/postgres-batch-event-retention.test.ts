import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

interface PausableEventStream {
  subscription: { unlisten(): Promise<void> } | null;
  pollTimer: ReturnType<typeof setInterval> | null;
  drain(): Promise<void>;
}

type NotificationMode = 'live' | 'missed';

function namespace(label: string): string {
  const value = `test-batch-retention-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    maxQueueEvents: 1,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
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

async function eventually(condition: () => boolean | Promise<boolean>, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  do {
    if (await condition()) return true;
    await Bun.sleep(20);
  } while (Date.now() < deadline);
  return false;
}

function stateIds(manager: PostgresQueueManager, queue: string, state: string): string {
  return manager
    .getJobs(queue, { state })
    .map((job) => String(job.id))
    .sort()
    .join();
}

function exactState(
  manager: PostgresQueueManager,
  queue: string,
  state: string,
  ids: readonly JobId[]
): boolean {
  return stateIds(manager, queue, state) === ids.map(String).sort().join();
}

async function waitForState(
  manager: PostgresQueueManager,
  queue: string,
  state: string,
  ids: readonly JobId[]
): Promise<void> {
  expect(await eventually(() => exactState(manager, queue, state, ids))).toBe(true);
}

async function seedWaiting(
  active: PostgresQueueManager,
  remote: PostgresQueueManager,
  queue: string
): Promise<JobId[]> {
  const ids: JobId[] = [];
  for (let index = 0; index < 3; index++) {
    ids.push((await active.push(queue, { data: { index } })).id);
    await waitForState(remote, queue, 'waiting', ids);
  }
  return ids;
}

async function claimIndividually(
  active: PostgresQueueManager,
  remote: PostgresQueueManager,
  queue: string,
  ids: readonly JobId[]
): Promise<string[]> {
  const tokens: string[] = [];
  const activeIds: JobId[] = [];
  for (let index = 0; index < ids.length; index++) {
    const claim = await active.pullWithLock(queue, `worker-${index}`);
    expect(claim.job?.id).toBe(ids[index]);
    expect(claim.token).not.toBeNull();
    tokens.push(claim.token!);
    activeIds.push(ids[index]);
    await waitForState(remote, queue, 'active', activeIds);
  }
  return tokens;
}

async function cleanup(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    await sql.begin(async (tx) => {
      await tx`DELETE FROM bunqueue_metric_buckets WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_metric_totals WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_workers WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_crons WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_job_logs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_repeat_links WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_flow_failures WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_dependencies WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_completions WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_jobs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_queue_state WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_prune_watermarks WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_events WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_commits WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_brokers WHERE namespace = ${value}`;
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

async function prepareCatchUp(
  mode: NotificationMode,
  remote: PostgresQueueManager
): Promise<PausableEventStream | null> {
  return mode === 'missed' ? await pauseNotifications(remote) : null;
}

async function catchUp(stream: PausableEventStream | null): Promise<void> {
  await stream?.drain();
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(postgresUrl, value);
});

describe('PostgreSQL batch event retention', () => {
  for (const mode of ['live', 'missed'] as const) {
    test.skipIf(!postgresUrl)(
      `${mode} admission refreshes the complete remote snapshot`,
      async () => {
        const value = namespace(`${mode}-admission`);
        const active = manager(value, `${mode}-admission-active`);
        const remote = manager(value, `${mode}-admission-remote`);
        try {
          await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
          const queue = `${mode}-admission`;
          const stream = await prepareCatchUp(mode, remote);
          const ids = await active.pushBatch(
            queue,
            Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
          );
          await catchUp(stream);

          await waitForState(remote, queue, 'waiting', ids);
          expect(remote.getQueueJobCounts(queue).waiting).toBe(3);
        } finally {
          await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
        }
      }
    );

    test.skipIf(!postgresUrl)(`${mode} claim refreshes the complete remote snapshot`, async () => {
      const value = namespace(`${mode}-claim`);
      const active = manager(value, `${mode}-claim-active`);
      const remote = manager(value, `${mode}-claim-remote`);
      try {
        await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
        const queue = `${mode}-claim`;
        const ids = await seedWaiting(active, remote, queue);
        const stream = await prepareCatchUp(mode, remote);
        const claims = await active.pullBatchWithLock(queue, 3, 'batch-worker', 0, 60_000);
        expect(claims.jobs.map((job) => job.id)).toEqual(ids);
        await catchUp(stream);

        await waitForState(remote, queue, 'active', ids);
        expect(remote.getQueueJobCounts(queue)).toMatchObject({ waiting: 0, active: 3 });
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    });

    test.skipIf(!postgresUrl)(
      `${mode} completion refreshes the complete remote snapshot`,
      async () => {
        const value = namespace(`${mode}-completion`);
        const active = manager(value, `${mode}-completion-active`);
        const remote = manager(value, `${mode}-completion-remote`);
        try {
          await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
          const queue = `${mode}-completion`;
          const ids = await seedWaiting(active, remote, queue);
          const tokens = await claimIndividually(active, remote, queue, ids);
          const stream = await prepareCatchUp(mode, remote);
          await active.ackBatchWithResults(
            ids.map((id, index) => ({ id, token: tokens[index], result: { index } }))
          );
          await catchUp(stream);

          await waitForState(remote, queue, 'completed', ids);
          expect(remote.getQueueJobCounts(queue)).toMatchObject({ active: 0, completed: 3 });
          expect(ids.map((id) => remote.getResult(id))).toEqual([
            { index: 0 },
            { index: 1 },
            { index: 2 },
          ]);
        } finally {
          await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
        }
      }
    );
  }

  test.skipIf(!postgresUrl)('obliterate removes the internal batch checkpoint', async () => {
    const value = namespace('obliterate-checkpoint');
    const active = manager(value, 'obliterate-checkpoint-active');
    try {
      await active.waitUntilReady();
      const queue = 'obliterate-checkpoint';
      await active.pushBatch(
        queue,
        Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
      );
      await active.obliterate(queue);

      const sql = new SQL(postgresUrl!, { max: 2 });
      try {
        const [row] = await sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_events
          WHERE namespace = ${value}
            AND event_type = 'queue-batch-overflow'
            AND job_id = ${`__queue__:${queue}`}
        `;
        expect(row.count).toBe(0);
      } finally {
        await sql.close({ timeout: 5 });
      }
    } finally {
      await active.shutdownPostgres();
    }
  });
});
