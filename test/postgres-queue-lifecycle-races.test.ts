import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace, deferred, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-queue-lifecycle-race-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function store(value: string, brokerId: string): PostgresQueueStore {
  return new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL queue lifecycle race coverage', () => {
  test.skipIf(!postgresUrl)(
    'serializes set-based batch admission after obliterate',
    async () => {
      const value = namespace('batch');
      const queue = 'batch-lifecycle';
      const destroyer = store(value, 'batch-destroyer');
      const producer = store(value, 'batch-producer');
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const observer = new SQL(postgresUrl!, { max: 2 });
      const locked = deferred<number>();
      const release = deferred<undefined>();
      let destruction: Promise<number> | null = null;
      let admission: Promise<unknown> | null = null;
      try {
        await Promise.all([destroyer.initialize(), producer.initialize()]);
        await destroyer.setConcurrency(queue, 2);
        await destroyer.insert(createJob(jobId('batch-old'), queue, { data: {} }));
        const rowBlock = blocker.begin(async (tx) => {
          const [backend] = await tx<{ pid: number }[]>`SELECT pg_backend_pid() AS pid`;
          await tx`
            SELECT queue FROM bunqueue_queue_state
            WHERE namespace = ${value} AND queue = ${queue}
            FOR UPDATE
          `;
          locked.resolve(backend.pid);
          await release.promise;
        });
        const blockerPid = await locked.promise;
        destruction = destroyer.obliterate(queue);
        expect(
          await eventually(async () => {
            const [row] = await observer<{ blocked: boolean }[]>`
              SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE ${blockerPid} = ANY(pg_blocking_pids(pid))
              ) AS blocked
            `;
            return row.blocked;
          })
        ).toBe(true);

        let settled = false;
        const jobs = [
          createJob(jobId('batch-new-a'), queue, { data: { batch: true } }),
          createJob(jobId('batch-new-b'), queue, { data: { batch: true } }),
        ];
        admission = producer.insertMany(jobs).finally(() => {
          settled = true;
        });
        await Bun.sleep(100);
        expect(settled).toBe(false);

        release.resolve(undefined);
        await Promise.all([rowBlock, destruction, admission]);
        expect((await producer.getJobs(jobs.map(({ id }) => id))).map(({ job }) => job.id)).toEqual(
          jobs.map(({ id }) => id)
        );
      } finally {
        release.resolve(undefined);
        await Promise.allSettled([
          destruction ?? Promise.resolve(),
          admission ?? Promise.resolve(),
          destroyer.close(),
          producer.close(),
          blocker.close({ timeout: 5 }),
          observer.close({ timeout: 5 }),
        ]);
      }
    },
    20_000
  );

  test.skipIf(!postgresUrl)(
    'rolls back a repeat ACK instead of deadlocking with obliterate',
    async () => {
      const value = namespace('repeat');
      const queue = 'repeat-lifecycle';
      const queueStore = store(value, 'repeat-store');
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const observer = new SQL(postgresUrl!, { max: 2 });
      const locked = deferred<number>();
      const release = deferred<undefined>();
      let destruction: Promise<number> | null = null;
      try {
        await queueStore.initialize();
        const job = createJob(jobId('repeat-original'), queue, {
          data: {},
          repeat: { every: 60_000, limit: 1 },
        });
        await queueStore.insert(job);
        const [claim] = await queueStore.claim(queue, 1, 'repeat-worker', 60_000);
        const rowBlock = blocker.begin(async (tx) => {
          const [backend] = await tx<{ pid: number }[]>`SELECT pg_backend_pid() AS pid`;
          await tx`
            SELECT queue FROM bunqueue_queue_state
            WHERE namespace = ${value} AND queue = ${queue}
            FOR UPDATE
          `;
          locked.resolve(backend.pid);
          await release.promise;
        });
        const blockerPid = await locked.promise;
        destruction = queueStore.obliterate(queue);
        expect(
          await eventually(async () => {
            const [row] = await observer<{ blocked: boolean }[]>`
              SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE ${blockerPid} = ANY(pg_blocking_pids(pid))
              ) AS blocked
            `;
            return row.blocked;
          })
        ).toBe(true);

        await expect(queueStore.complete(job.id, claim.token, 'done')).rejects.toThrow(
          'retry completion'
        );
        expect((await queueStore.getJob(job.id))?.state).toBe('active');
        release.resolve(undefined);
        await Promise.all([rowBlock, destruction]);
        const [rows] = await observer<{ jobs: number; links: number }[]>`
          SELECT
            (SELECT COUNT(*)::int FROM bunqueue_jobs WHERE namespace = ${value}) AS jobs,
            (SELECT COUNT(*)::int FROM bunqueue_repeat_links WHERE namespace = ${value}) AS links
        `;
        expect(rows).toEqual({ jobs: 0, links: 0 });
      } finally {
        release.resolve(undefined);
        await Promise.allSettled([
          destruction ?? Promise.resolve(),
          queueStore.close(),
          blocker.close({ timeout: 5 }),
          observer.close({ timeout: 5 }),
        ]);
      }
    },
    20_000
  );

  test.skipIf(!postgresUrl)(
    'does not register a phantom queue for a cross-queue duplicate ID',
    async () => {
      const value = namespace('phantom');
      const queueStore = store(value, 'phantom-store');
      try {
        await queueStore.initialize();
        const id = jobId('shared-id');
        await queueStore.insert(createJob(id, 'real-queue', { data: {} }));
        const duplicate = await queueStore.insert(
          createJob(id, 'phantom-queue', { data: { duplicate: true } })
        );

        expect(duplicate.inserted).toBe(false);
        expect(duplicate.job.queue).toBe('real-queue');
        expect(await queueStore.getQueues()).toEqual(['real-queue']);
      } finally {
        await queueStore.close();
      }
    }
  );
});
