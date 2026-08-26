import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace, deferred, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-queue-lifecycle-${label}-${Date.now()}-${crypto.randomUUID()}`;
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

describe('PostgreSQL queue lifecycle regressions', () => {
  test.skipIf(!postgresUrl)(
    'serializes admission after an in-flight obliterate and preserves its queue metadata',
    async () => {
      const value = namespace('obliterate-admission');
      const queue = 'obliterate-admission';
      const destroyer = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'destroyer',
      });
      const producer = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'producer',
      });
      const blockerSql = new SQL(postgresUrl!, { max: 1 });
      const observerSql = new SQL(postgresUrl!, { max: 2 });
      const blockerReady = deferred<number>();
      const releaseBlocker = deferred<undefined>();
      let obliterate: Promise<number> | null = null;
      let admission: Promise<unknown> | null = null;
      try {
        await Promise.all([destroyer.initialize(), producer.initialize()]);
        await destroyer.setConcurrency(queue, 1);
        await destroyer.insert(createJob(jobId('old-job'), queue, { data: {} }));
        const blocker = blockerSql.begin(async (tx) => {
          const [backend] = await tx<{ pid: number }[]>`SELECT pg_backend_pid() AS pid`;
          await tx`
            SELECT queue FROM bunqueue_queue_state
            WHERE namespace = ${value} AND queue = ${queue}
            FOR UPDATE
          `;
          blockerReady.resolve(backend.pid);
          await releaseBlocker.promise;
        });
        const blockerPid = await blockerReady.promise;
        obliterate = destroyer.obliterate(queue);
        expect(
          await eventually(async () => {
            const [row] = await observerSql<{ blocked: boolean }[]>`
              SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE ${blockerPid} = ANY(pg_blocking_pids(pid))
              ) AS blocked
            `;
            return row.blocked;
          })
        ).toBe(true);

        let admissionSettled = false;
        const lateJob = createJob(jobId('late-job'), queue, { data: { generation: 2 } });
        admission = producer.insert(lateJob).finally(() => {
          admissionSettled = true;
        });
        await Bun.sleep(100);
        expect(admissionSettled).toBe(false);

        releaseBlocker.resolve(undefined);
        await Promise.all([blocker, obliterate, admission]);
        expect((await producer.getJob(lateJob.id))?.job.data).toEqual({ generation: 2 });
        expect(await producer.getQueues()).toContain(queue);
        const pushed = await observerSql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count FROM bunqueue_events
          WHERE namespace = ${value} AND queue = ${queue}
            AND event_type = 'pushed' AND job_id = ${String(lateJob.id)}
        `;
        expect(pushed[0].count).toBe(1);
      } finally {
        releaseBlocker.resolve(undefined);
        await Promise.allSettled([
          obliterate ?? Promise.resolve(),
          admission ?? Promise.resolve(),
          destroyer.close(),
          producer.close(),
          blockerSql.close({ timeout: 5 }),
          observerSql.close({ timeout: 5 }),
        ]);
      }
    },
    20_000
  );

  test.skipIf(!postgresUrl)(
    'retains queue identity after the final pending job is cancelled and after restart',
    async () => {
      const value = namespace('registry');
      const first = manager(value, 'registry-first');
      let second: PostgresQueueManager | null = null;
      try {
        await first.waitUntilReady();
        const job = await first.push('registered-queue', { data: {} });
        expect(await first.cancel(job.id)).toBe(true);
        expect(first.listQueues()).toContain('registered-queue');

        await first.shutdownPostgres();
        second = manager(value, 'registry-second');
        await second.waitUntilReady();
        expect(second.listQueues()).toContain('registered-queue');
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second?.shutdownPostgres()]);
      }
    }
  );
});
