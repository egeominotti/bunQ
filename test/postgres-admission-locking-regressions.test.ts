import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { admitPostgresJob } from '../src/infrastructure/persistence/postgres/admission';
import {
  admitPostgresJobsBatch,
  PostgresBatchAdmissionConflict,
} from '../src/infrastructure/persistence/postgres/batchAdmission';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres/store';
import { cleanupPostgresNamespace, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

async function waitForAdvisoryLock(sql: SQL, pid: number): Promise<void> {
  expect(
    await eventually(async () => {
      const [row] = await sql<{ waiting: boolean }[]>`
        SELECT EXISTS (
          SELECT 1 FROM pg_stat_activity
          WHERE pid = ${pid} AND wait_event_type = 'Lock' AND wait_event = 'advisory'
        ) AS waiting
      `;
      return row.waiting;
    })
  ).toBe(true);
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const namespace of namespaces) await cleanupPostgresNamespace(postgresUrl, namespace);
});

describe('PostgreSQL admission locking regressions', () => {
  test.skipIf(!postgresUrl)(
    'degrades a late-ID dedup replacement to a duplicate while dependency admission owns the ID',
    async () => {
      const namespace = `test-admission-replace-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(namespace);
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace,
        brokerId: 'admission-replace',
      });
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const originalId = jobId('dedup-owner');
      const lockName = `${namespace}:dependency-completion:${String(originalId)}`;
      let blockerHeld = false;
      try {
        await store.initialize();
        await store.insert(
          createJob(originalId, 'dedup-replace', { data: { generation: 1 }, uniqueKey: 'shared' })
        );
        await blocker`SELECT pg_advisory_lock(hashtext(${lockName}))`;
        blockerHeld = true;

        const replacement = await store.insert(
          createJob(jobId('dedup-replacement'), 'dedup-replace', {
            data: { generation: 2 },
            uniqueKey: 'shared',
            dedup: { replace: true },
          })
        );

        expect(replacement).toMatchObject({ inserted: false, job: { id: originalId } });
        expect(await store.getJob(originalId)).not.toBeNull();
        expect(await store.getJob(jobId('dedup-replacement'))).toBeNull();
      } finally {
        if (blockerHeld) {
          await blocker`SELECT pg_advisory_unlock(hashtext(${lockName}))`.catch(() => []);
        }
        await Promise.allSettled([store.close(), blocker.close({ timeout: 5 })]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'uses one lock order for a conflicting batch and single admission',
    async () => {
      const namespace = `test-admission-lock-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(namespace);
      const queue = 'admission-lock';
      const uniqueKey = 'shared-key';
      const initializer = new PostgresQueueStore({
        url: postgresUrl!,
        namespace,
        brokerId: 'admission-lock-initializer',
      });
      const batchSql = new SQL(postgresUrl!, { max: 1 });
      const singleSql = new SQL(postgresUrl!, { max: 1 });
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const inspector = new SQL(postgresUrl!, { max: 1 });
      const operations: Promise<unknown>[] = [];
      const lockName = `${namespace}:dedup:${queue}:${uniqueKey}`;
      let blockerHeld = false;

      try {
        await initializer.initialize();
        const config = { ...initializer.context.config };
        const batchContext = { sql: batchSql, config } satisfies PostgresContext;
        const singleContext = { sql: singleSql, config } satisfies PostgresContext;
        const [batchBackend] = await batchSql<{ pid: number }[]>`
          SELECT pg_backend_pid() AS pid
        `;
        const [singleBackend] = await singleSql<{ pid: number }[]>`
          SELECT pg_backend_pid() AS pid
        `;
        await blocker`SELECT pg_advisory_lock(hashtext(${lockName}))`;
        blockerHeld = true;

        const batch = batchSql.begin((tx) =>
          admitPostgresJobsBatch(tx, batchContext, [
            createJob(jobId('shared-id'), queue, { data: { source: 'batch' }, uniqueKey }),
            createJob(jobId('batch-only-id'), queue, { data: { source: 'batch-only' } }),
          ])
        );
        operations.push(batch);
        await waitForAdvisoryLock(inspector, batchBackend.pid);

        const single = singleSql.begin((tx) =>
          admitPostgresJob(
            tx,
            singleContext,
            createJob(jobId('shared-id'), queue, { data: { source: 'single' }, uniqueKey })
          )
        );
        operations.push(single);
        await waitForAdvisoryLock(inspector, singleBackend.pid);

        await blocker`SELECT pg_advisory_unlock(hashtext(${lockName}))`;
        blockerHeld = false;
        const outcomes = await Promise.allSettled([batch, single]);
        const failures = outcomes.filter(
          (outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected'
        );
        const deadlocks = failures
          .map(({ reason }) => String(reason))
          .filter((reason) => /40P01|deadlock detected/i.test(reason));
        expect(deadlocks).toEqual([]);
        expect(
          failures.filter(({ reason }) => !(reason instanceof PostgresBatchAdmissionConflict))
        ).toEqual([]);
        expect(outcomes.some(({ status }) => status === 'fulfilled')).toBe(true);
        expect(await initializer.getJob(jobId('shared-id'))).not.toBeNull();
      } finally {
        if (blockerHeld) {
          await blocker`SELECT pg_advisory_unlock(hashtext(${lockName}))`.catch(() => []);
        }
        await Promise.allSettled(operations);
        await Promise.allSettled([
          initializer.close(),
          batchSql.close({ timeout: 5 }),
          singleSql.close({ timeout: 5 }),
          blocker.close({ timeout: 5 }),
          inspector.close({ timeout: 5 }),
        ]);
      }
    },
    15_000
  );
});
