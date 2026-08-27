import { afterAll, describe, expect, test } from 'bun:test';
import { SQL, type TransactionSQL } from 'bun';
import { createJob, generateJobId, jobId, type Job } from '../src/domain/types/job';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { admitPostgresJob } from '../src/infrastructure/persistence/postgres/admission';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import { decodePostgresValue } from '../src/infrastructure/persistence/postgres/codec';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import { lockPostgresDependencyCompletions } from '../src/infrastructure/persistence/postgres/dependencyPromotion';
import { completePostgresJob } from '../src/infrastructure/persistence/postgres/outcomes';
import {
  cleanupPostgresNamespace,
  eventually as eventuallyCondition,
  postgresRaceContext,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

interface ManagerPair {
  readonly namespace: string;
  readonly local: PostgresQueueManager;
  readonly remote: PostgresQueueManager;
}

function managers(label: string): ManagerPair {
  const namespace = `test-dependency-promotion-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(namespace);
  const create = (brokerId: string) =>
    new PostgresQueueManager({
      postgres: { url: postgresUrl!, namespace, brokerId, pollIntervalMs: 25 },
    });
  return { namespace, local: create(`${label}-a`), remote: create(`${label}-b`) };
}

async function eventuallyAssert(assertion: () => void, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let failure: unknown;
  do {
    try {
      assertion();
      return;
    } catch (error) {
      failure = error;
      await Bun.sleep(10);
    }
  } while (Date.now() < deadline);
  throw failure;
}

async function waitForAdvisoryLock(sql: SQL, pid: number): Promise<void> {
  expect(
    await eventuallyCondition(async () => {
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

async function query<T extends Record<PropertyKey, unknown>>(
  text: (sql: SQL) => Promise<T[]>
): Promise<T[]> {
  const sql = new SQL(postgresUrl!, { max: 1 });
  try {
    return await text(sql);
  } finally {
    await sql.close({ timeout: 5 });
  }
}

async function runAdmissionCompletionRace(
  pair: ManagerPair,
  first: 'admission' | 'completion'
): Promise<void> {
  const { namespace, local } = pair;
  const queue = `consumer-race-${first}`;
  const child = await local.push(queue, { data: { child: first }, removeOnComplete: true });
  const claim = await local.pullWithLock(queue, `worker-${first}`);
  expect(claim.job?.id).toBe(child.id);
  const parent = createJob(generateJobId(), queue, {
    data: { parent: first },
    dependsOn: [child.id],
  });
  const blocker = new SQL(postgresUrl!, { max: 1 });
  const admissionSql = new SQL(postgresUrl!, { max: 1 });
  const completionSql = new SQL(postgresUrl!, { max: 1 });
  const inspector = new SQL(postgresUrl!, { max: 1 });
  const lockName = postgresAdvisoryLockName('dependency-completion', namespace, String(child.id));
  const order: string[] = [];
  const operations: Promise<unknown>[] = [];
  let locked = false;
  try {
    const [admissionBackend] = await admissionSql<{ pid: number }[]>`
      SELECT pg_backend_pid() AS pid
    `;
    const [completionBackend] = await completionSql<{ pid: number }[]>`
      SELECT pg_backend_pid() AS pid
    `;
    await blocker`SELECT pg_advisory_lock(hashtextextended(${lockName}, 0))`;
    locked = true;
    const admission = () =>
      admissionSql
        .begin((tx) => admitPostgresJob(tx, postgresRaceContext(local, admissionSql), parent))
        .then(() => order.push('admission'));
    const completion = () =>
      completePostgresJob(
        postgresRaceContext(local, completionSql),
        child.id,
        claim.token!,
        `${first}-result`
      ).then(() => order.push('completion'));
    const firstOperation = first === 'admission' ? admission() : completion();
    operations.push(firstOperation);
    await waitForAdvisoryLock(
      inspector,
      first === 'admission' ? admissionBackend.pid : completionBackend.pid
    );
    const secondOperation = first === 'admission' ? completion() : admission();
    operations.push(secondOperation);
    await waitForAdvisoryLock(
      inspector,
      first === 'admission' ? completionBackend.pid : admissionBackend.pid
    );
    await blocker`SELECT pg_advisory_unlock(hashtextextended(${lockName}, 0))`;
    locked = false;
    await Promise.all([firstOperation, secondOperation]);

    const [stored] = await query(
      (sql) => sql<Array<{ state: string; version: bigint }>>`
        SELECT state, version FROM bunqueue_jobs
        WHERE namespace = ${namespace} AND id = ${String(parent.id)}
      `
    );
    expect(order).toEqual([first, first === 'admission' ? 'completion' : 'admission']);
    expect(stored.state).toBe('waiting');
    expect(Number(stored.version)).toBe(first === 'admission' ? 1 : 0);
    expect(await local.getChildrenValues(parent.id)).toEqual({
      [`${queue}:${child.id}`]: `${first}-result`,
    });
    const parentEvents = await query(
      (sql) => sql<Array<{ event_type: string }>>`
        SELECT event_type FROM bunqueue_events
        WHERE namespace = ${namespace} AND job_id = ${String(parent.id)}
        ORDER BY id
      `
    );
    expect(parentEvents.map(({ event_type }) => event_type)).toEqual(
      first === 'admission' ? ['pushed', 'retried'] : ['pushed']
    );
  } finally {
    if (locked) {
      await blocker`SELECT pg_advisory_unlock(hashtextextended(${lockName}, 0))`.catch(() => []);
    }
    await Promise.allSettled(operations);
    await Promise.allSettled([
      blocker.close({ timeout: 5 }),
      admissionSql.close({ timeout: 5 }),
      completionSql.close({ timeout: 5 }),
      inspector.close({ timeout: 5 }),
    ]);
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const namespace of namespaces) await cleanupPostgresNamespace(postgresUrl, namespace);
});

describe('PostgreSQL dependency promotion', () => {
  test('plans a multi-dependency advisory lock as one ordered query', async () => {
    let queryCount = 0;
    let plannedIds: unknown[] = [];
    const tx = Object.assign(
      async (_strings: TemplateStringsArray, ..._values: unknown[]) => {
        queryCount++;
        return [];
      },
      {
        array(values: readonly unknown[]) {
          plannedIds = [...values];
          return values;
        },
      }
    ) as unknown as TransactionSQL;
    const context = { config: { namespace: 'lock-plan' } } as unknown as PostgresContext;

    await lockPostgresDependencyCompletions(tx, context, []);
    expect(queryCount).toBe(0);
    await lockPostgresDependencyCompletions(tx, context, [jobId('z'), jobId('a'), jobId('z')]);
    expect(queryCount).toBe(1);
    expect(plannedIds).toEqual([
      postgresAdvisoryLockName('dependency-completion', 'lock-plan', 'a'),
      postgresAdvisoryLockName('dependency-completion', 'lock-plan', 'z'),
    ]);
  });

  test.skipIf(!postgresUrl)(
    'commits parent payload, timeline, event and removeOnComplete evidence atomically',
    async () => {
      const { namespace, local, remote } = managers('single');
      try {
        await Promise.all([local.waitUntilReady(), remote.waitUntilReady()]);
        const child = await local.push('single', {
          data: { role: 'child' },
          removeOnComplete: true,
        });
        const parent = await local.push('single', {
          data: { role: 'parent' },
          dependsOn: [child.id],
        });
        const [before] = await query(
          (sql) => sql<Array<{ version: bigint }>>`
          SELECT version FROM bunqueue_jobs
          WHERE namespace = ${namespace} AND id = ${String(parent.id)}
        `
        );
        const claim = await local.pullWithLock('single', 'single-worker');
        expect(claim.job?.id).toBe(child.id);

        await local.ack(child.id, { complete: true }, claim.token!);

        const [stored] = await query(
          (sql) => sql<Array<{ state: string; payload: Uint8Array; version: bigint }>>`
            SELECT state, payload, version FROM bunqueue_jobs
            WHERE namespace = ${namespace} AND id = ${String(parent.id)}
          `
        );
        const durableParent = decodePostgresValue<Job | null>(stored.payload, null, 'parent');
        expect(stored.state).toBe('waiting');
        expect(Number(stored.version)).toBe(Number(before.version) + 1);
        expect(durableParent?.timeline.at(-1)?.state).toBe('waiting');
        expect(await local.getJobState(child.id)).toBe('unknown');
        expect(await local.getChildrenValues(parent.id)).toEqual({
          [`single:${child.id}`]: { complete: true },
        });

        const events = await query(
          (sql) => sql<Array<{ event_type: string; job_id: string; transaction_id: bigint }>>`
            SELECT event_type, job_id, transaction_id FROM bunqueue_events
            WHERE namespace = ${namespace}
              AND ((job_id = ${String(child.id)} AND event_type = 'completed')
                OR (job_id = ${String(parent.id)} AND event_type = 'retried'))
            ORDER BY id
          `
        );
        expect(events.map(({ event_type }) => event_type)).toEqual(['completed', 'retried']);
        expect(new Set(events.map(({ transaction_id }) => String(transaction_id))).size).toBe(1);
        await eventuallyAssert(() => {
          expect(remote.getQueueJobCounts('single')).toMatchObject({
            waiting: 1,
            'waiting-children': 0,
          });
        });
      } finally {
        await Promise.allSettled([local.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'promotes every resolved parent with set-based batch persistence',
    async () => {
      const { namespace, local, remote } = managers('batch');
      try {
        await Promise.all([local.waitUntilReady(), remote.waitUntilReady()]);
        const children = await local.pushBatch('batch', [
          { data: { child: 1 }, removeOnComplete: true },
          { data: { child: 2 }, removeOnComplete: true },
        ]);
        const parents = await Promise.all([
          local.push('batch', { data: { parent: 'waiting' }, dependsOn: [children[0]] }),
          local.push('batch', {
            data: { parent: 'prioritized' },
            dependsOn: [children[1]],
            priority: 10,
          }),
          local.push('batch', {
            data: { parent: 'delayed-fan-in' },
            dependsOn: children,
            delay: 60_000,
          }),
        ]);
        const claims = await local.pullBatchWithLock('batch', 2, 'batch-worker');
        expect(claims.jobs.map((job) => job.id).sort()).toEqual([...children].sort());

        await local.ackBatchWithResults(
          claims.jobs.map((job, index) => ({
            id: job.id,
            token: claims.tokens[index],
            result: { child: String(job.id) },
          }))
        );

        expect(await Promise.all(parents.map((parent) => local.getJobState(parent.id)))).toEqual([
          'waiting',
          'prioritized',
          'delayed',
        ]);
        const stored = await query(
          (sql) => sql<Array<{ id: string; state: string; payload: Uint8Array; version: bigint }>>`
            SELECT id, state, payload, version FROM bunqueue_jobs
            WHERE namespace = ${namespace}
              AND id = ANY(${sql.array(
                parents.map(({ id }) => String(id)),
                'TEXT'
              )})
            ORDER BY id
          `
        );
        expect(stored).toHaveLength(3);
        for (const row of stored) {
          const job = decodePostgresValue<Job | null>(row.payload, null, `parent:${row.id}`);
          expect(Number(row.version)).toBe(1);
          expect(job?.timeline.at(-1)?.state).toBe(row.state);
        }
        const completionEvents = await query(
          (sql) => sql<Array<{ transaction_id: bigint }>>`
            SELECT transaction_id FROM bunqueue_events
            WHERE namespace = ${namespace}
              AND event_type IN ('completed', 'retried')
              AND job_id = ANY(${sql.array(
                [...children, ...parents.map(({ id }) => id)].map(String),
                'TEXT'
              )})
          `
        );
        expect(completionEvents).toHaveLength(5);
        expect(
          new Set(completionEvents.map(({ transaction_id }) => String(transaction_id))).size
        ).toBe(1);
        await eventuallyAssert(() => {
          expect(remote.getQueueJobCounts('batch')).toMatchObject({
            waiting: 1,
            prioritized: 1,
            delayed: 1,
            'waiting-children': 0,
          });
        });
      } finally {
        await Promise.allSettled([local.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'linearizes concurrent consumer admission in both commit orders',
    async () => {
      const pair = managers('consumer-race');
      try {
        await Promise.all([pair.local.waitUntilReady(), pair.remote.waitUntilReady()]);
        await runAdmissionCompletionRace(pair, 'completion');
        await runAdmissionCompletionRace(pair, 'admission');
      } finally {
        await Promise.allSettled([pair.local.shutdownPostgres(), pair.remote.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'serializes concurrent child completions across brokers without duplicate promotion',
    async () => {
      const { namespace, local, remote } = managers('concurrent');
      try {
        await Promise.all([local.waitUntilReady(), remote.waitUntilReady()]);
        const children = await local.pushBatch('concurrent', [
          { data: { child: 1 }, removeOnComplete: true },
          { data: { child: 2 }, removeOnComplete: true },
        ]);
        const parents = await Promise.all(
          ['left', 'right'].map((side) =>
            local.push('concurrent', { data: { side }, dependsOn: children })
          )
        );
        const first = await local.pullWithLock('concurrent', 'worker-a');
        const second = await remote.pullWithLock('concurrent', 'worker-b');

        await Promise.all([
          local.ack(first.job!.id, 'first', first.token!),
          remote.ack(second.job!.id, 'second', second.token!),
        ]);

        expect(await Promise.all(parents.map(({ id }) => local.getJobState(id)))).toEqual([
          'waiting',
          'waiting',
        ]);
        const rows = await query(
          (sql) => sql<Array<{ id: string; version: bigint; promotions: bigint }>>`
            SELECT parent.id, parent.version, COUNT(event.id)::bigint AS promotions
            FROM bunqueue_jobs AS parent
            LEFT JOIN bunqueue_events AS event
              ON event.namespace = parent.namespace AND event.job_id = parent.id
             AND event.event_type = 'retried'
            WHERE parent.namespace = ${namespace}
              AND parent.id = ANY(${sql.array(
                parents.map(({ id }) => String(id)),
                'TEXT'
              )})
            GROUP BY parent.id, parent.version
            ORDER BY parent.id
          `
        );
        expect(
          rows.map(({ version, promotions }) => [Number(version), Number(promotions)])
        ).toEqual([
          [1, 1],
          [1, 1],
        ]);
      } finally {
        await Promise.allSettled([local.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    }
  );
});
