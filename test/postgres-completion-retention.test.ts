import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { jobId } from '../src/domain/types/job';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-completion-retention-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(
  value: string,
  brokerId: string,
  limits: { maxCompletedJobs: number; maxJobResults: number }
): PostgresQueueManager {
  return new PostgresQueueManager({
    ...limits,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

async function complete(
  producer: PostgresQueueManager,
  worker: PostgresQueueManager,
  queue: string,
  id: string,
  removeOnComplete: boolean
): Promise<void> {
  const job = await producer.push(queue, {
    customId: id,
    data: { id },
    removeOnComplete,
  });
  const claimed = await worker.pullWithLock(queue, `worker-${id}`);
  expect(claimed.job?.id).toBe(job.id);
  await worker.ack(job.id, { id }, claimed.token!);
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL completion retention', () => {
  test.skipIf(!postgresUrl)(
    'bounds unpinned tombstones while preserving live dependency evidence',
    async () => {
      const value = namespace('tombstones');
      const first = manager(value, 'retention-a', { maxCompletedJobs: 2, maxJobResults: 2 });
      const second = manager(value, 'retention-b', { maxCompletedJobs: 2, maxJobResults: 2 });
      const sql = new SQL(postgresUrl!, { max: 1 });
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await complete(first, second, 'source', 'pinned', true);
        const consumer = await first.push('consumers', {
          customId: 'consumer',
          data: {},
          dependsOn: ['pinned'],
        });
        for (let index = 0; index < 5; index++) {
          await complete(first, second, 'source', `unpinned-${index}`, true);
        }

        const [counts] = await sql<{ total: number; pinned: number; unpinned: number }[]>`
          SELECT COUNT(*)::int AS total,
                 COUNT(*) FILTER (WHERE job_id = 'pinned')::int AS pinned,
                 COUNT(*) FILTER (WHERE job_id <> 'pinned')::int AS unpinned
          FROM bunqueue_completions
          WHERE namespace = ${value}
        `;
        expect(counts).toEqual({ total: 3, pinned: 1, unpinned: 2 });
        expect(await second.getChildrenValues(consumer.id)).toEqual({
          'source:pinned': { id: 'pinned' },
        });
      } finally {
        await Promise.allSettled([
          first.shutdownPostgres(),
          second.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'converges a multi-batch tombstone backlog after restart and a lower limit',
    async () => {
      const value = namespace('restart-backlog');
      const generousLimits = { maxCompletedJobs: 2_000, maxJobResults: 2 };
      const bootstrap = manager(value, 'restart-backlog-bootstrap', generousLimits);
      const sql = new SQL(postgresUrl!, { max: 1 });
      let reader: PostgresQueueManager | null = null;
      try {
        await bootstrap.waitUntilReady();
        await complete(bootstrap, bootstrap, 'source', 'pinned', true);
        const consumer = await bootstrap.push('consumers', {
          customId: 'consumer',
          data: {},
          dependsOn: ['pinned'],
        });
        await sql`
          INSERT INTO bunqueue_completions
            (namespace, job_id, queue, completed_at, result)
          SELECT ${value}, 'backlog-' || item::text, 'source', item::bigint, NULL
          FROM generate_series(1, 1105) AS item
        `;
        await bootstrap.shutdownPostgres();

        reader = manager(value, 'restart-backlog-reader', {
          maxCompletedJobs: 2,
          maxJobResults: 2,
        });
        await reader.waitUntilReady();
        const [counts] = await sql<{ total: number; pinned: number; unpinned: number }[]>`
          SELECT COUNT(*)::int AS total,
                 COUNT(*) FILTER (WHERE job_id = 'pinned')::int AS pinned,
                 COUNT(*) FILTER (WHERE job_id <> 'pinned')::int AS unpinned
          FROM bunqueue_completions
          WHERE namespace = ${value}
        `;
        expect(counts).toEqual({ total: 3, pinned: 1, unpinned: 2 });
        expect(await reader.getChildrenValues(consumer.id)).toEqual({
          'source:pinned': { id: 'pinned' },
        });
      } finally {
        await Promise.allSettled([
          bootstrap.shutdownPostgres(),
          ...(reader ? [reader.shutdownPostgres()] : []),
          sql.close({ timeout: 5 }),
        ]);
      }
    },
    15_000
  );

  test.skipIf(!postgresUrl)(
    'drains a multi-batch tombstone backlog after a removed completion',
    async () => {
      const value = namespace('post-completion-backlog');
      const queueManager = manager(value, 'post-completion-backlog', {
        maxCompletedJobs: 2,
        maxJobResults: 2,
      });
      const sql = new SQL(postgresUrl!, { max: 1 });
      try {
        await queueManager.waitUntilReady();
        await sql`
          INSERT INTO bunqueue_completions
            (namespace, job_id, queue, completed_at, result)
          SELECT ${value}, 'backlog-' || item::text, 'source', item::bigint, NULL
          FROM generate_series(1, 1105) AS item
        `;

        await complete(queueManager, queueManager, 'source', 'retention-trigger', true);

        const [row] = await sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count FROM bunqueue_completions
          WHERE namespace = ${value}
        `;
        expect(row.count).toBe(2);
      } finally {
        await Promise.allSettled([queueManager.shutdownPostgres(), sql.close({ timeout: 5 })]);
      }
    },
    15_000
  );

  test.skipIf(!postgresUrl)('bounds startup jobs and result cache independently', async () => {
    const value = namespace('snapshot');
    const limits = { maxCompletedJobs: 2, maxJobResults: 2 };
    const writer = manager(value, 'snapshot-writer', limits);
    const worker = manager(value, 'snapshot-worker', limits);
    const ids = Array.from({ length: 5 }, (_, index) => `completed-${index}`);
    try {
      await Promise.all([writer.waitUntilReady(), worker.waitUntilReady()]);
      for (const id of ids) await complete(writer, worker, 'completed', id, false);
    } finally {
      await Promise.allSettled([writer.shutdownPostgres(), worker.shutdownPostgres()]);
    }

    const reader = manager(value, 'snapshot-reader', limits);
    const sql = new SQL(postgresUrl!, { max: 1 });
    try {
      await reader.waitUntilReady();
      expect(reader.getCompletedJobs().size).toBe(2);
      expect(ids.filter((id) => reader.getResult(id) !== undefined)).toHaveLength(2);
      const [row] = await sql<{ count: number }[]>`
        SELECT COUNT(*)::int AS count FROM bunqueue_jobs
        WHERE namespace = ${value} AND queue = 'completed' AND state = 'completed'
      `;
      expect(row.count).toBe(5);
    } finally {
      await Promise.allSettled([reader.shutdownPostgres(), sql.close({ timeout: 5 })]);
    }
  });

  test.skipIf(!postgresUrl)(
    'keeps dependency results durable with a zero-sized cache',
    async () => {
      const value = namespace('zero-results');
      const first = manager(value, 'zero-results-a', { maxCompletedJobs: 10, maxJobResults: 0 });
      const second = manager(value, 'zero-results-b', { maxCompletedJobs: 10, maxJobResults: 0 });
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await complete(first, second, 'source', 'producer', false);
        const consumer = await first.push('consumers', {
          customId: 'consumer',
          data: {},
          dependsOn: ['producer'],
        });

        expect(first.getResult('producer')).toBeUndefined();
        expect(second.getResult('producer')).toBeUndefined();
        expect(await second.getResultAsync(jobId('producer'))).toEqual({ id: 'producer' });
        expect(await first.waitForJobCompletion(jobId('producer'), 100)).toBe(true);
        expect(await second.getChildrenValues(consumer.id)).toEqual({
          'source:producer': { id: 'producer' },
        });
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );
});
