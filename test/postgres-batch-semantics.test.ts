import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-batch-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

async function cleanup(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    for (const table of [
      'bunqueue_metric_buckets',
      'bunqueue_metric_totals',
      'bunqueue_workers',
      'bunqueue_crons',
      'bunqueue_job_logs',
      'bunqueue_repeat_links',
      'bunqueue_flow_failures',
      'bunqueue_dependencies',
      'bunqueue_completions',
      'bunqueue_jobs',
      'bunqueue_queue_state',
      'bunqueue_events',
      'bunqueue_brokers',
    ]) {
      await sql.unsafe(`DELETE FROM ${table} WHERE namespace = $1`, [value]);
    }
  } finally {
    await sql.close({ timeout: 5 });
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(postgresUrl, value);
});

describe('PostgreSQL set-based batch semantics', () => {
  test.skipIf(!postgresUrl)(
    'rolls back the whole completion batch when one token is invalid',
    async () => {
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: namespace('atomic-completion'),
          brokerId: 'atomic-completion',
        },
      });
      try {
        await manager.waitUntilReady();
        const ids = await manager.pushBatch(
          'atomic-completion',
          Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
        );
        const claims = await manager.pullBatchWithLock(
          'atomic-completion',
          3,
          'batch-worker',
          0,
          60_000
        );
        await expect(
          manager.ackBatchWithResults(
            claims.jobs.map((job, index) => ({
              id: job.id,
              token: index === 2 ? 'invalid-token' : claims.tokens[index],
              result: { index },
            }))
          )
        ).rejects.toThrow('Invalid or expired lock token');
        expect(manager.getQueueJobCounts('atomic-completion')).toMatchObject({
          active: 3,
          completed: 0,
        });

        await manager.ackBatchWithResults(
          claims.jobs.map((job, index) => ({
            id: job.id,
            token: claims.tokens[index],
            result: { index },
          }))
        );
        expect(manager.getQueueJobCounts('atomic-completion')).toMatchObject({
          active: 0,
          completed: 3,
        });
        expect(ids.map((id) => manager.getResult(id))).toEqual([
          { index: 0 },
          { index: 1 },
          { index: 2 },
        ]);
      } finally {
        await manager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'falls back atomically on an existing ID without duplicating admission state',
    async () => {
      const value = namespace('admission-conflict');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'admission-conflict',
      });
      try {
        await store.initialize();
        const existingId = jobId('batch-existing-id');
        await store.insert(createJob(existingId, 'admission-conflict', { data: { version: 1 } }));
        const fresh = createJob(jobId('batch-fresh-id'), 'admission-conflict', {
          data: { version: 1 },
        });
        const stored = await store.insertMany([
          createJob(existingId, 'admission-conflict', { data: { version: 2 } }),
          fresh,
        ]);

        expect(stored[0].data).toEqual({ version: 1 });
        expect(stored[1].id).toBe(fresh.id);
        expect((await store.getJob(fresh.id))?.job.timeline).toHaveLength(1);
        const [events] = await store.context.sql<{ count: number | string | bigint }[]>`
          SELECT COUNT(*)::bigint AS count
          FROM bunqueue_events
          WHERE namespace = ${value} AND event_type = 'pushed'
        `;
        expect(Number(events.count)).toBe(2);
      } finally {
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'falls back atomically when one batch deduplication key already exists',
    async () => {
      const value = namespace('dedup-conflict');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'dedup-conflict',
      });
      try {
        await store.initialize();
        const existing = createJob(jobId('dedup-existing'), 'dedup-conflict', {
          data: { version: 1 },
          uniqueKey: 'shared-key',
        });
        await store.insert(existing);
        const duplicate = createJob(jobId('dedup-candidate'), 'dedup-conflict', {
          data: { version: 2 },
          uniqueKey: 'shared-key',
        });
        const fresh = createJob(jobId('dedup-fresh'), 'dedup-conflict', {
          data: { version: 1 },
          uniqueKey: 'fresh-key',
        });

        const stored = await store.insertMany([duplicate, fresh]);

        expect(stored[0].id).toBe(existing.id);
        expect(stored[0].data).toEqual({ version: 1 });
        expect(stored[1].id).toBe(fresh.id);
        expect(await store.getJob(duplicate.id)).toBeNull();
        const events = await store.context.sql<
          Array<{ event_type: string; count: number | string | bigint }>
        >`
          SELECT event_type, COUNT(*)::bigint AS count
          FROM bunqueue_events
          WHERE namespace = ${value}
          GROUP BY event_type
        `;
        expect(
          Object.fromEntries(events.map((row) => [row.event_type, Number(row.count)]))
        ).toEqual({
          duplicated: 1,
          pushed: 2,
        });
      } finally {
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'retains the exact newest event window across batch and single admission',
    async () => {
      const value = namespace('event-retention');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'event-retention',
        maxQueueEvents: 5,
      });
      try {
        await store.initialize();
        await store.insertMany(
          Array.from({ length: 12 }, (_, index) =>
            createJob(jobId(`retained-${index}`), 'event-retention', { data: { index } })
          )
        );
        await store.insert(
          createJob(jobId('retained-single'), 'event-retention', { data: { index: 12 } })
        );

        const events = await store.context.sql<Array<{ job_id: string }>>`
          SELECT job_id
          FROM bunqueue_events
          WHERE namespace = ${value} AND queue = 'event-retention'
          ORDER BY id
        `;
        expect(events.map((row) => row.job_id)).toEqual([
          'retained-8',
          'retained-9',
          'retained-10',
          'retained-11',
          'retained-single',
        ]);
      } finally {
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'preserves priority and mixed FIFO/LIFO ordering in a set-based claim',
    async () => {
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: namespace('claim-order'),
        brokerId: 'claim-order',
      });
      try {
        await store.initialize();
        const now = Date.now();
        const jobs = [
          createJob(jobId('fifo-a'), 'claim-order', { data: { role: 'fifo-a' }, priority: 1 }, now),
          createJob(jobId('fifo-b'), 'claim-order', { data: { role: 'fifo-b' }, priority: 1 }, now),
          createJob(
            jobId('lifo-a'),
            'claim-order',
            { data: { role: 'lifo-a' }, priority: 1, lifo: true },
            now
          ),
          createJob(
            jobId('lifo-b'),
            'claim-order',
            { data: { role: 'lifo-b' }, priority: 1, lifo: true },
            now
          ),
          createJob(jobId('high'), 'claim-order', { data: { role: 'high' }, priority: 10 }, now),
        ];
        await store.insertMany(jobs);
        const claimed = await store.claim('claim-order', jobs.length, 'claim-order-worker');
        expect(claimed.map(({ job }) => (job.data as { role: string }).role)).toEqual([
          'high',
          'lifo-b',
          'lifo-a',
          'fifo-a',
          'fifo-b',
        ]);
        await store.completeMany(claimed.map(({ job, token }) => ({ id: job.id, token })));
      } finally {
        await store.close();
      }
    }
  );
});
