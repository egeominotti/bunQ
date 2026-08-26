import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { addPostgresJobLog } from '../src/infrastructure/persistence/postgres/logs';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-concurrency-${label}-${Date.now()}-${crypto.randomUUID()}`;
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
      'bunqueue_event_prune_watermarks',
      'bunqueue_events',
      'bunqueue_event_commits',
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

describe('PostgreSQL write-concurrency regressions', () => {
  test.skipIf(!postgresUrl)(
    'admits reversed duplicate batches concurrently without a PostgreSQL deadlock',
    async () => {
      const value = namespace('batch-order');
      const first = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'batch-order-a',
      });
      const second = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'batch-order-b',
      });
      try {
        await Promise.all([first.initialize(), second.initialize()]);
        for (let round = 0; round < 8; round++) {
          const queue = `batch-order-${round}`;
          const jobs = Array.from({ length: 500 }, (_, index) =>
            createJob(jobId(`${queue}-${index}`), queue, { data: { round, index } })
          );
          const outcomes = await Promise.allSettled([
            first.insertMany(jobs),
            second.insertMany([...jobs].reverse()),
          ]);
          const failures = outcomes.filter(
            (outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected'
          );
          expect(failures.map((failure) => String(failure.reason))).toEqual([]);
          expect(await first.getCounts(queue)).toMatchObject({ waiting: jobs.length });
        }
      } finally {
        await Promise.allSettled([first.close(), second.close()]);
      }
    },
    60_000
  );

  test.skipIf(!postgresUrl)(
    'serializes log retention so concurrent writers keep the exact configured maximum',
    async () => {
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: namespace('log-retention'),
        brokerId: 'log-retention',
        poolSize: 16,
      });
      try {
        await store.initialize();
        const job = createJob(jobId('log-retention-job'), 'log-retention', { data: {} });
        await store.insert(job);
        const inserted = await Promise.all(
          Array.from({ length: 64 }, (_, index) =>
            addPostgresJobLog(store.context, job.id, `entry-${index}`, 'info', 1)
          )
        );
        expect(inserted.every(Boolean)).toBe(true);
        expect(await store.getLogs(job.id)).toHaveLength(1);
      } finally {
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'cannot commit an orphan log while another broker removes its job',
    async () => {
      const value = namespace('log-remove');
      const writer = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'log-remove-writer',
      });
      const remover = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'log-remove-remover',
      });
      try {
        await Promise.all([writer.initialize(), remover.initialize()]);
        const job = createJob(jobId('log-remove-job'), 'log-remove', { data: {} });
        await writer.insert(job);
        await writer.context.sql.unsafe(`
          CREATE OR REPLACE FUNCTION bunqueue_test_delay_log_insert()
          RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN
            IF NEW.message = 'orphan-race-barrier' THEN
              PERFORM pg_sleep(0.5);
            END IF;
            RETURN NEW;
          END
          $$
        `);
        await writer.context.sql.unsafe(`
          DROP TRIGGER IF EXISTS bunqueue_test_delay_log_insert ON bunqueue_job_logs
        `);
        await writer.context.sql.unsafe(`
          CREATE TRIGGER bunqueue_test_delay_log_insert
          AFTER INSERT ON bunqueue_job_logs
          FOR EACH ROW EXECUTE FUNCTION bunqueue_test_delay_log_insert()
        `);

        const adding = writer.addLog(job.id, 'orphan-race-barrier', 'info');
        await Bun.sleep(100);
        expect(await remover.remove(job.id)).toBe(true);
        expect(await adding).toBe(true);
        const [orphan] = await writer.context.sql<{ count: number | string | bigint }[]>`
          SELECT COUNT(*)::bigint AS count
          FROM bunqueue_job_logs
          WHERE namespace = ${value} AND job_id = ${String(job.id)}
        `;
        expect(Number(orphan.count)).toBe(0);
      } finally {
        await Promise.allSettled([
          writer.context.sql.unsafe(
            'DROP TRIGGER IF EXISTS bunqueue_test_delay_log_insert ON bunqueue_job_logs'
          ),
        ]);
        await Promise.allSettled([
          writer.context.sql.unsafe('DROP FUNCTION IF EXISTS bunqueue_test_delay_log_insert()'),
        ]);
        await Promise.allSettled([writer.close(), remover.close()]);
      }
    }
  );
});
