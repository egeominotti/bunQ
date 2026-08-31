import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-group-order-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function orderedJob(
  queue: string,
  label: string,
  id: string,
  timestamp: number,
  delay: number
) {
  return createJob(jobId(id), queue, {
    data: { label },
    delay,
    groupId: 'A',
    timestamp,
  });
}

async function cleanup(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    for (const table of [
      'bunqueue_job_logs',
      'bunqueue_flow_failures',
      'bunqueue_dependencies',
      'bunqueue_completions',
      'bunqueue_jobs',
      'bunqueue_group_state',
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

describe('PostgreSQL durable group order and retention', () => {
  test.skipIf(!postgresUrl)(
    'preserves same-time FIFO across brokers and a complete restart',
    async () => {
      const value = namespace('restart');
      const first = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'first',
      });
      const second = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'second',
      });
      const timestamp = Date.now() - 60_000;
      try {
        await Promise.all([first.initialize(), second.initialize()]);
        await first.insert(orderedJob('fifo', 'first', 'z-first-broker', timestamp, 60_000));
        await second.insert(orderedJob('fifo', 'second', 'a-second-broker', timestamp, 60_000));
      } finally {
        await Promise.allSettled([first.close(), second.close()]);
      }

      const restored = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'restored',
      });
      try {
        await restored.initialize();
        const claimed = await restored.claim('fifo', 2);
        expect(claimed.map(({ job }) => (job.data as { label: string }).label)).toEqual([
          'first',
          'second',
        ]);
      } finally {
        await restored.close();
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'allocates FIFO order across the 1000-row batch boundary',
    async () => {
      const value = namespace('batch-boundary');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'batch',
      });
      const count = 1002;
      const timestamp = Date.now() - 60_000;
      try {
        await store.initialize();
        const jobs = Array.from({ length: count }, (_, index) =>
          orderedJob(
            'batch',
            String(index),
            `batch-${String(count - index).padStart(4, '0')}`,
            timestamp,
            60_000
          )
        );
        await store.insertMany(jobs);
        const claimed = await store.claim('batch', count);
        expect(claimed).toHaveLength(count);
        expect(claimed.map(({ job }) => Number((job.data as { label: string }).label))).toEqual(
          Array.from({ length: count }, (_, index) => index)
        );
      } finally {
        await store.close();
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'retains an unexpired default rate window after the group becomes empty',
    async () => {
      const value = namespace('window');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'window',
      });
      const sql = new SQL(postgresUrl!, { max: 1 });
      const options = { limit: { max: 1, duration: 60_000 } };
      try {
        await store.initialize();
        await store.insert(orderedJob('window', 'first', 'window-first', Date.now(), 0));
        const [first] = await store.claim('window', 1, 'window', undefined, options);
        await store.complete(first.job.id, first.token);

        const [state] = await sql<
          Array<{
            last_served: number | string | bigint | null;
            rate_count: number | string | bigint;
          }>
        >`
          SELECT last_served, rate_count
          FROM bunqueue_group_state
          WHERE namespace = ${value} AND queue = 'window' AND group_id = 'A'
        `;
        expect(state).toBeDefined();
        expect(state.last_served).toBeNull();
        expect(Number(state.rate_count)).toBe(1);

        await store.insert(orderedJob('window', 'second', 'window-second', Date.now(), 0));
        expect(await store.claim('window', 1, 'window', undefined, options)).toHaveLength(0);
      } finally {
        await Promise.allSettled([store.close(), sql.close({ timeout: 5 })]);
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'keeps durable overrides but deletes their row after the final override is removed',
    async () => {
      const value = namespace('override');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'override',
      });
      const sql = new SQL(postgresUrl!, { max: 1 });
      try {
        await store.initialize();
        await store.setGroupConcurrency('override', 'A', 2);
        await store.insert(orderedJob('override', 'only', 'override-only', Date.now(), 0));
        const [claimed] = await store.claim('override', 1, 'override', undefined, {
          concurrency: 4,
        });
        await store.complete(claimed.job.id, claimed.token);

        const [retained] = await sql<
          Array<{
            concurrency_limit: number | string | bigint | null;
            last_served: number | string | bigint | null;
          }>
        >`
          SELECT concurrency_limit, last_served
          FROM bunqueue_group_state
          WHERE namespace = ${value} AND queue = 'override' AND group_id = 'A'
        `;
        expect(Number(retained.concurrency_limit)).toBe(2);
        expect(retained.last_served).toBeNull();

        expect(await store.removeGroupConcurrency('override', 'A')).toBe(1);
        const [remaining] = await sql<{ count: number | string | bigint }[]>`
          SELECT COUNT(*)::bigint AS count
          FROM bunqueue_group_state
          WHERE namespace = ${value} AND queue = 'override' AND group_id = 'A'
        `;
        expect(Number(remaining.count)).toBe(0);
      } finally {
        await Promise.allSettled([store.close(), sql.close({ timeout: 5 })]);
      }
    },
    30_000
  );
});
