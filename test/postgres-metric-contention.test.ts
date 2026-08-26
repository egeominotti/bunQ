import { afterAll, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { recordPostgresMetricAdditions } from '../src/infrastructure/persistence/postgres/metricWrites';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-metric-contention-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

async function cleanup(value: string): Promise<void> {
  const sql = new SQL(postgresUrl!, { max: 2 });
  try {
    await sql.begin(async (tx) => {
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
        await tx.unsafe(`DELETE FROM ${table} WHERE namespace = $1`, [value]);
      }
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(value);
});

test.skipIf(!postgresUrl)(
  'keeps the latest metric timestamp monotonic across reversed commit order',
  async () => {
    const value = namespace('timestamp-order');
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace: value,
      brokerId: 'metric-timestamp-order',
    });
    const newer = new SQL(postgresUrl!, { max: 1 });
    const older = new SQL(postgresUrl!, { max: 1 });
    const newerLocked = Promise.withResolvers<undefined>();
    const releaseNewer = Promise.withResolvers<undefined>();
    try {
      await store.initialize();
      const newerWrite = newer.begin(async (tx) => {
        await recordPostgresMetricAdditions(
          tx,
          store.context,
          'completed',
          [{ queue: 'timestamp-order', count: 1 }],
          120_000
        );
        newerLocked.resolve(undefined);
        await releaseNewer.promise;
      });
      await newerLocked.promise;
      const olderWrite = older.begin((tx) =>
        recordPostgresMetricAdditions(
          tx,
          store.context,
          'completed',
          [{ queue: 'timestamp-order', count: 1 }],
          60_000
        )
      );
      releaseNewer.resolve(undefined);
      await Promise.all([newerWrite, olderWrite]);

      const metrics = await store.getQueueMetrics('timestamp-order', 'completed');
      expect(metrics.meta).toEqual({ count: 2, prevTS: 120_000, prevCount: 1 });
      expect(metrics.data).toEqual([1, 1]);
    } finally {
      releaseNewer.resolve(undefined);
      await Promise.allSettled([
        store.close(),
        newer.close({ timeout: 5 }),
        older.close({ timeout: 5 }),
      ]);
    }
  }
);

test.skipIf(!postgresUrl)(
  'keeps exact durable metrics while four brokers complete batches concurrently',
  async () => {
    const value = namespace('four-brokers');
    const stores = Array.from(
      { length: 4 },
      (_, index) =>
        new PostgresQueueStore({
          url: postgresUrl!,
          namespace: value,
          brokerId: `metric-broker-${index}`,
        })
    );
    try {
      await Promise.all(stores.map((store) => store.initialize()));
      await stores[0].insertMany(
        Array.from({ length: 400 }, (_, index) =>
          createJob(jobId(`metric-job-${index}`), 'metric-contention', { data: { index } })
        )
      );
      const claims = await Promise.all(
        stores.map((store, index) =>
          store.claim('metric-contention', 100, `metric-worker-${index}`, 60_000)
        )
      );
      expect(claims.flat()).toHaveLength(400);

      await Promise.all(
        claims.map((batch, index) =>
          stores[index].completeMany(
            batch.map((claim) => ({
              id: claim.job.id,
              token: claim.token,
              result: { broker: index },
            }))
          )
        )
      );

      const metrics = await stores[0].getQueueMetrics('metric-contention', 'completed');
      const lifetime = await stores[0].loadLifetimeMetrics();
      expect(metrics.meta.count).toBe(400);
      expect(metrics.meta.prevCount).toBe(400);
      expect(metrics.data[0]).toBe(400);
      expect(lifetime.totalCompleted).toBe(400n);
      expect(lifetime.queues).toContainEqual({
        queue: 'metric-contention',
        totalCompleted: 400n,
        totalFailed: 0n,
      });
    } finally {
      await Promise.allSettled(stores.map((store) => store.close()));
    }
  },
  30_000
);

test.skipIf(!postgresUrl)(
  'keeps lifetime totals exact when metric buckets are disabled',
  async () => {
    const value = namespace('zero-buckets');
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace: value,
      brokerId: 'metric-zero-buckets',
      maxMetricDataPoints: 0,
    });
    try {
      await store.initialize();
      await store.insertMany(
        Array.from({ length: 10 }, (_, index) =>
          createJob(jobId(`zero-metric-job-${index}`), 'zero-metric-buckets', { data: { index } })
        )
      );
      const claims = await store.claim('zero-metric-buckets', 10, 'metric-worker', 60_000);
      await store.completeMany(
        claims.map((claim) => ({ id: claim.job.id, token: claim.token, result: null }))
      );

      const metrics = await store.getQueueMetrics('zero-metric-buckets', 'completed');
      expect(metrics).toMatchObject({
        meta: { count: 10, prevCount: 10 },
        data: [],
        count: 0,
      });

      const sql = new SQL(postgresUrl!, { max: 1 });
      try {
        await sql.begin((tx) =>
          recordPostgresMetricAdditions(
            tx,
            store.context,
            'completed',
            [{ queue: 'zero-metric-buckets', count: 2 }],
            metrics.meta.prevTS
          )
        );
        expect(
          (await store.getQueueMetrics('zero-metric-buckets', 'completed')).meta
        ).toMatchObject({ count: 12, prevCount: 12 });
      } finally {
        await sql.close({ timeout: 5 });
      }
    } finally {
      await store.close();
    }
  }
);
