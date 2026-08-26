import { afterAll, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type {
  PostgresQueueStore,
  PostgresStoreEvent,
} from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-batch-performance-${Date.now()}-${crypto.randomUUID()}`;
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

test.skipIf(!postgresUrl)(
  'enqueues and completes 500 PostgreSQL jobs within the batch latency budgets',
  async () => {
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: namespace(),
        brokerId: 'batch-performance',
      },
    });
    try {
      await manager.waitUntilReady();
      const inputs = Array.from({ length: 500 }, (_, index) => ({ data: { index } }));
      const enqueueStartedAt = performance.now();
      const ids = await manager.pushBatch('batch-performance', inputs);
      const enqueueElapsedMs = performance.now() - enqueueStartedAt;
      const claimed = await manager.pullBatchWithLock(
        'batch-performance',
        500,
        'batch-worker',
        0,
        120_000
      );
      expect(claimed.jobs.map((job) => job.id)).toEqual(ids);

      const startedAt = performance.now();
      await manager.ackBatchWithResults(
        claimed.jobs.map((job, index) => ({
          id: job.id,
          token: claimed.tokens[index],
          result: { index },
        }))
      );
      const elapsedMs = performance.now() - startedAt;

      expect(enqueueElapsedMs).toBeLessThan(2_500);
      expect(elapsedMs).toBeLessThan(15_000);
      expect(manager.getQueueJobCounts('batch-performance')).toMatchObject({
        active: 0,
        completed: 500,
        failed: 0,
        waiting: 0,
      });
      expect(manager.getResult(ids[0])).toEqual({ index: 0 });
      expect(manager.getResult(ids[499])).toEqual({ index: 499 });
    } finally {
      await manager.shutdownPostgres();
    }
  },
  120_000
);

test.skipIf(!postgresUrl)(
  'keeps custom-ID and deduplication batch admission on the bounded-latency path',
  async () => {
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: namespace(),
        brokerId: 'batch-unique-performance',
      },
    });
    try {
      await manager.waitUntilReady();
      const cases = [
        {
          queue: 'batch-custom-id',
          inputs: Array.from({ length: 500 }, (_, index) => ({
            data: { index },
            customId: `custom-${index}`,
          })),
        },
        {
          queue: 'batch-deduplication',
          inputs: Array.from({ length: 500 }, (_, index) => ({
            data: { index },
            uniqueKey: `dedup-${index}`,
          })),
        },
      ];

      for (const { queue, inputs } of cases) {
        const startedAt = performance.now();
        const ids = await manager.pushBatch(queue, inputs);
        const elapsedMs = performance.now() - startedAt;
        expect(ids).toHaveLength(inputs.length);
        expect(new Set(ids).size).toBe(inputs.length);
        expect(elapsedMs).toBeLessThan(2_500);
      }
    } finally {
      await manager.shutdownPostgres();
    }
  },
  120_000
);

test.skipIf(!postgresUrl)(
  'does not reload every previously admitted job after each PostgreSQL batch',
  async () => {
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: namespace(),
        brokerId: 'batch-snapshot-performance',
      },
    });
    try {
      await manager.waitUntilReady();
      const store = (manager as unknown as { postgresStore: PostgresQueueStore }).postgresStore;
      const originalList = store.list;
      let fullQueueReloads = 0;
      store.list = (...args) => {
        fullQueueReloads++;
        return originalList(...args);
      };

      for (let batch = 0; batch < 3; batch++) {
        await manager.pushBatch(
          'batch-snapshot-performance',
          Array.from({ length: 100 }, (_, index) => ({ data: { index: batch * 100 + index } }))
        );
      }

      expect(fullQueueReloads).toBe(0);
      expect(manager.getQueueJobCounts('batch-snapshot-performance')).toMatchObject({
        waiting: 300,
      });
    } finally {
      await manager.shutdownPostgres();
    }
  }
);

test.skipIf(!postgresUrl)(
  'does not let an affected-ID refresh overwrite a newer queue event',
  async () => {
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: namespace(),
        brokerId: 'batch-refresh-race',
      },
    });
    try {
      await manager.waitUntilReady();
      const ids = await manager.pushBatch(
        'batch-refresh-race',
        Array.from({ length: 100 }, (_, index) => ({ data: { index } }))
      );
      await Bun.sleep(50);
      const internals = manager as unknown as {
        postgresStore: PostgresQueueStore;
        refreshJobs(ids: typeof ids): Promise<void>;
        onStoreEvent(event: PostgresStoreEvent): void;
      };
      const originalGetJobs = internals.postgresStore.getJobs;
      let reads = 0;
      let removedId = ids[0];
      internals.postgresStore.getJobs = async (...args) => {
        const rows = await originalGetJobs(...args);
        reads++;
        if (reads === 1) {
          removedId = rows[0].job.id;
          internals.onStoreEvent({
            id: Number.MAX_SAFE_INTEGER,
            queue: 'batch-refresh-race',
            type: 'removed',
            jobId: removedId,
            occurredAt: Date.now(),
            job: null,
            state: null,
            removed: true,
          });
        }
        return rows;
      };

      await internals.refreshJobs(ids);

      expect(reads).toBe(1);
      expect(manager.getJobs('batch-refresh-race').some((job) => job.id === removedId)).toBe(false);
      expect(manager.getQueueJobCounts('batch-refresh-race')).toMatchObject({ waiting: 99 });
    } finally {
      await manager.shutdownPostgres();
    }
  }
);

test.skipIf(!postgresUrl)(
  'allows default-policy claims to share the queue-state lock',
  async () => {
    const value = namespace();
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: value,
        brokerId: 'shared-state-lock',
      },
    });
    const blocker = new SQL(postgresUrl!, { max: 1 });
    const lockAcquired = Promise.withResolvers<undefined>();
    const releaseLock = Promise.withResolvers<undefined>();
    let blockingTransaction: Promise<void> | undefined;
    let claimPromise: ReturnType<typeof manager.pullBatchWithLock> | undefined;
    try {
      await manager.waitUntilReady();
      await manager.pushBatch(
        'shared-state-lock',
        Array.from({ length: 10 }, (_, index) => ({ data: { index } }))
      );
      await blocker`
        INSERT INTO bunqueue_queue_state (namespace, queue)
        VALUES (${value}, ${'shared-state-lock'})
        ON CONFLICT DO NOTHING
      `;
      blockingTransaction = blocker.begin(async (tx) => {
        await tx`
          SELECT queue
          FROM bunqueue_queue_state
          WHERE namespace = ${value} AND queue = ${'shared-state-lock'}
          FOR SHARE
        `;
        lockAcquired.resolve(undefined);
        await releaseLock.promise;
      });
      await lockAcquired.promise;

      claimPromise = manager.pullBatchWithLock('shared-state-lock', 10, 'shared-worker');
      const completedWithoutExclusiveLock = await Promise.race([
        claimPromise.then(() => true),
        Bun.sleep(1_000).then(() => false),
      ]);

      expect(completedWithoutExclusiveLock).toBe(true);
      const claimed = await claimPromise;
      expect(claimed.jobs).toHaveLength(10);
    } finally {
      releaseLock.resolve(undefined);
      await blockingTransaction;
      await claimPromise?.catch(() => undefined);
      await Promise.allSettled([manager.shutdownPostgres(), blocker.close({ timeout: 5 })]);
    }
  },
  15_000
);
