import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

interface PausableEventStream {
  cursor: number;
  subscription: { unlisten(): Promise<void> } | null;
  pollTimer: ReturnType<typeof setInterval> | null;
  drain(): Promise<void>;
}

interface RefreshInspectableManager {
  readonly refreshes: Map<string, Promise<void>>;
}

function namespace(label: string): string {
  const value = `test-event-catchup-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    maxQueueEvents: 1,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function managerStore(value: PostgresQueueManager): PostgresQueueStore {
  return (value as unknown as { postgresStore: PostgresQueueStore }).postgresStore;
}

function eventStream(value: PostgresQueueManager): PausableEventStream {
  return managerStore(value).events as unknown as PausableEventStream;
}

async function eventually(condition: () => boolean | Promise<boolean>, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  do {
    if (await condition()) return true;
    await Bun.sleep(20);
  } while (Date.now() < deadline);
  return false;
}

async function cleanup(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    await sql.begin(async (tx) => {
      await tx`DELETE FROM bunqueue_metric_buckets WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_metric_totals WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_workers WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_crons WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_job_logs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_repeat_links WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_flow_failures WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_dependencies WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_completions WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_jobs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_queue_state WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_prune_watermarks WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_events WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_commits WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_brokers WHERE namespace = ${value}`;
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

async function failTerminal(
  producer: PostgresQueueManager,
  consumer: PostgresQueueManager,
  queue: string,
  index: number
): Promise<JobId> {
  const job = await producer.push(queue, { data: { index }, maxAttempts: 1 });
  const claimed = await consumer.pullWithLock(queue, `worker-${index}`);
  await consumer.fail(job.id, `failure-${index}`, claimed.token ?? undefined);
  return job.id;
}

async function pauseNotifications(value: PostgresQueueManager): Promise<PausableEventStream> {
  const stream = eventStream(value);
  if (stream.pollTimer) clearInterval(stream.pollTimer);
  stream.pollTimer = null;
  await stream.subscription?.unlisten();
  stream.subscription = null;
  return stream;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(postgresUrl, value);
});

describe('PostgreSQL event catch-up regressions', () => {
  test.skipIf(!postgresUrl)(
    'retries a failed authoritative refresh after a prune invalidation',
    async () => {
      const value = namespace('refresh-retry');
      const active = manager(value, 'refresh-retry-active');
      const remote = manager(value, 'refresh-retry-remote');
      try {
        await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
        const store = managerStore(remote);
        const originalLoadQueue = store.loadQueueReadModel;
        const retryGate = Promise.withResolvers<undefined>();
        let refreshAttempts = 0;
        store.loadQueueReadModel = async (...args) => {
          refreshAttempts++;
          if (refreshAttempts === 2) await retryGate.promise;
          if (refreshAttempts <= 3) throw new Error('injected queue refresh failure');
          return await originalLoadQueue(...args);
        };
        try {
          const queue = 'refresh-retry';
          const ids = await active.pushBatch(
            queue,
            Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
          );
          expect(
            await eventually(
              () =>
                refreshAttempts === 2 &&
                remote.getStorageStatus().error?.includes('injected queue refresh failure') === true
            )
          ).toBe(true);
          retryGate.resolve(undefined);
          expect(
            await eventually(
              () =>
                refreshAttempts >= 4 &&
                remote.getStorageStatus().error === null &&
                remote
                  .getJobs(queue, { state: 'waiting' })
                  .map((job) => job.id)
                  .sort()
                  .join() === [...ids].sort().join()
            )
          ).toBe(true);
        } finally {
          retryGate.resolve(undefined);
          store.loadQueueReadModel = originalLoadQueue;
        }
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)('stops a persistent queue refresh retry on shutdown', async () => {
    const value = namespace('refresh-shutdown');
    const active = manager(value, 'refresh-shutdown-active');
    const remote = manager(value, 'refresh-shutdown-remote');
    let remoteClosed = false;
    try {
      await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
      const store = managerStore(remote);
      const originalLoadQueue = store.loadQueueReadModel;
      let refreshAttempts = 0;
      store.loadQueueReadModel = () => {
        refreshAttempts++;
        return Promise.reject(new Error('persistent queue refresh failure'));
      };
      try {
        await active.pushBatch(
          'refresh-shutdown',
          Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
        );
        expect(await eventually(() => refreshAttempts >= 2)).toBe(true);
        expect(
          await Promise.race([
            remote.shutdownPostgres().then(() => true),
            Bun.sleep(1_500).then(() => false),
          ])
        ).toBe(true);
        remoteClosed = true;
        const attemptsAfterShutdown = refreshAttempts;
        await Bun.sleep(100);
        expect(refreshAttempts).toBe(attemptsAfterShutdown);
      } finally {
        store.loadQueueReadModel = originalLoadQueue;
      }
    } finally {
      await Promise.allSettled([
        active.shutdownPostgres(),
        remoteClosed ? Promise.resolve() : remote.shutdownPostgres(),
      ]);
    }
  });

  test.skipIf(!postgresUrl)(
    'does not invalidate a warm broker for events already applied before a manual trim',
    async () => {
      const value = namespace('warm-manual-trim');
      const active = new PostgresQueueManager({
        maxQueueEvents: 100,
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'warm-manual-trim-active',
          pollIntervalMs: 25,
        },
      });
      const stale = new PostgresQueueManager({
        maxQueueEvents: 100,
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'warm-manual-trim-stale',
          pollIntervalMs: 25,
        },
      });
      try {
        await Promise.all([active.waitUntilReady(), stale.waitUntilReady()]);
        const paused = await pauseNotifications(stale);
        const queue = 'warm-manual-trim';
        await active.pushBatch(
          queue,
          Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
        );
        const stream = eventStream(active);
        await stream.drain();

        const store = managerStore(active);
        const originalList = store.list;
        let refreshReads = 0;
        store.list = async (...args) => {
          refreshReads++;
          return await originalList(...args);
        };
        try {
          expect(await active.trimQueueEventsDurable(queue, 1)).toBe(2);
          await stream.drain();
          const inspectable = active as unknown as RefreshInspectableManager;
          await Promise.all(inspectable.refreshes.values());
          expect(refreshReads).toBe(0);

          await paused.drain();
          const staleInspectable = stale as unknown as RefreshInspectableManager;
          await Promise.all(staleInspectable.refreshes.values());
          expect(stale.getJobs(queue)).toHaveLength(3);
        } finally {
          store.list = originalList;
        }
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), stale.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)('hydrates a remote broker DLQ after terminal failure', async () => {
    const value = namespace('remote-dlq');
    const producer = manager(value, 'remote-dlq-producer');
    const consumer = manager(value, 'remote-dlq-consumer');
    try {
      await Promise.all([producer.waitUntilReady(), consumer.waitUntilReady()]);
      const queue = 'remote-dlq';
      const id = await failTerminal(producer, consumer, queue, 0);

      expect(
        await eventually(
          () =>
            producer.getDlqCount(queue) === 1 &&
            producer.getDlq(queue).some((job) => job.id === id) &&
            producer.getDlqEntries(queue).some((entry) => entry.job.id === id) &&
            producer.getDlqStats(queue).total === 1
        )
      ).toBe(true);
      expect((await managerStore(producer).getDlq(queue)).map((entry) => entry.job.id)).toEqual([
        id,
      ]);
    } finally {
      await Promise.allSettled([producer.shutdownPostgres(), consumer.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)(
    'recovers a pruned DLQ invalidation after notifications are missed',
    async () => {
      const value = namespace('missed-notify');
      const active = manager(value, 'missed-notify-active');
      const stale = manager(value, 'missed-notify-stale');
      try {
        await Promise.all([active.waitUntilReady(), stale.waitUntilReady()]);
        const queue = 'missed-notify';
        await active.setDlqConfigDurable(queue, { maxEntries: 1, maxAge: null });
        const evicted = await failTerminal(stale, stale, queue, 0);
        const stream = eventStream(stale);
        expect(
          await eventually(async () => stream.cursor >= (await managerStore(stale).latestEventId()))
        ).toBe(true);
        expect(stale.getDlq(queue).map((job) => job.id)).toEqual([evicted]);

        const paused = await pauseNotifications(stale);
        const retained = await failTerminal(active, active, queue, 1);
        expect((await managerStore(active).getDlq(queue)).map((entry) => entry.job.id)).toEqual([
          retained,
        ]);

        await paused.drain();
        expect(
          await eventually(
            () =>
              stale.getDlqCount(queue) === 1 &&
              stale
                .getDlq(queue)
                .map((job) => job.id)
                .join() === String(retained) &&
              stale
                .getDlqEntries(queue)
                .map((entry) => entry.job.id)
                .join() === String(retained) &&
              stale.getDlqStats(queue).total === 1
          )
        ).toBe(true);
        expect(await stale.getJob(evicted)).toBeNull();
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), stale.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'retains the DLQ invalidation after a terminal child releases its parent',
    async () => {
      const value = namespace('flow-missed-notify');
      const active = manager(value, 'flow-missed-notify-active');
      const stale = manager(value, 'flow-missed-notify-stale');
      try {
        await Promise.all([active.waitUntilReady(), stale.waitUntilReady()]);
        const queue = 'flow-missed-notify';
        await active.setDlqConfigDurable(queue, { maxEntries: 1, maxAge: null });
        const evicted = await failTerminal(stale, stale, queue, 0);
        const stream = eventStream(stale);
        expect(
          await eventually(async () => stream.cursor >= (await managerStore(stale).latestEventId()))
        ).toBe(true);
        expect(stale.getDlq(queue).map((job) => job.id)).toEqual([evicted]);

        const paused = await pauseNotifications(stale);
        const parent = await active.push(queue, { data: { role: 'parent' } });
        const child = await active.push(queue, {
          data: {
            role: 'child',
            __parentId: String(parent.id),
            __parentQueue: queue,
          },
          parentId: parent.id,
          continueParentOnFailure: true,
          maxAttempts: 1,
        });
        await active.updateJobParent(child.id, parent.id);
        const claimed = await active.pullWithLock(queue, 'flow-worker');
        expect(claimed.job?.id).toBe(child.id);
        await active.fail(child.id, 'child-failure', claimed.token ?? undefined);
        expect(await active.getJobState(parent.id)).toBe('waiting');
        expect((await managerStore(active).getDlq(queue)).map((entry) => entry.job.id)).toEqual([
          child.id,
        ]);

        await paused.drain();
        expect(
          await eventually(
            () =>
              stale.getDlqCount(queue) === 1 &&
              stale
                .getDlq(queue)
                .map((job) => job.id)
                .join() === String(child.id) &&
              stale
                .getDlqEntries(queue)
                .map((entry) => entry.job.id)
                .join() === String(child.id) &&
              stale.getDlqStats(queue).total === 1
          )
        ).toBe(true);
        expect(await stale.getJob(evicted)).toBeNull();
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), stale.shutdownPostgres()]);
      }
    }
  );
});
