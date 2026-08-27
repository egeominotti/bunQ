import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import { createJob, generateJobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-regression-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string, maxQueueEvents = 10_000): PostgresQueueManager {
  return new PostgresQueueManager({
    maxQueueEvents,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function managerStore(value: PostgresQueueManager): PostgresQueueStore {
  return (value as unknown as { postgresStore: PostgresQueueStore }).postgresStore;
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

async function completeWithResult(
  manager: PostgresQueueManager,
  queue: string,
  result: unknown
): Promise<JobId> {
  const job = await manager.push(queue, { data: {}, customId: `${queue}-job` });
  const claimed = await manager.pullWithLock(queue, 'result-worker');
  await manager.ack(job.id, result, claimed.token ?? undefined);
  return job.id;
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

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(postgresUrl, value);
});

describe('PostgreSQL concurrency and snapshot regressions', () => {
  test.skipIf(!postgresUrl)('locks queue state before jobs while obliterating', async () => {
    const value = namespace('lock-order');
    const queue = 'lock-order';
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace: value,
      brokerId: 'lock-order-broker',
    });
    const blockerSql = new SQL(postgresUrl!, { max: 1 });
    const observerSql = new SQL(postgresUrl!, { max: 2 });
    let releaseBlocker!: () => void;
    const released = new Promise<void>((resolve) => (releaseBlocker = resolve));
    let reportBlocker!: (pid: number) => void;
    const blockerReady = new Promise<number>((resolve) => (reportBlocker = resolve));
    let obliterate: Promise<number> | null = null;
    try {
      await store.initialize();
      await store.setConcurrency(queue, 1);
      await store.insert(createJob(generateJobId(), queue, { data: {} }));
      const blocker = blockerSql.begin(async (tx) => {
        const [backend] = await tx<{ pid: number }[]>`SELECT pg_backend_pid() AS pid`;
        await tx`
          SELECT queue FROM bunqueue_queue_state
          WHERE namespace = ${value} AND queue = ${queue}
          FOR UPDATE
        `;
        reportBlocker(backend.pid);
        await released;
      });
      const blockerPid = await blockerReady;
      obliterate = store.obliterate(queue);
      const waiting = await eventually(async () => {
        const [row] = await observerSql<{ blocked: boolean }[]>`
          SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE ${blockerPid} = ANY(pg_blocking_pids(pid))
          ) AS blocked
        `;
        return row.blocked;
      });
      let probeError: unknown = null;
      try {
        await observerSql.begin(async (tx) => {
          await tx`
            SELECT id FROM bunqueue_jobs
            WHERE namespace = ${value} AND queue = ${queue}
            FOR UPDATE NOWAIT
          `;
        });
      } catch (error) {
        probeError = error;
      }
      releaseBlocker();
      await blocker;
      await obliterate;
      expect(waiting).toBe(true);
      expect(probeError).toBeNull();
    } finally {
      releaseBlocker();
      await Promise.allSettled([
        obliterate ?? Promise.resolve(),
        store.close(),
        blockerSql.close({ timeout: 5 }),
        observerSql.close({ timeout: 5 }),
      ]);
    }
  });

  test.skipIf(!postgresUrl)('clears cached completion results when retrying', async () => {
    const value = namespace('retry-result');
    const broker = manager(value, 'retry-result-broker');
    try {
      await broker.waitUntilReady();
      const id = await completeWithResult(broker, 'retry-result', { generation: 1 });
      expect(broker.getResult(id)).toEqual({ generation: 1 });
      expect(await broker.retryCompletedDurable('retry-result', id)).toBe(1);
      expect(await broker.getJobState(id)).toBe('waiting');
      expect(broker.getResult(id)).toBeUndefined();
    } finally {
      await broker.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)('clears results after clean and custom-ID reuse', async () => {
    const value = namespace('clean-result');
    const broker = manager(value, 'clean-result-broker');
    try {
      await broker.waitUntilReady();
      const queue = 'clean-result';
      const id = await completeWithResult(broker, queue, { generation: 1 });
      await Bun.sleep(2);
      expect(await broker.cleanDurable(queue, 0, 'completed')).toEqual([id]);
      const reused = await broker.push(queue, {
        data: { generation: 2 },
        customId: `${queue}-job`,
      });
      expect(reused.id).toBe(id);
      expect(broker.getResult(id)).toBeUndefined();
    } finally {
      await broker.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)('broadcasts bounded DLQ evictions to every broker', async () => {
    const value = namespace('dlq-limit');
    const first = manager(value, 'dlq-limit-a');
    const second = manager(value, 'dlq-limit-b');
    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const queue = 'bounded';
      await first.setDlqConfigDurable(queue, { maxEntries: 2, maxAge: null });
      expect(await eventually(() => second.getDlqConfig(queue).maxEntries === 2)).toBe(true);
      const ids: JobId[] = [];
      for (let index = 0; index < 3; index++) {
        ids.push(await failTerminal(first, second, queue, index));
        await Bun.sleep(2);
      }
      expect(await managerStore(first).getDlq(queue)).toHaveLength(2);
      expect(
        await eventually(
          () =>
            first.getDlqCount(queue) === 2 &&
            second.getDlqCount(queue) === 2 &&
            !first.getDlq(queue).some((job) => job.id === ids[0]) &&
            !second.getDlq(queue).some((job) => job.id === ids[0])
        )
      ).toBe(true);
      expect(
        first
          .getDlq(queue)
          .map((job) => job.id)
          .sort()
      ).toEqual(ids.slice(1).sort());
      expect(
        second
          .getDlqEntries(queue)
          .map((entry) => entry.job.id)
          .sort()
      ).toEqual(ids.slice(1).sort());
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)('retains DLQ invalidations with a one-event window', async () => {
    const value = namespace('dlq-event-window');
    const first = manager(value, 'dlq-window-a', 1);
    const second = manager(value, 'dlq-window-b', 1);
    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const queue = 'one-event';
      await first.setDlqConfigDurable(queue, { maxEntries: 1, maxAge: null });
      expect(await eventually(() => second.getDlqConfig(queue).maxEntries === 1)).toBe(true);
      const oldest = await failTerminal(first, second, queue, 0);
      await Bun.sleep(2);
      const retained = await failTerminal(first, second, queue, 1);
      expect(await managerStore(first).getDlq(queue)).toHaveLength(1);
      expect(
        await eventually(
          () =>
            first.getDlqCount(queue) === 1 &&
            second.getDlqCount(queue) === 1 &&
            !first.getDlq(queue).some((job) => job.id === oldest) &&
            !second.getDlq(queue).some((job) => job.id === oldest)
        )
      ).toBe(true);
      expect(first.getDlq(queue).map((job) => job.id)).toEqual([retained]);
      expect(second.getDlqEntries(queue).map((entry) => entry.job.id)).toEqual([retained]);
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)('broadcasts expired DLQ removals to every broker', async () => {
    const value = namespace('dlq-expiry');
    const first = manager(value, 'dlq-expiry-a');
    const second = manager(value, 'dlq-expiry-b');
    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const queue = 'expiring';
      await first.setDlqConfigDurable(queue, { maxAge: 1, maxEntries: 10 });
      expect(await eventually(() => second.getDlqConfig(queue).maxAge === 1)).toBe(true);
      const job = await first.push(queue, { data: { index: 0 }, maxAttempts: 1 });
      const claimed = await second.pullWithLock(queue, 'worker-clock-skew');
      const systemNow = Date.now;
      try {
        Date.now = () => systemNow() + 60_000;
        await second.fail(job.id, 'clock-skew-failure', claimed.token ?? undefined);
      } finally {
        Date.now = systemNow;
      }
      const id = job.id;
      expect(
        await eventually(() => first.getDlqCount(queue) === 1 && second.getDlqCount(queue) === 1)
      ).toBe(true);
      await Bun.sleep(5);
      expect((await managerStore(first).maintainDlq()).purged).toBe(1);
      expect(await managerStore(first).getJob(id)).toBeNull();
      expect(
        await eventually(() => first.getDlqCount(queue) === 0 && second.getDlqCount(queue) === 0)
      ).toBe(true);
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });
});
