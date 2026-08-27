import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import { isRetryablePostgresTransactionError } from '../src/infrastructure/persistence/postgres/transactionRetry';
import { cleanupPostgresNamespace, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

test('classifies only rollback-certain PostgreSQL transaction failures as retryable', () => {
  for (const errno of ['40001', '40P01', '55P03']) {
    expect(isRetryablePostgresTransactionError({ code: 'ERR_POSTGRES_SERVER_ERROR', errno })).toBe(
      true
    );
  }
  for (const error of [
    { code: 'ERR_POSTGRES_CONNECTION_CLOSED' },
    { errno: '23505' },
    { errno: '57014' },
    new Error('canceling statement due to lock timeout'),
  ]) {
    expect(isRetryablePostgresTransactionError(error)).toBe(false);
  }
});

test.skipIf(!postgresUrl)(
  'retries a batch completion after the event commit sequencer lock times out',
  async () => {
    const namespace = `test-ackb-lock-timeout-${Date.now()}-${crypto.randomUUID()}`;
    const commitLock = postgresAdvisoryLockName('event-commit', namespace);
    const queue = 'ackb-lock-timeout';
    const brokerId = 'ackb-lock-timeout';
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace,
      brokerId,
      lockTimeoutMs: 100,
      poolSize: 2,
    });
    const blocker = new SQL(postgresUrl!, { max: 1 });
    const observer = new SQL(postgresUrl!, { max: 1 });
    let completion:
      | Promise<
          | {
              readonly status: 'fulfilled';
              readonly value: Awaited<ReturnType<typeof store.completeMany>>;
            }
          | { readonly status: 'rejected'; readonly error: unknown }
        >
      | undefined;
    let lockHeld = false;
    try {
      await store.initialize();
      const id = jobId('ackb-lock-timeout-job');
      await store.insert(createJob(id, queue, { data: { attempt: 1 } }));
      const [claimed] = await store.claim(queue, 1, 'ackb-lock-timeout-worker', 60_000);
      expect(claimed.job.id).toBe(id);

      await blocker`SELECT pg_advisory_lock(hashtextextended(${commitLock}, 0))`;
      lockHeld = true;
      completion = store
        .completeMany([{ id, token: claimed.token, result: { accepted: true } }])
        .then(
          (value) => ({ status: 'fulfilled', value }) as const,
          (error: unknown) => ({ status: 'rejected', error }) as const
        );

      expect(
        await eventually(async () => {
          const [activity] = await observer<Array<{ waiting: boolean }>>`
            SELECT EXISTS (
              SELECT 1 FROM pg_stat_activity
              WHERE application_name = ${`bunqueue:${brokerId}`}
                AND wait_event_type = 'Lock'
                AND wait_event = 'advisory'
            ) AS waiting
          `;
          return activity.waiting;
        })
      ).toBe(true);

      await Bun.sleep(175);
      const [released] = await blocker<Array<{ unlocked: boolean }>>`
        SELECT pg_advisory_unlock(hashtextextended(${commitLock}, 0)) AS unlocked
      `;
      lockHeld = false;
      expect(released.unlocked).toBe(true);

      const outcome = await completion;
      expect(outcome.status).toBe('fulfilled');
      if (outcome.status !== 'fulfilled') throw outcome.error;
      expect(outcome.value).toMatchObject([{ applied: true, state: 'completed' }]);
      expect(await store.getCounts(queue)).toMatchObject({ active: 0, completed: 1, waiting: 0 });
      expect(await store.getResult(id)).toEqual({ found: true, result: { accepted: true } });

      const [durable] = await observer<
        Array<{
          buckets: number | string | bigint;
          completions: number | string | bigint;
          events: number | string | bigint;
          jobs: number | string | bigint;
          total: number | string | bigint;
        }>
      >`
        SELECT
          (SELECT COUNT(*) FROM bunqueue_jobs
           WHERE namespace = ${namespace} AND id = ${String(id)} AND state = 'completed') AS jobs,
          (SELECT COUNT(*) FROM bunqueue_completions
           WHERE namespace = ${namespace} AND job_id = ${String(id)}) AS completions,
          (SELECT COUNT(*) FROM bunqueue_events
           WHERE namespace = ${namespace} AND job_id = ${String(id)}
             AND event_type = 'completed') AS events,
          (SELECT COALESCE(SUM(total_count), 0) FROM bunqueue_metric_totals
           WHERE namespace = ${namespace} AND queue = ${queue}
             AND metric_type = 'completed') AS total,
          (SELECT COALESCE(SUM(count), 0) FROM bunqueue_metric_buckets
           WHERE namespace = ${namespace} AND queue = ${queue}
             AND metric_type = 'completed') AS buckets
      `;
      expect(
        Object.fromEntries(Object.entries(durable).map(([key, value]) => [key, Number(value)]))
      ).toEqual({ buckets: 1, completions: 1, events: 1, jobs: 1, total: 1 });
    } finally {
      if (lockHeld) {
        await blocker`SELECT pg_advisory_unlock(hashtextextended(${commitLock}, 0))`;
      }
      await completion;
      await Promise.allSettled([
        store.close(),
        blocker.close({ timeout: 5 }),
        observer.close({ timeout: 5 }),
      ]);
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  },
  15_000
);

test.skipIf(!postgresUrl)(
  'bounds batch-completion retries and leaves no partial durable transition',
  async () => {
    const namespace = `test-ackb-retry-bound-${Date.now()}-${crypto.randomUUID()}`;
    const commitLock = postgresAdvisoryLockName('event-commit', namespace);
    const queue = 'ackb-retry-bound';
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace,
      brokerId: 'ackb-retry-bound',
      lockTimeoutMs: 75,
      poolSize: 2,
    });
    const blocker = new SQL(postgresUrl!, { max: 1 });
    const observer = new SQL(postgresUrl!, { max: 1 });
    let lockHeld = false;
    try {
      await store.initialize();
      const id = jobId('ackb-retry-bound-job');
      await store.insert(createJob(id, queue, { data: {} }));
      const [claimed] = await store.claim(queue, 1, 'ackb-retry-bound-worker', 60_000);
      await blocker`SELECT pg_advisory_lock(hashtextextended(${commitLock}, 0))`;
      lockHeld = true;

      const startedAt = performance.now();
      const outcome = await store
        .completeMany([{ id, token: claimed.token, result: 'must-rollback' }])
        .then(
          (value) => ({ status: 'fulfilled', value }) as const,
          (error: unknown) => ({ status: 'rejected', error }) as const
        );
      const elapsedMs = performance.now() - startedAt;
      expect(outcome.status).toBe('rejected');
      if (outcome.status !== 'rejected') throw new Error('the held sequencer lock did not fail');
      expect((outcome.error as Record<string, unknown>)['errno']).toBe('55P03');
      expect(elapsedMs).toBeGreaterThanOrEqual(140);
      expect(elapsedMs).toBeLessThan(2_000);

      const [durable] = await observer<
        Array<{
          completedEvents: number | string | bigint;
          completions: number | string | bigint;
          metrics: number | string | bigint;
          state: string;
        }>
      >`
        SELECT
          (SELECT state FROM bunqueue_jobs
           WHERE namespace = ${namespace} AND id = ${String(id)}) AS state,
          (SELECT COUNT(*) FROM bunqueue_completions
           WHERE namespace = ${namespace} AND job_id = ${String(id)}) AS completions,
          (SELECT COUNT(*) FROM bunqueue_events
           WHERE namespace = ${namespace} AND job_id = ${String(id)}
             AND event_type = 'completed') AS "completedEvents",
          (SELECT COALESCE(SUM(total_count), 0) FROM bunqueue_metric_totals
           WHERE namespace = ${namespace} AND queue = ${queue}
             AND metric_type = 'completed') AS metrics
      `;
      expect({
        completedEvents: Number(durable.completedEvents),
        completions: Number(durable.completions),
        metrics: Number(durable.metrics),
        state: durable.state,
      }).toEqual({ completedEvents: 0, completions: 0, metrics: 0, state: 'active' });
    } finally {
      if (lockHeld) {
        await blocker`SELECT pg_advisory_unlock(hashtextextended(${commitLock}, 0))`;
      }
      await Promise.allSettled([
        store.close(),
        blocker.close({ timeout: 5 }),
        observer.close({ timeout: 5 }),
      ]);
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  },
  15_000
);
