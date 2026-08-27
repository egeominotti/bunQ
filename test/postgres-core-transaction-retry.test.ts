import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import { planFlows } from '../src/client/flowPlan';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import { cleanupPostgresNamespace, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

interface Settled<T> {
  readonly status: 'fulfilled' | 'rejected';
  readonly value?: T;
  readonly error?: unknown;
}

async function throughCommitLock<T>(
  namespace: string,
  brokerId: string,
  operation: () => Promise<T>
): Promise<Settled<T>> {
  const blocker = new SQL(postgresUrl!, { max: 1 });
  const observer = new SQL(postgresUrl!, { max: 1 });
  const lockName = postgresAdvisoryLockName('event-commit', namespace);
  let lockHeld = false;
  let pending: Promise<Settled<T>> | undefined;
  try {
    await blocker`SELECT pg_advisory_lock(hashtextextended(${lockName}, 0))`;
    lockHeld = true;
    pending = operation().then(
      (value) => ({ status: 'fulfilled', value }) as const,
      (error: unknown) => ({ status: 'rejected', error }) as const
    );
    expect(
      await eventually(async () => {
        const [activity] = await observer<Array<{ waiting: boolean }>>`
          SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE application_name = ${`bunqueue:${brokerId}`}
              AND wait_event_type = 'Lock' AND wait_event = 'advisory'
          ) AS waiting
        `;
        return activity.waiting;
      })
    ).toBe(true);
    await Bun.sleep(150);
    await blocker`SELECT pg_advisory_unlock(hashtextextended(${lockName}, 0))`;
    lockHeld = false;
    return await pending;
  } finally {
    if (lockHeld) {
      await blocker`SELECT pg_advisory_unlock(hashtextextended(${lockName}, 0))`;
    }
    await pending;
    await Promise.allSettled([blocker.close({ timeout: 5 }), observer.close({ timeout: 5 })]);
  }
}

async function durableCount(
  namespace: string,
  table: 'bunqueue_completions' | 'bunqueue_dependencies' | 'bunqueue_events' | 'bunqueue_jobs',
  predicate = ''
): Promise<number> {
  const sql = new SQL(postgresUrl!, { max: 1 });
  try {
    const [row] = await sql.unsafe<Array<{ count: number | string | bigint }>>(
      `SELECT COUNT(*)::bigint AS count FROM ${table} WHERE namespace = $1 ${predicate}`,
      [namespace]
    );
    return Number(row.count);
  } finally {
    await sql.close({ timeout: 5 });
  }
}

test.skipIf(!postgresUrl)('retries a rolled-back single admission exactly once', async () => {
  const namespace = `test-core-retry-push-${Date.now()}-${crypto.randomUUID()}`;
  const brokerId = 'core-retry-push';
  const store = new PostgresQueueStore({
    url: postgresUrl!,
    namespace,
    brokerId,
    lockTimeoutMs: 100,
  });
  try {
    await store.initialize();
    const id = jobId('core-retry-push-job');
    const outcome = await throughCommitLock(namespace, brokerId, () =>
      store.insert(createJob(id, 'core-retry-push', { data: {} }))
    );
    expect(outcome.status).toBe('fulfilled');
    expect(outcome.value).toMatchObject({ inserted: true, job: { id } });
    expect(await durableCount(namespace, 'bunqueue_jobs')).toBe(1);
    expect(await durableCount(namespace, 'bunqueue_events', "AND event_type = 'pushed'")).toBe(1);
    expect((await store.getJob(id))?.job.timeline).toHaveLength(1);
  } finally {
    await store.close();
    await cleanupPostgresNamespace(postgresUrl!, namespace);
  }
});

test.skipIf(!postgresUrl)('retries a rolled-back atomic flow exactly once', async () => {
  const namespace = `test-core-retry-flow-${Date.now()}-${crypto.randomUUID()}`;
  const brokerId = 'core-retry-flow';
  const suffix = crypto.randomUUID();
  const plan = planFlows([
    {
      children: [
        {
          data: { value: 1 },
          name: 'leaf',
          opts: { durable: true, jobId: `${suffix}-leaf` },
          queueName: 'core-retry-flow-leaf',
        },
      ],
      data: { kind: 'root' },
      name: 'root',
      opts: { durable: true, jobId: `${suffix}-root` },
      queueName: 'core-retry-flow-root',
    },
  ]).batch;
  const store = new PostgresQueueStore({
    url: postgresUrl!,
    namespace,
    brokerId,
    lockTimeoutMs: 100,
  });
  try {
    await store.initialize();
    const outcome = await throughCommitLock(namespace, brokerId, () => store.insertFlow(plan));
    expect(outcome.status).toBe('fulfilled');
    expect(outcome.value).toHaveLength(2);
    expect(await durableCount(namespace, 'bunqueue_jobs')).toBe(2);
    expect(await durableCount(namespace, 'bunqueue_dependencies')).toBe(1);
    expect(await durableCount(namespace, 'bunqueue_events', "AND event_type = 'pushed'")).toBe(2);
    const jobs = await Promise.all(plan.jobs.map(({ id }) => store.getJob(id)));
    expect(jobs.every((stored) => stored?.job.timeline.length === 1)).toBe(true);
  } finally {
    await store.close();
    await cleanupPostgresNamespace(postgresUrl!, namespace);
  }
});

test.skipIf(!postgresUrl)('retries a rolled-back batch claim exactly once', async () => {
  const namespace = `test-core-retry-pull-${Date.now()}-${crypto.randomUUID()}`;
  const brokerId = 'core-retry-pull';
  const queue = 'core-retry-pull';
  const store = new PostgresQueueStore({
    url: postgresUrl!,
    namespace,
    brokerId,
    lockTimeoutMs: 100,
  });
  try {
    await store.initialize();
    await store.insert(createJob(jobId('core-retry-pull-job'), queue, { data: {} }));
    const outcome = await throughCommitLock(namespace, brokerId, () =>
      store.claim(queue, 1, 'core-retry-worker', 60_000)
    );
    expect(outcome.status).toBe('fulfilled');
    expect(outcome.value).toHaveLength(1);
    expect(await store.getCounts(queue)).toMatchObject({ active: 1, waiting: 0 });
    expect(await durableCount(namespace, 'bunqueue_events', "AND event_type = 'pulled'")).toBe(1);
  } finally {
    await store.close();
    await cleanupPostgresNamespace(postgresUrl!, namespace);
  }
});

test.skipIf(!postgresUrl)('retries a rolled-back single completion exactly once', async () => {
  const namespace = `test-core-retry-ack-${Date.now()}-${crypto.randomUUID()}`;
  const brokerId = 'core-retry-ack';
  const queue = 'core-retry-ack';
  const id = jobId('core-retry-ack-job');
  const store = new PostgresQueueStore({
    url: postgresUrl!,
    namespace,
    brokerId,
    lockTimeoutMs: 100,
  });
  try {
    await store.initialize();
    await store.insert(createJob(id, queue, { data: {} }));
    const [claimed] = await store.claim(queue, 1, 'core-retry-worker', 60_000);
    const outcome = await throughCommitLock(namespace, brokerId, () =>
      store.complete(id, claimed.token, 'completed')
    );
    expect(outcome.status).toBe('fulfilled');
    expect(outcome.value).toMatchObject({ applied: true, state: 'completed' });
    expect(await durableCount(namespace, 'bunqueue_completions')).toBe(1);
    expect(await durableCount(namespace, 'bunqueue_events', "AND event_type = 'completed'")).toBe(
      1
    );
  } finally {
    await store.close();
    await cleanupPostgresNamespace(postgresUrl!, namespace);
  }
});

test.skipIf(!postgresUrl)('retries a rolled-back terminal failure exactly once', async () => {
  const namespace = `test-core-retry-fail-${Date.now()}-${crypto.randomUUID()}`;
  const brokerId = 'core-retry-fail';
  const queue = 'core-retry-fail';
  const id = jobId('core-retry-fail-job');
  const store = new PostgresQueueStore({
    url: postgresUrl!,
    namespace,
    brokerId,
    lockTimeoutMs: 100,
  });
  try {
    await store.initialize();
    await store.insert(createJob(id, queue, { data: {}, maxAttempts: 1 }));
    const [claimed] = await store.claim(queue, 1, 'core-retry-worker', 60_000);
    const outcome = await throughCommitLock(namespace, brokerId, () =>
      store.fail({ id, token: claimed.token, error: 'terminal', unrecoverable: true })
    );
    expect(outcome.status).toBe('fulfilled');
    expect(outcome.value).toMatchObject({ applied: true, state: 'failed' });
    expect(await store.getCounts(queue)).toMatchObject({ active: 0, failed: 1 });
    expect(await durableCount(namespace, 'bunqueue_events', "AND event_type = 'failed'")).toBe(1);
  } finally {
    await store.close();
    await cleanupPostgresNamespace(postgresUrl!, namespace);
  }
});
