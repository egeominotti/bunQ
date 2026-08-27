import { afterAll, describe, expect, test } from 'bun:test';
import { createJob, jobId, type JobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { PostgresPostCommitMaintenance } from '../src/infrastructure/persistence/postgres/postCommitMaintenance';
import { cleanupPostgresNamespace, deferred, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-postcommit-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function store(value: string, label: string, maxCompletedJobs = 1000): PostgresQueueStore {
  return new PostgresQueueStore({
    url: postgresUrl!,
    namespace: value,
    brokerId: label,
    maxCompletedJobs,
  });
}

async function claimJob(
  queueStore: PostgresQueueStore,
  queue: string,
  id: string,
  options: { removeOnComplete?: boolean } = {}
) {
  const job = createJob(jobId(id), queue, { data: {}, ...options });
  await queueStore.insert(job);
  const [claim] = await queueStore.claim(queue, 1, `${id}-worker`, 60_000);
  expect(claim.job.id).toBe(job.id);
  return claim;
}

async function installDeleteFailure(
  queueStore: PostgresQueueStore,
  value: string,
  table: 'bunqueue_completions' | 'bunqueue_jobs'
): Promise<() => Promise<void>> {
  const suffix = crypto.randomUUID().replaceAll('-', '');
  const functionName = `bunqueue_test_fail_delete_${suffix}`;
  const triggerName = `bunqueue_test_fail_delete_${suffix}`;
  const namespaceLiteral = value.replaceAll("'", "''");
  await queueStore.context.sql.unsafe(`
    CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF OLD.namespace = '${namespaceLiteral}' THEN
        RAISE EXCEPTION 'injected post-commit maintenance failure';
      END IF;
      RETURN OLD;
    END
    $$
  `);
  await queueStore.context.sql.unsafe(`
    CREATE TRIGGER ${triggerName}
    BEFORE DELETE ON ${table}
    FOR EACH ROW EXECUTE FUNCTION ${functionName}()
  `);
  return async () => {
    await queueStore.context.sql.unsafe(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
    await queueStore.context.sql.unsafe(`DROP FUNCTION IF EXISTS ${functionName}()`);
  };
}

async function configureBoundedDlq(queueStore: PostgresQueueStore, queue: string): Promise<void> {
  const state = await queueStore.getQueueState(queue);
  await queueStore.setDlqConfig(queue, { ...state.dlqConfig, maxEntries: 1 });
}

async function failTerminal(
  queueStore: PostgresQueueStore,
  queue: string,
  id: string
): Promise<JobId> {
  const claim = await claimJob(queueStore, queue, id);
  const transition = await queueStore.fail({
    id: claim.job.id,
    token: claim.token,
    error: `${id}-failure`,
    unrecoverable: true,
  });
  expect(transition.applied).toBe(true);
  return claim.job.id;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL post-commit maintenance regressions', () => {
  test('serializes repeated work for the same subsystem', async () => {
    const maintenance = new PostgresPostCommitMaintenance(() => undefined, 10_000);
    const firstEntered = deferred<undefined>();
    const releaseFirst = deferred<undefined>();
    let active = 0;
    let maximumActive = 0;
    const operation = async () => {
      active++;
      maximumActive = Math.max(maximumActive, active);
      if (maximumActive === 1) {
        firstEntered.resolve(undefined);
        await releaseFirst.promise;
      }
      active--;
    };

    const first = maintenance.run('retention', operation);
    await firstEntered.promise;
    const second = maintenance.run('retention', operation);
    await Bun.sleep(20);
    expect(maximumActive).toBe(1);

    releaseFirst.resolve(undefined);
    await Promise.all([first, second]);
    maintenance.close();
    expect(maximumActive).toBe(1);
  });

  test('does not let an older failed generation overwrite newer maintenance success', async () => {
    const reports: Array<{ subsystem: string; error: unknown }> = [];
    const maintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => reports.push({ subsystem, error }),
      10_000
    );
    const oldAttempt = deferred<undefined>();
    const first = maintenance.run('retention', () => oldAttempt.promise);
    const second = maintenance.run('retention', async () => undefined);
    oldAttempt.reject(new Error('stale failure'));
    await Promise.all([first, second]);
    maintenance.close();

    expect(reports).toEqual([{ subsystem: 'retention', error: null }]);
  });

  test('does not let an older success discard a reused-generation failure', async () => {
    const reports: Array<{ subsystem: string; error: unknown }> = [];
    const maintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => reports.push({ subsystem, error }),
      10_000
    );
    const oldAttempt = deferred<undefined>();
    const currentAttempt = deferred<undefined>();
    const old = maintenance.run('completion-retention', () => oldAttempt.promise);
    const superseded = maintenance.run('completion-retention', async () => undefined);
    const current = maintenance.run('completion-retention', () => currentAttempt.promise);

    oldAttempt.resolve(undefined);
    await old;
    const currentError = new Error('current maintenance failure');
    currentAttempt.reject(currentError);
    await Promise.all([superseded, current]);
    maintenance.close();

    expect(reports).toEqual([{ subsystem: 'completion-retention', error: currentError }]);
  });

  test.skipIf(!postgresUrl)(
    'does not reject a committed remove-on-complete ACK when tombstone pruning fails',
    async () => {
      const value = namespace('complete');
      const queueStore = store(value, 'complete', 0);
      let removeFailure: (() => Promise<void>) | null = null;
      try {
        await queueStore.initialize();
        const seed = await claimJob(queueStore, 'complete', 'complete-seed', {
          removeOnComplete: true,
        });
        expect((await queueStore.complete(seed.job.id, seed.token, 'seed', true)).applied).toBe(
          true
        );
        const claim = await claimJob(queueStore, 'complete', 'complete-job', {
          removeOnComplete: true,
        });
        removeFailure = await installDeleteFailure(queueStore, value, 'bunqueue_completions');

        const transition = await queueStore.complete(
          claim.job.id,
          claim.token,
          { committed: true },
          true
        );

        expect(transition.applied).toBe(true);
        expect(await queueStore.getResult(claim.job.id)).toEqual({
          found: true,
          result: { committed: true },
        });
        expect(queueStore.health()).toMatchObject({
          ok: false,
          error: expect.stringContaining('injected post-commit maintenance failure'),
        });

        await removeFailure();
        removeFailure = null;
        expect(await eventually(() => queueStore.health().ok)).toBe(true);
        const [retained] = await queueStore.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count FROM bunqueue_completions WHERE namespace = ${value}
        `;
        expect(retained.count).toBe(1);
      } finally {
        await Promise.allSettled([removeFailure?.(), queueStore.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not reject a committed batch ACK when tombstone pruning fails',
    async () => {
      const value = namespace('complete-many');
      const queueStore = store(value, 'complete-many', 0);
      let removeFailure: (() => Promise<void>) | null = null;
      try {
        await queueStore.initialize();
        const first = await claimJob(queueStore, 'complete-many', 'batch-first', {
          removeOnComplete: true,
        });
        const second = await claimJob(queueStore, 'complete-many', 'batch-second', {
          removeOnComplete: true,
        });
        removeFailure = await installDeleteFailure(queueStore, value, 'bunqueue_completions');

        const transitions = await queueStore.completeMany([
          { id: first.job.id, token: first.token, result: 1, removeOnComplete: true },
          { id: second.job.id, token: second.token, result: 2, removeOnComplete: true },
        ]);

        expect(transitions.map(({ applied }) => applied)).toEqual([true, true]);
        expect(await queueStore.getResult(first.job.id)).toEqual({ found: true, result: 1 });
        expect(await queueStore.getResult(second.job.id)).toEqual({ found: true, result: 2 });
        expect(queueStore.health().ok).toBe(false);
        await removeFailure();
        removeFailure = null;
        expect(await eventually(() => queueStore.health().ok)).toBe(true);
        const [retained] = await queueStore.context.sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count FROM bunqueue_completions WHERE namespace = ${value}
        `;
        expect(retained.count).toBe(1);
      } finally {
        await Promise.allSettled([removeFailure?.(), queueStore.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not reject a committed terminal failure when DLQ pruning fails',
    async () => {
      const value = namespace('fail');
      const queueStore = store(value, 'fail');
      let removeFailure: (() => Promise<void>) | null = null;
      try {
        await queueStore.initialize();
        await configureBoundedDlq(queueStore, 'fail');
        await failTerminal(queueStore, 'fail', 'old-failure');
        const current = await claimJob(queueStore, 'fail', 'current-failure');
        removeFailure = await installDeleteFailure(queueStore, value, 'bunqueue_jobs');

        const transition = await queueStore.fail({
          id: current.job.id,
          token: current.token,
          error: 'current failure',
          unrecoverable: true,
        });

        expect(transition.applied).toBe(true);
        expect((await queueStore.getJob(current.job.id))?.state).toBe('failed');
        expect(queueStore.health().ok).toBe(false);
        await removeFailure();
        removeFailure = null;
        expect(await eventually(() => queueStore.health().ok)).toBe(true);
        expect(await queueStore.getDlq('fail')).toHaveLength(1);
      } finally {
        await Promise.allSettled([removeFailure?.(), queueStore.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not reject a committed discard when DLQ pruning fails',
    async () => {
      const value = namespace('discard');
      const queueStore = store(value, 'discard');
      let removeFailure: (() => Promise<void>) | null = null;
      try {
        await queueStore.initialize();
        await configureBoundedDlq(queueStore, 'discard');
        await failTerminal(queueStore, 'discard', 'old-discard');
        const current = createJob(jobId('current-discard'), 'discard', { data: {} });
        await queueStore.insert(current);
        removeFailure = await installDeleteFailure(queueStore, value, 'bunqueue_jobs');

        expect(await queueStore.discard(current.id)).toBe(true);
        expect((await queueStore.getJob(current.id))?.state).toBe('failed');
        expect(queueStore.health().ok).toBe(false);
        await removeFailure();
        removeFailure = null;
        expect(await eventually(() => queueStore.health().ok)).toBe(true);
        expect(await queueStore.getDlq('discard')).toHaveLength(1);
      } finally {
        await Promise.allSettled([removeFailure?.(), queueStore.close()]);
      }
    }
  );
});
