import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { maintainPostgresDlq } from '../src/infrastructure/persistence/postgres/dlqMaintenance';
import {
  cleanupPostgresNamespace,
  eventually,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-dlq-generation-race-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function identifier(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().replaceAll('-', '')}`;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL DLQ dependency generation races', () => {
  test.skipIf(!postgresUrl)(
    'never retries a consumer from stale completion evidence after dependency ID reuse',
    async () => {
      const value = namespace();
      const dependencyQueue = 'dependencies';
      const consumerQueue = 'consumers';
      const dependencyId = 'shared-dependency';
      const consumerId = 'consumer';
      const first = manager(value, 'dlq-generation-a');
      const second = manager(value, 'dlq-generation-b');
      const peers = Array.from(
        { length: 3 },
        (_, index) =>
          new PostgresQueueStore({
            url: postgresUrl!,
            namespace: value,
            brokerId: `dlq-generation-maintenance-${index}`,
          })
      );
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const observer = new SQL(postgresUrl!, { max: 1 });
      const functionName = identifier('pause_dlq_retry');
      const triggerName = identifier('pause_dlq_retry');
      const lockKey = crypto.getRandomValues(new Uint32Array(1))[0] & 0x7fffffff;
      let lockHeld = false;
      let functionCreated = false;
      let triggerCreated = false;
      let maintenance: Array<ReturnType<typeof maintainPostgresDlq>> = [];
      let admission: Promise<unknown> | null = null;

      try {
        await Promise.all([
          first.waitUntilReady(),
          second.waitUntilReady(),
          ...peers.map((peer) => peer.initialize()),
        ]);
        await first.setDlqConfigDurable(consumerQueue, {
          autoRetry: true,
          autoRetryInterval: 1,
          maxAutoRetries: 2,
          maxAge: null,
        });

        const dependency = await first.push(dependencyQueue, {
          customId: dependencyId,
          data: { generation: 1 },
          removeOnComplete: true,
        });
        const dependencyClaim = await first.pullWithLock(dependencyQueue, 'dependency-worker');
        expect(dependencyClaim.job?.id).toBe(dependency.id);
        await first.ack(dependency.id, undefined, dependencyClaim.token!);
        expect(await first.getJobState(dependency.id)).toBe('unknown');

        const consumer = await first.push(consumerQueue, {
          customId: consumerId,
          data: {},
          dependsOn: [dependency.id],
          maxAttempts: 1,
        });
        const consumerClaim = await first.pullWithLock(consumerQueue, 'consumer-worker');
        expect(consumerClaim.job?.id).toBe(consumer.id);
        await first.fail(consumer.id, 'terminal consumer failure', consumerClaim.token!, true);
        expect(await first.getJobState(consumer.id)).toBe('failed');
        await Bun.sleep(5);

        await observer
          .unsafe(
            `CREATE FUNCTION ${functionName}() RETURNS trigger AS $$ BEGIN ` +
              `IF OLD.namespace = '${value}' AND OLD.id = '${consumerId}' ` +
              `AND OLD.state = 'failed' AND NEW.state <> 'failed' THEN ` +
              `PERFORM pg_advisory_xact_lock(${lockKey}); END IF; ` +
              'RETURN NEW; END $$ LANGUAGE plpgsql'
          )
          .simple();
        functionCreated = true;
        await observer
          .unsafe(
            `CREATE TRIGGER ${triggerName} BEFORE UPDATE ON bunqueue_jobs ` +
              `FOR EACH ROW EXECUTE FUNCTION ${functionName}()`
          )
          .simple();
        triggerCreated = true;

        const [backend] = await blocker<{ pid: number }[]>`SELECT pg_backend_pid() AS pid`;
        await blocker`SELECT pg_advisory_lock(${lockKey})`;
        lockHeld = true;
        maintenance = [postgresManagerStore(first), ...peers].map((store) =>
          maintainPostgresDlq(store.context)
        );
        expect(
          await eventually(async () => {
            const [row] = await observer<{ waiting: boolean }[]>`
              SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE ${backend.pid} = ANY(pg_blocking_pids(pid))
              ) AS waiting
            `;
            return row.waiting;
          })
        ).toBe(true);

        admission = second.push(dependencyQueue, {
          customId: dependencyId,
          data: { generation: 2 },
        });
        const admissionOutcome = admission.then(
          (value) => ({ status: 'fulfilled' as const, value }),
          (error: unknown) => ({ status: 'rejected' as const, error })
        );
        await Promise.race([
          admission.then(
            () => undefined,
            () => undefined
          ),
          Bun.sleep(500),
        ]);
        await blocker`SELECT pg_advisory_unlock(${lockKey})`;
        lockHeld = false;
        const [maintenanceResults, admitted] = await Promise.all([
          Promise.all(maintenance),
          admissionOutcome,
        ]);
        expect(maintenanceResults.reduce((sum, result) => sum + result.retried, 0)).toBe(1);
        expect(admitted.status).toBe('rejected');

        const sql = postgresManagerStore(first).context.sql;
        const [states] = await sql<
          Array<{
            consumer_state: string | null;
            dependency_state: string | null;
            completion_rows: number;
            retry_events: number;
          }>
        >`
          SELECT
            (SELECT state FROM bunqueue_jobs
             WHERE namespace = ${value} AND id = ${consumerId}) AS consumer_state,
            (SELECT state FROM bunqueue_jobs
             WHERE namespace = ${value} AND id = ${dependencyId}) AS dependency_state,
            (SELECT COUNT(*)::int FROM bunqueue_completions
             WHERE namespace = ${value} AND job_id = ${dependencyId}) AS completion_rows,
            (SELECT COUNT(*)::int FROM bunqueue_events
             WHERE namespace = ${value} AND job_id = ${consumerId}
               AND event_type = 'retried') AS retry_events
        `;
        expect(states).toEqual({
          consumer_state: 'waiting',
          dependency_state: null,
          completion_rows: 1,
          retry_events: 1,
        });
      } finally {
        if (lockHeld) await blocker`SELECT pg_advisory_unlock(${lockKey})`.catch(() => []);
        await Promise.allSettled([
          ...maintenance,
          ...([admission].filter(Boolean) as Promise<unknown>[]),
        ]);
        if (triggerCreated) {
          await observer
            .unsafe(`DROP TRIGGER IF EXISTS ${triggerName} ON bunqueue_jobs`)
            .simple()
            .catch(() => undefined);
        }
        if (functionCreated) {
          await observer
            .unsafe(`DROP FUNCTION IF EXISTS ${functionName}()`)
            .simple()
            .catch(() => undefined);
        }
        await Promise.allSettled([
          first.shutdownPostgres(),
          second.shutdownPostgres(),
          ...peers.map((peer) => peer.close()),
          blocker.close({ timeout: 5 }),
          observer.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'parks the consumer when dependency ID reuse commits before DLQ maintenance',
    async () => {
      const value = namespace();
      const first = manager(value, 'inverse-a');
      const second = manager(value, 'inverse-b');
      const dependencyId = 'inverse-dependency';
      const consumerId = 'inverse-consumer';
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await first.setDlqConfigDurable('inverse-consumers', {
          autoRetry: true,
          autoRetryInterval: 1,
          maxAutoRetries: 2,
          maxAge: null,
        });
        const dependency = await first.push('inverse-dependencies', {
          customId: dependencyId,
          data: { generation: 1 },
          removeOnComplete: true,
        });
        const dependencyClaim = await first.pullWithLock('inverse-dependencies', 'inverse-worker');
        await first.ack(dependency.id, undefined, dependencyClaim.token!);
        const consumer = await first.push('inverse-consumers', {
          customId: consumerId,
          data: {},
          dependsOn: [dependency.id],
          maxAttempts: 1,
        });
        const consumerClaim = await first.pullWithLock('inverse-consumers', 'consumer-worker');
        await first.fail(consumer.id, 'terminal', consumerClaim.token!, true);
        await Bun.sleep(5);

        await second.push('inverse-dependencies', {
          customId: dependencyId,
          data: { generation: 2 },
        });
        expect(await maintainPostgresDlq(postgresManagerStore(first).context)).toMatchObject({
          retried: 1,
        });

        const [states] = await postgresManagerStore(first).context.sql<
          Array<{ consumer_state: string; dependency_state: string; completion_rows: number }>
        >`
          SELECT
            (SELECT state FROM bunqueue_jobs
             WHERE namespace = ${value} AND id = ${consumerId}) AS consumer_state,
            (SELECT state FROM bunqueue_jobs
             WHERE namespace = ${value} AND id = ${dependencyId}) AS dependency_state,
            (SELECT COUNT(*)::int FROM bunqueue_completions
             WHERE namespace = ${value} AND job_id = ${dependencyId}) AS completion_rows
        `;
        expect(states).toEqual({
          consumer_state: 'waiting-children',
          dependency_state: 'waiting',
          completion_rows: 0,
        });
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );
});
