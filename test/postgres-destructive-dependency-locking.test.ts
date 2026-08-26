import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { cleanupPostgresNamespace, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

type Outcome<T> =
  | { readonly status: 'fulfilled'; readonly value: T }
  | { readonly status: 'rejected'; readonly error: unknown };

function namespace(label: string): string {
  const value = `test-destructive-lock-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function outcome<T>(promise: Promise<T>): Promise<Outcome<T>> {
  return promise.then(
    (value) => ({ status: 'fulfilled', value }),
    (error: unknown) => ({ status: 'rejected', error })
  );
}

function identifier(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().replaceAll('-', '')}`;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL destructive dependency lock domain', () => {
  test.skipIf(!postgresUrl)(
    'serializes drain after the final transactional dependency check',
    async () => {
      const value = namespace('drain');
      const first = manager(value, 'drain-a');
      const second = manager(value, 'drain-b');
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const observer = new SQL(postgresUrl!, { max: 1 });
      const functionName = identifier('pause_dependency_insert');
      const triggerName = identifier('pause_dependency_insert');
      const lockKey = crypto.getRandomValues(new Uint32Array(1))[0] & 0x7fffffff;
      let lockHeld = false;
      let functionCreated = false;
      let triggerCreated = false;
      let admission: Promise<Outcome<unknown>> | null = null;
      let drain: Promise<Outcome<number>> | null = null;
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const parent = await first.push('parents', { data: { parent: true } });
        await observer
          .unsafe(
            `CREATE FUNCTION ${functionName}() RETURNS trigger AS $$ BEGIN ` +
              `IF NEW.namespace = '${value}' THEN ` +
              `PERFORM pg_advisory_xact_lock(${lockKey}); END IF; ` +
              'RETURN NEW; END $$ LANGUAGE plpgsql'
          )
          .simple();
        functionCreated = true;
        await observer
          .unsafe(
            `CREATE TRIGGER ${triggerName} BEFORE INSERT ON bunqueue_jobs ` +
              `FOR EACH ROW EXECUTE FUNCTION ${functionName}()`
          )
          .simple();
        triggerCreated = true;

        const [backend] = await blocker<{ pid: number }[]>`SELECT pg_backend_pid() AS pid`;
        await blocker`SELECT pg_advisory_lock(${lockKey})`;
        lockHeld = true;
        admission = outcome(
          second.push('consumers', {
            customId: `child-${crypto.randomUUID()}`,
            data: { child: true },
            dependsOn: [parent.id],
          })
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

        drain = outcome(first.drainDurable('parents'));
        let drainSettled = false;
        void drain.then(() => (drainSettled = true));
        await Bun.sleep(100);
        expect(drainSettled).toBe(false);

        await blocker`SELECT pg_advisory_unlock(${lockKey})`;
        lockHeld = false;
        const admissionResult = await admission;
        expect(admissionResult.status).toBe('fulfilled');
        const drainResult = await drain;
        const parentState = await first.getJobState(parent.id);
        const child =
          admissionResult.status === 'fulfilled' ? (admissionResult.value as { id: string }) : null;
        const childState = child ? await second.getJobState(child.id) : 'unknown';

        expect({ parentState, childState }).not.toEqual({
          parentState: 'unknown',
          childState: 'waiting-children',
        });
        if (drainResult.status === 'fulfilled' && drainResult.value > 0) {
          expect(childState).not.toBe('waiting-children');
        }
      } finally {
        if (lockHeld) await blocker`SELECT pg_advisory_unlock(${lockKey})`.catch(() => []);
        await Promise.allSettled([admission ?? Promise.resolve(), drain ?? Promise.resolve()]);
        if (triggerCreated) {
          await observer.unsafe(`DROP TRIGGER IF EXISTS ${triggerName} ON bunqueue_jobs`).simple();
        }
        if (functionCreated) {
          await observer.unsafe(`DROP FUNCTION IF EXISTS ${functionName}()`).simple();
        }
        await Promise.allSettled([
          first.shutdownPostgres(),
          second.shutdownPostgres(),
          blocker.close({ timeout: 5 }),
          observer.close({ timeout: 5 }),
        ]);
      }
    }
  );
});
