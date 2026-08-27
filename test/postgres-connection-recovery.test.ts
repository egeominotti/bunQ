import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, generateJobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  cleanupPostgresNamespace,
  eventually,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

async function retryAfterConnectionReset<T>(operation: () => Promise<T>): Promise<T> {
  const deadline = Date.now() + 5_000;
  let lastError: unknown;
  do {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      await Bun.sleep(20);
    }
  } while (Date.now() < deadline);
  throw lastError;
}

test.skipIf(!postgresUrl)(
  'reconnects foreground pools and event polling after every broker backend is terminated',
  async () => {
    const namespace = `test-connection-reset-${Date.now()}-${crypto.randomUUID()}`;
    const queue = 'connection-reset';
    const first = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace,
        brokerId: 'connection-reset-a',
        pollIntervalMs: 25,
      },
    });
    const second = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace,
        brokerId: 'connection-reset-b',
        pollIntervalMs: 25,
      },
    });
    const admin = new SQL(postgresUrl!, { max: 1 });
    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const applications = admin.array(
        ['bunqueue:connection-reset-a', 'bunqueue:connection-reset-b'],
        'TEXT'
      );
      const terminated = await admin<
        Array<{ application_name: string; pid: number; terminated: boolean }>
      >`
        SELECT application_name, pid, pg_terminate_backend(pid) AS terminated
        FROM pg_stat_activity
        WHERE application_name = ANY(${applications}) AND pid <> pg_backend_pid()
      `;
      expect(new Set(terminated.map(({ application_name }) => application_name))).toEqual(
        new Set(['bunqueue:connection-reset-a', 'bunqueue:connection-reset-b'])
      );
      expect(terminated.every(({ terminated: stopped }) => stopped)).toBe(true);

      const [fromFirst, fromSecond] = await Promise.all([
        retryAfterConnectionReset(() =>
          first.push(queue, {
            customId: 'connection-reset-job-a',
            data: { broker: 'connection-reset-job-a' },
          })
        ),
        retryAfterConnectionReset(() =>
          second.push(queue, {
            customId: 'connection-reset-job-b',
            data: { broker: 'connection-reset-job-b' },
          })
        ),
      ]);
      const expected = [String(fromFirst.id), String(fromSecond.id)].sort();
      expect(
        await eventually(
          () =>
            [first, second].every(
              (broker) =>
                broker
                  .getJobs(queue)
                  .map(({ id }) => String(id))
                  .sort()
                  .join(',') === expected.join(',')
            ),
          5_000
        )
      ).toBe(true);
      expect(await postgresManagerStore(first).getCounts(queue)).toMatchObject({ waiting: 2 });
      expect(postgresManagerStore(first).health().ok).toBe(true);
      expect(postgresManagerStore(second).health().ok).toBe(true);
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      await cleanupPostgresNamespace(postgresUrl!, namespace);
      await admin.close({ timeout: 5 });
    }
  }
);

test.skipIf(!postgresUrl)(
  'rolls back a half-written admission when its backend dies and retries it exactly once',
  async () => {
    const root = new SQL(postgresUrl!, { max: 1 });
    const database = `bq_reset_${Date.now()}_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
    const url = new URL(postgresUrl!);
    url.pathname = `/${database}`;
    let created = false;
    try {
      await root.unsafe(`CREATE DATABASE "${database}"`).simple();
      created = true;
      const store = new PostgresQueueStore({
        url: url.toString(),
        namespace: 'transaction-reset',
        brokerId: 'transaction-reset',
        pollIntervalMs: 25,
      });
      const blocker = new SQL(url.toString(), { max: 1 });
      const admin = new SQL(url.toString(), { max: 1 });
      const lockKey = 'bunqueue:test:transaction-reset';
      try {
        await store.initialize();
        await admin
          .unsafe(
            `CREATE FUNCTION bunqueue_test_block_event() RETURNS trigger
             LANGUAGE plpgsql AS $function$
             BEGIN
               IF NEW.namespace = 'transaction-reset' THEN
                 PERFORM pg_advisory_xact_lock(hashtext('bunqueue:test:transaction-reset'));
               END IF;
               RETURN NEW;
             END;
             $function$;
             CREATE TRIGGER bunqueue_test_block_event
             BEFORE INSERT ON bunqueue_events
             FOR EACH ROW EXECUTE FUNCTION bunqueue_test_block_event();`
          )
          .simple();
        await blocker`SELECT pg_advisory_lock(hashtext(${lockKey}))`;

        const job = createJob(generateJobId(), 'transaction-reset', { data: { attempt: 1 } });
        const interrupted = store.insert(job).then(
          (value) => ({ status: 'fulfilled', value }) as const,
          (error: unknown) => ({ status: 'rejected', error }) as const
        );
        let blockedPid: number | null = null;
        expect(
          await eventually(async () => {
            const [blocked] = await admin<{ pid: number }[]>`
              SELECT pid FROM pg_stat_activity
              WHERE application_name = 'bunqueue:transaction-reset'
                AND wait_event_type = 'Lock'
                AND query LIKE '%INSERT INTO bunqueue_events%'
              LIMIT 1
            `;
            blockedPid = blocked?.pid ?? null;
            return blockedPid !== null;
          }, 5_000)
        ).toBe(true);
        const [killed] = await admin<{ terminated: boolean }[]>`
          SELECT pg_terminate_backend(${blockedPid!}) AS terminated
        `;
        expect(killed.terminated).toBe(true);
        await blocker`SELECT pg_advisory_unlock(hashtext(${lockKey}))`;
        const interruptedResult = await interrupted;
        expect(interruptedResult.status).toBe('rejected');
        if (interruptedResult.status === 'rejected') {
          expect(interruptedResult.error).toBeInstanceOf(Error);
        }

        const [rolledBack] = await admin<
          Array<{ events: number | string | bigint; jobs: number | string | bigint }>
        >`
          SELECT
            (SELECT COUNT(*) FROM bunqueue_jobs WHERE namespace = 'transaction-reset') AS jobs,
            (SELECT COUNT(*) FROM bunqueue_events WHERE namespace = 'transaction-reset') AS events
        `;
        expect({ events: Number(rolledBack.events), jobs: Number(rolledBack.jobs) }).toEqual({
          events: 0,
          jobs: 0,
        });

        expect(await retryAfterConnectionReset(() => store.insert(job))).toMatchObject({
          inserted: true,
          job: { id: job.id },
        });
        expect(
          await retryAfterConnectionReset(() => store.getCounts('transaction-reset'))
        ).toMatchObject({ waiting: 1 });
      } finally {
        await blocker.close({ timeout: 5 });
        await retryAfterConnectionReset(() => store.close());
        await admin.close({ timeout: 5 });
      }
    } finally {
      if (created) await root.unsafe(`DROP DATABASE "${database}" WITH (FORCE)`).simple();
      await root.close({ timeout: 5 });
    }
  },
  15_000
);
