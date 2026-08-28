import { SQL } from 'bun';
import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createCronJob } from '../src/domain/types/cron';
import { encodePostgresValue } from '../src/infrastructure/persistence/postgres/codec';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-cron-native-migration-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
}, 30_000);

describe('PostgreSQL persisted Croner schedule migration', () => {
  for (const repeatEvery of [-1, Number.MAX_VALUE]) {
    test.skipIf(!postgresUrl)(
      `rejects repeatEvery=${repeatEvery} through the PostgreSQL public path`,
      async () => {
        const value = namespace();
        const manager = new PostgresQueueManager({
          postgres: { url: postgresUrl!, namespace: value, brokerId: 'cron-invalid-interval' },
        });
        try {
          await manager.waitUntilReady();
          await expect(
            manager.addCronDurable({
              name: 'invalid-interval',
              queue: 'reports',
              data: {},
              repeatEvery,
            })
          ).rejects.toThrow(/repeatEvery.*positive.*safe integer/i);
          expect(await manager.listCrons()).toEqual([]);
        } finally {
          await manager.shutdownPostgres();
        }
      }
    );
  }

  test.skipIf(!postgresUrl)(
    'rejects a persisted non-positive interval before broker registration',
    async () => {
      const value = namespace();
      const bootstrap = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'interval-bootstrap' },
      });
      await bootstrap.waitUntilReady();
      await bootstrap.shutdownPostgres();

      const sql = new SQL(postgresUrl!, { max: 1 });
      const valid = createCronJob(
        {
          name: 'invalid-interval',
          queue: 'reports',
          data: {},
          repeatEvery: 1,
        },
        Date.now() - 1
      );
      const invalid = { ...valid, repeatEvery: -1 };
      await sql`
        INSERT INTO bunqueue_crons
          (namespace, name, payload, next_run, executions, max_limit, updated_at)
        VALUES
          (${value}, ${invalid.name}, ${encodePostgresValue(invalid)}, ${invalid.nextRun}, 0, NULL,
           ${Date.now()})
      `;

      const candidate = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'interval-candidate' },
      });
      try {
        await expect(candidate.waitUntilReady()).rejects.toThrow(
          /Persisted cron "invalid-interval".*repeatEvery.*positive.*safe integer/i
        );
        const [brokerRow] = await sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_brokers
          WHERE namespace = ${value}
        `;
        expect(brokerRow.count).toBe(0);
      } finally {
        await Promise.allSettled([candidate.shutdownPostgres(), sql.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'rejects the namespace before broker registration or valid-row reconciliation',
    async () => {
      const value = namespace();
      const bootstrap = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'cron-bootstrap' },
      });
      await bootstrap.waitUntilReady();
      await bootstrap.shutdownPostgres();

      const sql = new SQL(postgresUrl!, { max: 1 });
      const originalNextRun = Date.now() - 120_000;
      const valid = createCronJob(
        {
          name: 'valid-overdue',
          queue: 'reports',
          data: {},
          schedule: '@daily',
          skipMissedOnRestart: true,
        },
        originalNextRun
      );
      const invalid = createCronJob(
        {
          name: 'legacy-last-day',
          queue: 'reports',
          data: {},
          schedule: '0 0 L * *',
        },
        Date.now() + 86_400_000
      );
      await sql`
        INSERT INTO bunqueue_crons
          (namespace, name, payload, next_run, executions, max_limit, updated_at)
        VALUES
          (${value}, ${valid.name}, ${encodePostgresValue(valid)}, ${valid.nextRun}, 0, NULL,
           ${Date.now()}),
          (${value}, ${invalid.name}, ${encodePostgresValue(invalid)}, ${invalid.nextRun}, 0, NULL,
           ${Date.now()})
      `;

      const candidate = new PostgresQueueManager({
        postgres: { url: postgresUrl!, namespace: value, brokerId: 'cron-candidate' },
      });
      try {
        await expect(candidate.waitUntilReady()).rejects.toThrow(
          /Persisted cron "legacy-last-day".*0 0 L \* \*.*update or remove.*bunqueue 2\.9\.0/i
        );
        const [validRow] = await sql<{ next_run: number | string | bigint }[]>`
          SELECT next_run
          FROM bunqueue_crons
          WHERE namespace = ${value} AND name = ${valid.name}
        `;
        const [brokerRow] = await sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_brokers
          WHERE namespace = ${value}
        `;
        expect(Number(validRow.next_run)).toBe(originalNextRun);
        expect(brokerRow.count).toBe(0);
      } finally {
        await Promise.allSettled([candidate.shutdownPostgres(), sql.close()]);
      }
    }
  );
});
