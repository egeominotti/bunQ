import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { createJob, jobId } from '../src/domain/types/job';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-event-count-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL event-retention count', () => {
  test.skipIf(!postgresUrl)(
    'tracks exact retention without scanning from the newest event',
    async () => {
      const value = namespace('exact');
      const queue = 'event-count';
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'event-count-broker',
        maxQueueEvents: 100,
      });

      const retained = async () => {
        const [row] = await store.context.sql<
          Array<{ actual: number; tracked: number | string | bigint | null }>
        >`
        SELECT COUNT(event.id)::int AS actual, MAX(state.retained_count) AS tracked
        FROM bunqueue_events AS event
        FULL JOIN bunqueue_event_retention_state AS state
          ON state.namespace = event.namespace AND state.queue = event.queue
        WHERE COALESCE(event.namespace, state.namespace) = ${value}
          AND COALESCE(event.queue, state.queue) = ${queue}
      `;
        return { actual: row.actual, tracked: Number(row.tracked ?? 0) };
      };

      try {
        await store.initialize();
        for (let batch = 0; batch < 5; batch++) {
          await store.insertMany(
            Array.from({ length: 25 }, (_, index) =>
              createJob(jobId(`event-count-${batch}-${index}`), queue, {
                data: { batch, index },
              })
            )
          );
        }

        expect(await retained()).toEqual({ actual: 100, tracked: 100 });
        expect(await store.trimQueueEvents(queue, 10)).toBe(90);
        expect(await retained()).toEqual({ actual: 10, tracked: 10 });
        expect(await store.trimQueueEvents(queue, 0)).toBe(10);
        expect(await retained()).toEqual({ actual: 0, tracked: 0 });
      } finally {
        await store.close();
      }
    }
  );
});
