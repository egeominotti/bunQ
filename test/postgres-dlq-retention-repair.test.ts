import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { DEFAULT_DLQ_CONFIG } from '../src/domain/types/dlq';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { encodePostgresValue } from '../src/infrastructure/persistence/postgres/codec';
import {
  maintainPostgresDlq,
  repairPostgresDlq,
} from '../src/infrastructure/persistence/postgres/dlqMaintenance';
import { cleanupPostgresNamespace, postgresManagerStore } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-dlq-retention-repair-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId },
  });
}

async function createOverage(
  queueManager: PostgresQueueManager,
  value: string,
  queue: string,
  count: number,
  retained: number
): Promise<void> {
  await queueManager.setDlqConfigDurable(queue, { maxEntries: count });
  for (let index = 0; index < count; index++) {
    const job = await queueManager.push(queue, { data: { index } });
    const claim = await queueManager.pullWithLock(queue, `worker-${index}`);
    expect(claim.job?.id).toBe(job.id);
    await queueManager.fail(job.id, 'terminal', claim.token!, true);
  }
  await postgresManagerStore(queueManager).context.sql`
    UPDATE bunqueue_queue_state
    SET dlq_config = ${encodePostgresValue({ ...DEFAULT_DLQ_CONFIG, maxEntries: retained })}
    WHERE namespace = ${value} AND queue = ${queue}
  `;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL DLQ retention repair', () => {
  test.skipIf(!postgresUrl)('repairs a persisted DLQ overage during broker startup', async () => {
    const value = namespace();
    const queue = 'repair';
    const writer = manager(value, 'writer');
    let reader: PostgresQueueManager | null = null;

    try {
      await writer.waitUntilReady();
      await createOverage(writer, value, queue, 3, 1);
      await writer.shutdownPostgres();

      reader = manager(value, 'reader');
      await reader.waitUntilReady();
      const [remaining] = await postgresManagerStore(reader).context.sql<{ count: number }[]>`
        SELECT COUNT(*)::int AS count
        FROM bunqueue_jobs
        WHERE namespace = ${value} AND queue = ${queue} AND state = 'failed'
      `;
      expect(remaining.count).toBe(1);
    } finally {
      await Promise.allSettled([
        writer.shutdownPostgres(),
        reader?.shutdownPostgres() ?? Promise.resolve(),
      ]);
    }
  });

  test.skipIf(!postgresUrl)('repairs a live overage through periodic maintenance', async () => {
    const value = namespace();
    const queue = 'periodic';
    const writer = manager(value, 'periodic-writer');
    try {
      await writer.waitUntilReady();
      await createOverage(writer, value, queue, 3, 1);
      const writerStore = postgresManagerStore(writer);

      await maintainPostgresDlq(writerStore.context);

      const [remaining] = await writerStore.context.sql<{ count: number }[]>`
        SELECT COUNT(*)::int AS count FROM bunqueue_jobs
        WHERE namespace = ${value} AND queue = ${queue} AND state = 'failed'
      `;
      expect(remaining.count).toBe(1);
    } finally {
      await writer.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)(
    'is idempotent when four brokers repair the same overage concurrently',
    async () => {
      const value = namespace();
      const queue = 'four-brokers';
      const writer = manager(value, 'four-writer');
      const peers = Array.from(
        { length: 3 },
        (_, index) =>
          new PostgresQueueStore({
            url: postgresUrl!,
            namespace: value,
            brokerId: `four-peer-${index}`,
          })
      );
      try {
        await Promise.all([writer.waitUntilReady(), ...peers.map((peer) => peer.initialize())]);
        await createOverage(writer, value, queue, 8, 3);
        const writerStore = postgresManagerStore(writer);
        const expected = await writerStore.context.sql<{ id: string }[]>`
          SELECT id FROM bunqueue_jobs
          WHERE namespace = ${value} AND queue = ${queue} AND state = 'failed'
          ORDER BY completed_at DESC NULLS LAST, id DESC
          LIMIT 3
        `;
        const stores = [writerStore, ...peers];

        const removed = await Promise.all(
          stores.map((queueStore) => repairPostgresDlq(queueStore.context))
        );
        const retained = await writerStore.context.sql<{ id: string }[]>`
          SELECT id FROM bunqueue_jobs
          WHERE namespace = ${value} AND queue = ${queue} AND state = 'failed'
          ORDER BY completed_at DESC NULLS LAST, id DESC
        `;

        expect(removed.reduce((sum, count) => sum + count, 0)).toBe(5);
        expect(retained).toEqual(expected);
        expect(
          await Promise.all(stores.map((queueStore) => repairPostgresDlq(queueStore.context)))
        ).toEqual([0, 0, 0, 0]);
      } finally {
        await Promise.allSettled([writer.shutdownPostgres(), ...peers.map((peer) => peer.close())]);
      }
    }
  );
});
