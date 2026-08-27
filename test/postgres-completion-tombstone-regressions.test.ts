import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { jobId } from '../src/domain/types/job';
import {
  cleanupPostgresNamespace,
  eventually,
  postgresEventStream,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-completion-tombstone-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string, maxQueueEvents = 10_000): PostgresQueueManager {
  return new PostgresQueueManager({
    maxQueueEvents,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

async function completeRemoved(
  producer: PostgresQueueManager,
  consumer: PostgresQueueManager,
  queue: string,
  customId: string,
  result: unknown
): Promise<void> {
  const job = await producer.push(queue, { data: {}, customId, removeOnComplete: true });
  const claimed = await consumer.pullWithLock(queue, `${customId}-worker`);
  expect(claimed.job?.id).toBe(job.id);
  expect(claimed.token).not.toBeNull();
  await consumer.ack(job.id, result, claimed.token!);
  expect(await producer.getJobState(job.id)).toBe('unknown');
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL completion tombstone lifecycle', () => {
  test.skipIf(!postgresUrl)(
    'retires a removeOnComplete result before reusing the same custom ID',
    async () => {
      const value = namespace('single-reuse');
      const first = manager(value, 'single-reuse-a');
      const second = manager(value, 'single-reuse-b');
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const queue = 'single-reuse';
        await completeRemoved(first, second, queue, 'stable', { generation: 1 });

        const generation = await first.push(queue, {
          customId: 'stable',
          data: { generation: 2 },
        });
        const dependent = await second.push(queue, {
          customId: 'consumer',
          data: {},
          dependsOn: [generation.id],
        });

        expect(await second.getJobState(dependent.id)).toBe('waiting-children');
        expect(await second.getChildrenValues(dependent.id)).toEqual({});
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'retires completion tombstones on the set-based batch admission path',
    async () => {
      const value = namespace('batch-reuse');
      const first = manager(value, 'batch-reuse-a');
      const second = manager(value, 'batch-reuse-b');
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const queue = 'batch-reuse';
        await completeRemoved(first, second, queue, 'stable-batch', { generation: 1 });

        const ids = await first.pushBatch(queue, [
          { customId: 'stable-batch', data: { generation: 2 } },
          { customId: 'batch-peer', data: { generation: 1 } },
        ]);
        expect(ids.map(String)).toEqual(['stable-batch', 'batch-peer']);
        const dependent = await second.push(queue, {
          customId: 'batch-consumer',
          data: {},
          dependsOn: [ids[0]],
        });

        expect(await second.getJobState(dependent.id)).toBe('waiting-children');
        expect(await second.getChildrenValues(dependent.id)).toEqual({});
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'obliterate removes completion-only rows and rejects later dependencies',
    async () => {
      const value = namespace('obliterate');
      const first = manager(value, 'obliterate-a');
      const second = manager(value, 'obliterate-b');
      const sql = new SQL(postgresUrl!, { max: 1 });
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        await completeRemoved(first, second, 'source', 'removed', { stale: true });
        const projection = await postgresManagerStore(second).loadJobProjections([
          { id: jobId('removed'), queue: '' },
        ]);
        expect(projection.get(jobId('removed'))?.completion?.queue).toBe('source');

        await first.obliterateDurable('source');
        const [row] = await sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count
          FROM bunqueue_completions
          WHERE namespace = ${value} AND queue = 'source'
        `;

        expect(row.count).toBe(0);
        const stream = postgresEventStream(second);
        await stream.drain();
        expect(await eventually(() => second.getResult('removed') === undefined)).toBe(true);
        await expect(second.push('consumer', { data: {}, dependsOn: ['removed'] })).rejects.toThrow(
          'Dependency job not found: removed'
        );
      } finally {
        await Promise.allSettled([
          first.shutdownPostgres(),
          second.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );
});

describe('PostgreSQL zero event retention', () => {
  test.skipIf(!postgresUrl)('rejects maxQueueEvents zero instead of retaining it silently', () => {
    const value = namespace('zero-events');
    expect(() => manager(value, 'zero-events', 0)).toThrow(
      'PostgreSQL storage requires maxQueueEvents to be at least 1'
    );
  });
});
