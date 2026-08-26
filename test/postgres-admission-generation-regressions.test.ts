import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { jobId, type JobId } from '../src/domain/types/job';
import { decodePostgresValue } from '../src/infrastructure/persistence/postgres/codec';
import {
  cleanupPostgresNamespace,
  eventually,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-admission-generation-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

async function complete(
  producer: PostgresQueueManager,
  worker: PostgresQueueManager,
  queue: string,
  customId: string,
  result: unknown,
  removeOnComplete: boolean
): Promise<JobId> {
  const job = await producer.push(queue, { customId, data: {}, removeOnComplete });
  const claimed = await worker.pullWithLock(queue, `${customId}-worker`);
  expect(claimed.job?.id).toBe(job.id);
  await worker.ack(job.id, result, claimed.token!);
  expect(await eventually(() => producer.getResult(job.id) !== undefined)).toBe(true);
  return job.id;
}

async function completionCount(sql: SQL, value: string, id: JobId): Promise<number> {
  const [row] = await sql<{ count: number }[]>`
    SELECT COUNT(*)::int AS count
    FROM bunqueue_completions
    WHERE namespace = ${value} AND job_id = ${String(id)}
  `;
  return row.count;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL admission generation regressions', () => {
  test.skipIf(!postgresUrl)(
    'preserves a retained terminal generation when admission deduplicates elsewhere',
    async () => {
      const value = namespace('retained-dedup');
      const producer = manager(value, 'retained-producer');
      const worker = manager(value, 'retained-worker');
      const oldId = jobId('retained-old');
      try {
        await Promise.all([producer.waitUntilReady(), worker.waitUntilReady()]);
        await complete(producer, worker, 'retained', String(oldId), { generation: 1 }, false);
        const owner = await producer.push('retained', {
          customId: 'retained-owner',
          data: {},
          uniqueKey: 'retained-key',
        });

        const duplicate = await producer.push('retained', {
          customId: String(oldId),
          data: {},
          uniqueKey: 'retained-key',
        });

        expect(duplicate.id).toBe(owner.id);
        expect(await producer.getJobState(oldId)).toBe('completed');
        expect(producer.getResult(oldId)).toEqual({ generation: 1 });
        expect(await postgresManagerStore(producer).getResult(oldId)).toEqual({
          found: true,
          result: { generation: 1 },
        });
      } finally {
        await Promise.allSettled([producer.shutdownPostgres(), worker.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not reject a deduplicated candidate whose old completion is pinned',
    async () => {
      const value = namespace('pinned-dedup');
      const producer = manager(value, 'pinned-producer');
      const worker = manager(value, 'pinned-worker');
      const sql = new SQL(postgresUrl!, { max: 1 });
      const oldId = jobId('pinned-old');
      try {
        await Promise.all([producer.waitUntilReady(), worker.waitUntilReady()]);
        await complete(producer, worker, 'pinned', String(oldId), 'old-result', true);
        const dependent = await producer.push('pinned', {
          customId: 'pinned-consumer',
          data: {},
          dependsOn: [oldId],
        });
        const owner = await producer.push('pinned', {
          customId: 'pinned-owner',
          data: {},
          uniqueKey: 'pinned-key',
        });

        const duplicate = await producer.push('pinned', {
          customId: String(oldId),
          data: {},
          uniqueKey: 'pinned-key',
        });

        expect(duplicate.id).toBe(owner.id);
        expect(await producer.getJobState(dependent.id)).toBe('waiting');
        expect(await completionCount(sql, value, oldId)).toBe(1);
      } finally {
        await Promise.allSettled([
          producer.shutdownPostgres(),
          worker.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'falls back from a fast batch conflict without deleting pinned completion evidence',
    async () => {
      const value = namespace('fast-pinned-dedup');
      const producer = manager(value, 'fast-pinned-producer');
      const worker = manager(value, 'fast-pinned-worker');
      const sql = new SQL(postgresUrl!, { max: 1 });
      const oldId = jobId('fast-pinned-old');
      try {
        await Promise.all([producer.waitUntilReady(), worker.waitUntilReady()]);
        await complete(producer, worker, 'fast-pinned', String(oldId), 'old-result', true);
        const dependent = await producer.push('fast-pinned', {
          customId: 'fast-pinned-consumer',
          data: {},
          dependsOn: [oldId],
        });
        const owner = await producer.push('fast-pinned', {
          customId: 'fast-pinned-owner',
          data: {},
          uniqueKey: 'fast-pinned-key',
        });

        const admitted = await producer.pushBatch('fast-pinned', [
          { customId: String(oldId), data: {}, uniqueKey: 'fast-pinned-key' },
          { customId: 'fast-pinned-peer', data: {} },
        ]);

        expect(admitted).toEqual([owner.id, jobId('fast-pinned-peer')]);
        expect(await producer.getJobState(dependent.id)).toBe('waiting');
        expect(await completionCount(sql, value, oldId)).toBe(1);
      } finally {
        await Promise.allSettled([
          producer.shutdownPostgres(),
          worker.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'reconciles a reverse-order consumer against an actually reused tombstone ID',
    async () => {
      const value = namespace('reverse-reuse');
      const producer = manager(value, 'reverse-producer');
      const worker = manager(value, 'reverse-worker');
      const sql = new SQL(postgresUrl!, { max: 1 });
      const parentId = jobId('reverse-parent');
      const childId = jobId('reverse-child');
      try {
        await Promise.all([producer.waitUntilReady(), worker.waitUntilReady()]);
        await complete(producer, worker, 'reverse', String(parentId), 'old-result', true);

        expect(
          await producer.pushBatch('reverse', [
            { customId: String(childId), data: {}, dependsOn: [parentId] },
            { customId: String(parentId), data: { generation: 2 } },
          ])
        ).toEqual([childId, parentId]);
        expect(await producer.getJobState(childId)).toBe('waiting-children');
        expect(await producer.getJobState(parentId)).toBe('waiting');
        const [event] = await sql<{ payload: Uint8Array }[]>`
          SELECT payload FROM bunqueue_events
          WHERE namespace = ${value} AND event_type = 'pushed' AND job_id = ${String(childId)}
          ORDER BY id DESC LIMIT 1
        `;
        expect(
          decodePostgresValue<{ state: string }>(event.payload, { state: 'missing' }, 'test').state
        ).toBe('waiting-children');

        const claimed = await worker.pullWithLock('reverse', 'new-parent-worker');
        expect(claimed.job?.id).toBe(parentId);
        await worker.ack(parentId, 'new-result', claimed.token!);
        expect(
          await eventually(async () => (await producer.getJobState(childId)) === 'waiting')
        ).toBe(true);
      } finally {
        await Promise.allSettled([
          producer.shutdownPostgres(),
          worker.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps pushed and removed events causal during an in-batch dedup replacement',
    async () => {
      const value = namespace('event-order');
      const queueManager = manager(value, 'event-order');
      const sql = new SQL(postgresUrl!, { max: 1 });
      const firstId = jobId('event-first');
      const replacementId = jobId('event-replacement');
      try {
        await queueManager.waitUntilReady();
        expect(
          await queueManager.pushBatch('event-order', [
            { customId: String(firstId), data: { generation: 1 }, uniqueKey: 'event-key' },
            {
              customId: String(replacementId),
              data: { generation: 2 },
              uniqueKey: 'event-key',
              dedup: { replace: true },
            },
          ])
        ).toEqual([firstId, replacementId]);

        const events = await sql<Array<{ event_type: string; job_id: string }>>`
          SELECT event_type, job_id FROM bunqueue_events
          WHERE namespace = ${value} AND event_type IN ('pushed', 'removed')
          ORDER BY id
        `;
        expect(events).toEqual([
          { event_type: 'pushed', job_id: String(firstId) },
          { event_type: 'removed', job_id: String(firstId) },
          { event_type: 'pushed', job_id: String(replacementId) },
        ]);
        expect(await queueManager.getJob(firstId)).toBeNull();
        expect(await queueManager.getJobState(replacementId)).toBe('waiting');
      } finally {
        await Promise.allSettled([queueManager.shutdownPostgres(), sql.close({ timeout: 5 })]);
      }
    }
  );
});
