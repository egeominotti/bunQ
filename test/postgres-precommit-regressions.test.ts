import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { PostgresDeferredWriteQueue } from '../src/application/postgres-queue-manager/deferredWrites';
import { jobId, type JobId } from '../src/domain/types/job';
import type {
  ClaimedPostgresJob,
  PostgresQueueStore,
} from '../src/infrastructure/persistence/postgres';
import { cleanupPostgresNamespace, deferred, eventually } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-precommit-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
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
  const job = await producer.push(queue, { customId, data: {}, removeOnComplete: true });
  const claimed = await consumer.pullWithLock(queue, `${customId}-worker`);
  expect(claimed.job?.id).toBe(job.id);
  await consumer.ack(job.id, result, claimed.token!);
  expect(await eventually(() => producer.getResult(job.id) !== undefined)).toBe(true);
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

describe('PostgreSQL pre-commit regressions', () => {
  test.skipIf(!postgresUrl)(
    'preserves the previous progress message when a later update omits it',
    async () => {
      const value = namespace('progress-message');
      const queueManager = manager(value, 'progress-message');
      try {
        await queueManager.waitUntilReady();
        const job = await queueManager.push('progress', { data: {} });
        const claimed = await queueManager.pullWithLock('progress', 'progress-worker');
        expect(claimed.job?.id).toBe(job.id);
        expect(await queueManager.updateProgress(job.id, 10, 'checkpoint')).toBe(true);
        expect(await queueManager.updateProgress(job.id, 20)).toBe(true);

        const stored = await queueManager.getJob(job.id);
        expect(stored?.progress).toBe(20);
        expect(stored?.progressMessage).toBe('checkpoint');
        expect(queueManager.getProgress(job.id)).toEqual({
          progress: 20,
          message: 'checkpoint',
        });
      } finally {
        await queueManager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps completion evidence when single admission deduplicates to another ID',
    async () => {
      const value = namespace('single-dedup-completion');
      const producer = manager(value, 'single-dedup-producer');
      const consumer = manager(value, 'single-dedup-consumer');
      const sql = new SQL(postgresUrl!, { max: 1 });
      const oldId = jobId('old-single');
      try {
        await Promise.all([producer.waitUntilReady(), consumer.waitUntilReady()]);
        await completeRemoved(producer, consumer, 'single', String(oldId), { generation: 1 });
        const owner = await producer.push('single', {
          customId: 'owner-single',
          data: {},
          uniqueKey: 'shared-single',
        });

        const duplicate = await producer.push('single', {
          customId: String(oldId),
          data: {},
          uniqueKey: 'shared-single',
        });

        expect(duplicate.id).toBe(owner.id);
        expect(await completionCount(sql, value, oldId)).toBe(1);
        expect(producer.getResult(oldId)).toEqual({ generation: 1 });
      } finally {
        await Promise.allSettled([
          producer.shutdownPostgres(),
          consumer.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps completion evidence when serial batch admission deduplicates to another ID',
    async () => {
      const value = namespace('batch-dedup-completion');
      const producer = manager(value, 'batch-dedup-producer');
      const consumer = manager(value, 'batch-dedup-consumer');
      const sql = new SQL(postgresUrl!, { max: 1 });
      const oldId = jobId('old-batch');
      try {
        await Promise.all([producer.waitUntilReady(), consumer.waitUntilReady()]);
        await completeRemoved(producer, consumer, 'batch', String(oldId), { generation: 1 });
        const owner = await producer.push('batch', {
          customId: 'owner-batch',
          data: {},
          uniqueKey: 'shared-batch',
        });

        const admitted = await producer.pushBatch('batch', [
          {
            customId: String(oldId),
            data: {},
            uniqueKey: 'shared-batch',
            dependsOn: [owner.id],
          },
          { customId: 'batch-peer', data: {} },
        ]);

        expect(admitted).toEqual([owner.id, jobId('batch-peer')]);
        expect(await completionCount(sql, value, oldId)).toBe(1);
        expect(producer.getResult(oldId)).toEqual({ generation: 1 });
      } finally {
        await Promise.allSettled([
          producer.shutdownPostgres(),
          consumer.shutdownPostgres(),
          sql.close({ timeout: 5 }),
        ]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not release a newer lease when deferred disconnect recovery targets an old generation',
    async () => {
      const value = namespace('disconnect-generation');
      const queueManager = manager(value, 'disconnect-generation');
      const resume = deferred<undefined>();
      let resumed = false;
      try {
        await queueManager.waitUntilReady();
        const first = await queueManager.push('disconnect', { customId: 'stable', data: {} });
        const firstClaim = await queueManager.pullWithLock('disconnect', 'first-worker');
        queueManager.registerClientJob('old-client', first.id);

        const internals = queueManager as unknown as {
          activeTokens: Map<JobId, string>;
          postgresSnapshot: { claim(claim: ClaimedPostgresJob): void };
          postgresStore: PostgresQueueStore;
          writes: PostgresDeferredWriteQueue;
        };
        internals.writes.enqueue(async () => await resume.promise);
        expect(queueManager.forceReleaseClientJobs('old-client')).toBe(1);

        await queueManager.ack(first.id, undefined, firstClaim.token!);
        const second = await queueManager.push('disconnect', { customId: 'stable', data: {} });
        const [secondClaim] = await internals.postgresStore.claim('disconnect', 1, 'second-worker');
        expect(secondClaim.job.id).toBe(second.id);
        expect(secondClaim.token).not.toBe(firstClaim.token);
        internals.activeTokens.set(second.id, secondClaim.token);
        internals.postgresSnapshot.claim(secondClaim);

        resumed = true;
        resume.resolve(undefined);
        await queueManager.flushPostgresWrites();

        expect((await internals.postgresStore.getJob(second.id))?.state).toBe('active');
      } finally {
        if (!resumed) resume.resolve(undefined);
        await Promise.allSettled([queueManager.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'captures every client lease before an awaited multi-job disconnect release',
    async () => {
      const value = namespace('awaited-disconnect-generation');
      const queueManager = manager(value, 'awaited-disconnect-generation');
      const firstReleaseEntered = deferred<undefined>();
      const resumeFirstRelease = deferred<undefined>();
      let resumed = false;
      try {
        await queueManager.waitUntilReady();
        const ids = await queueManager.pushBatch('awaited-disconnect', [
          { customId: 'first', data: {} },
          { customId: 'second', data: {} },
        ]);
        const claims = await queueManager.pullBatchWithLock('awaited-disconnect', 2, 'old-worker');
        const tokens = new Map(claims.jobs.map((job, index) => [job.id, claims.tokens[index]]));
        for (const id of ids) queueManager.registerClientJob('old-client', id);

        const internals = queueManager as unknown as {
          activeTokens: Map<JobId, string>;
          postgresSnapshot: { claim(claim: ClaimedPostgresJob): void };
          postgresStore: PostgresQueueStore;
        };
        const releaseClientLease = internals.postgresStore.releaseClientLease.bind(
          internals.postgresStore
        );
        let releases = 0;
        internals.postgresStore.releaseClientLease = async (id, token) => {
          releases++;
          if (releases === 1) {
            firstReleaseEntered.resolve(undefined);
            await resumeFirstRelease.promise;
          }
          return await releaseClientLease(id, token);
        };

        const releasing = queueManager.releaseClientJobs('old-client');
        await firstReleaseEntered.promise;
        await queueManager.ack(ids[1], undefined, tokens.get(ids[1])!);
        const second = await queueManager.push('awaited-disconnect', {
          customId: String(ids[1]),
          data: { generation: 2 },
        });
        const [secondClaim] = await internals.postgresStore.claim(
          'awaited-disconnect',
          1,
          'new-worker'
        );
        expect(secondClaim.job.id).toBe(second.id);
        internals.activeTokens.set(second.id, secondClaim.token);
        internals.postgresSnapshot.claim(secondClaim);

        resumed = true;
        resumeFirstRelease.resolve(undefined);
        expect(await releasing).toBe(1);
        expect((await internals.postgresStore.getJob(second.id))?.state).toBe('active');
        expect(queueManager.verifyLock(second.id, secondClaim.token)).toBe(true);
      } finally {
        if (!resumed) resumeFirstRelease.resolve(undefined);
        await Promise.allSettled([queueManager.shutdownPostgres()]);
      }
    }
  );
});
