import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { DEFAULT_DLQ_CONFIG } from '../src/domain/types/dlq';
import { createJob, jobId, type JobId, type JobInput } from '../src/domain/types/job';
import { expirePendingPostgresJobs } from '../src/infrastructure/persistence/postgres/expiry';
import { enforcePostgresDlqLimit } from '../src/infrastructure/persistence/postgres/dlqLifecycle';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  createPostgresFastCheckScope,
  postgresFastCheckParameters,
  postgresTestUrl,
} from './support/postgres-fast-check';

const scope = createPostgresFastCheckScope('destruction');
const operations = [
  'cancel',
  'remove',
  'clean',
  'expire',
  'remove-dlq',
  'purge-dlq',
  'limit-dlq',
  'expire-dlq',
  'retry-completed',
  'obliterate',
  'dedup-replace',
  'remove-on-fail',
  'release-cron',
] as const;
type DestructiveOperation = (typeof operations)[number];

async function withStores(
  run: (a: PostgresQueueStore, b: PostgresQueueStore) => Promise<void>
): Promise<void> {
  const namespace = scope.namespace('campaign');
  const a = new PostgresQueueStore({
    url: postgresTestUrl!,
    namespace,
    brokerId: 'destruction-a',
    leaseDurationMs: 1000,
  });
  const b = new PostgresQueueStore({
    url: postgresTestUrl!,
    namespace,
    brokerId: 'destruction-b',
    leaseDurationMs: 1000,
  });
  try {
    await Promise.all([a.initialize(), b.initialize()]);
    await run(a, b);
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

async function insertPair(
  a: PostgresQueueStore,
  b: PostgresQueueStore,
  label: string,
  input: Partial<JobInput> = {}
): Promise<{ producerId: JobId; consumerId: JobId; producerQueue: string }> {
  const producerId = jobId(`${label}-producer`);
  const consumerId = jobId(`${label}-consumer`);
  const producerQueue = `${label}-source`;
  await a.insert(createJob(producerId, producerQueue, { data: {}, ...input }));
  await b.insert(
    createJob(consumerId, `${label}-consumer-queue`, {
      data: {},
      dependsOn: [producerId],
    })
  );
  return { producerId, consumerId, producerQueue };
}

async function failProducer(
  a: PostgresQueueStore,
  b: PostgresQueueStore,
  queue: string,
  id: JobId
): Promise<void> {
  const [claim] = await a.claim(queue, 1, `worker-${String(id)}`);
  expect(claim?.job.id).toBe(id);
  const failed = await b.fail({ id, token: claim.token, error: 'generated terminal failure' });
  expect(failed).toMatchObject({ applied: true, state: 'failed' });
}

async function completeProducer(
  a: PostgresQueueStore,
  b: PostgresQueueStore,
  queue: string,
  id: JobId
): Promise<void> {
  const [claim] = await a.claim(queue, 1, `worker-${String(id)}`);
  expect((await b.complete(id, claim.token, { id })).applied).toBe(true);
}

async function assertConsumerStillResolvable(
  store: PostgresQueueStore,
  producerId: JobId,
  consumerId: JobId
): Promise<void> {
  const consumer = await store.getJob(consumerId);
  expect(['waiting', 'prioritized', 'delayed', 'waiting-children', 'active']).toContain(
    consumer?.state
  );
  const producer = await store.getJob(producerId);
  const completion = await store.getResult(producerId);
  expect(producer !== null || completion.found).toBe(true);
}

async function executeOperation(
  operation: DestructiveOperation,
  run: number,
  a: PostgresQueueStore,
  b: PostgresQueueStore
): Promise<void> {
  const label = `run-${run}-${operation}`;
  if (operation === 'retry-completed') {
    const producerId = jobId(`${label}-producer`);
    const producerQueue = `${label}-source`;
    await a.insert(createJob(producerId, producerQueue, { data: {} }));
    await completeProducer(a, b, producerQueue, producerId);
    const consumerId = jobId(`${label}-consumer`);
    await b.insert(
      createJob(consumerId, `${label}-consumer-queue`, {
        data: {},
        dependsOn: [producerId],
      })
    );
    expect(await a.retry(producerId)).toBe(false);
    await assertConsumerStillResolvable(b, producerId, consumerId);
    return;
  }

  const input: Partial<JobInput> = {
    ...(operation === 'remove-on-fail' && { removeOnFail: true }),
    ...(['remove-dlq', 'purge-dlq', 'limit-dlq', 'expire-dlq', 'remove-on-fail'].includes(
      operation
    ) && { maxAttempts: 1 }),
    ...(operation === 'expire' && { ttl: 1 }),
    ...(operation === 'release-cron' && { uniqueKey: `cron:${label}` }),
    ...(operation === 'dedup-replace' && { uniqueKey: `${label}-dedup` }),
  };
  const { producerId, consumerId, producerQueue } = await insertPair(a, b, label, input);

  if (operation === 'cancel') expect(await a.cancel(producerId)).toBe(false);
  else if (operation === 'remove') expect(await a.remove(producerId)).toBe(false);
  else if (operation === 'clean') expect(await a.clean(producerQueue, 0, 'waiting')).toEqual([]);
  else if (operation === 'expire') {
    expect(await expirePendingPostgresJobs(a.context, producerQueue, Date.now() + 10_000)).toBe(0);
  } else if (operation === 'obliterate') {
    await expect(a.obliterate(producerQueue)).rejects.toThrow('live dependents');
  } else if (operation === 'dedup-replace') {
    await expect(
      a.insert(
        createJob(jobId(`${label}-replacement`), producerQueue, {
          data: {},
          uniqueKey: `${label}-dedup`,
          dedup: { replace: true },
        })
      )
    ).rejects.toThrow('unresolved dependency consumers');
  } else if (operation === 'release-cron') {
    const [claim] = await a.claim(producerQueue, 1, 'cron-worker');
    expect(await b.releaseClientLease(producerId, claim.token)).toBe(true);
  } else {
    if (operation === 'expire-dlq') {
      await a.setDlqConfig(producerQueue, { ...DEFAULT_DLQ_CONFIG, maxAge: 1 });
    }
    await failProducer(a, b, producerQueue, producerId);
    if (operation === 'remove-dlq')
      expect(await a.removeDlq(producerQueue, producerId)).toBe(false);
    if (operation === 'purge-dlq') expect(await a.purgeDlq(producerQueue)).toEqual([]);
    if (operation === 'limit-dlq') {
      const extraId = jobId(`${label}-z-unprotected`);
      await a.insert(createJob(extraId, producerQueue, { data: {}, maxAttempts: 1 }));
      await failProducer(a, b, producerQueue, extraId);
      expect(await enforcePostgresDlqLimit(a.context, producerQueue, 1)).not.toContain(producerId);
    }
    if (operation === 'expire-dlq') {
      await Bun.sleep(5);
      expect((await a.maintainDlq()).purged).toBe(0);
    }
  }
  await assertConsumerStillResolvable(b, producerId, consumerId);
}

afterAll(() => scope.cleanup(), { timeout: 30_000 });

describe('PostgreSQL fast-check destructive dependency invariants', () => {
  test.skipIf(!postgresTestUrl)(
    'covers every destructive adapter with a live consumer',
    async () => {
      await withStores(async (a, b) => {
        for (const [index, operation] of operations.entries()) {
          await executeOperation(operation, index, a, b);
        }
      });
    }
  );

  test.skipIf(!postgresTestUrl)(
    'never deletes a producer that still has a generated live consumer',
    async () => {
      await withStores(async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.constantFrom(...operations), async (operation) => {
            await executeOperation(operation, run++, a, b);
          }),
          postgresFastCheckParameters(48)
        );
      });
    },
    130_000
  );
});
