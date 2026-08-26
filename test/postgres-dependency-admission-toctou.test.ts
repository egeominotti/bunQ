import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { jobId } from '../src/domain/types/job';
import { handlePush, handlePushBatch } from '../src/infrastructure/server/handlers/core';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { cleanupPostgresNamespace, deferred } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-dependency-admission-toctou-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function context(queueManager: PostgresQueueManager): HandlerContext {
  return { queueManager, authTokens: new Set(), authenticated: true };
}

async function raceDependencyRemoval(batch: boolean): Promise<void> {
  const value = namespace(batch ? 'pushb' : 'push');
  const first = manager(value, 'removal-a');
  const second = manager(value, 'admission-b');
  const parentQueue = `parents-${crypto.randomUUID()}`;
  const childQueue = `children-${crypto.randomUUID()}`;
  const childId = jobId(`child-${crypto.randomUUID()}`);
  try {
    await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
    const parent = await first.push(parentQueue, { data: { parent: true } });
    const checked = deferred<undefined>();
    const resumeAdmission = deferred<undefined>();
    const findMissing = second.findMissingDependenciesDurable.bind(second);
    second.findMissingDependenciesDurable = async (ids) => {
      const missing = await findMissing(ids);
      checked.resolve(undefined);
      await resumeAdmission.promise;
      return missing;
    };

    const responsePromise = batch
      ? handlePushBatch(
          {
            cmd: 'PUSHB',
            queue: childQueue,
            jobs: [{ customId: childId, data: { child: true }, dependsOn: [parent.id] }],
          },
          context(second)
        )
      : handlePush(
          {
            cmd: 'PUSH',
            queue: childQueue,
            jobId: childId,
            data: { child: true },
            dependsOn: [parent.id],
          },
          context(second)
        );

    await checked.promise;
    expect(await first.drainDurable(parentQueue)).toBe(1);
    expect(await first.getJob(parent.id)).toBeNull();
    resumeAdmission.resolve(undefined);

    const response = await responsePromise;
    const child = await second.getJob(childId);
    const childState = await second.getJobState(childId);
    expect({
      responseOk: response.ok,
      responseError: response.ok ? null : response.error,
      childState,
    }).toEqual({
      responseOk: false,
      responseError: `Dependency job not found: ${parent.id}`,
      childState: 'unknown',
    });
    expect(child).toBeNull();
  } finally {
    await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL dependency admission/removal serialization', () => {
  test.skipIf(!postgresUrl)(
    'PUSH cannot create an orphan after its dependency is drained post-validation',
    async () => await raceDependencyRemoval(false)
  );

  test.skipIf(!postgresUrl)(
    'PUSHB cannot create an orphan after its dependency is drained post-validation',
    async () => await raceDependencyRemoval(true)
  );

  test.skipIf(!postgresUrl)(
    'keeps reverse-order intra-batch dependency admission atomic',
    async () => {
      const value = namespace('reverse-batch');
      const queue = `reverse-${crypto.randomUUID()}`;
      const queueManager = manager(value, 'reverse-batch');
      const parentId = jobId(`parent-${crypto.randomUUID()}`);
      const childId = jobId(`child-${crypto.randomUUID()}`);
      try {
        await queueManager.waitUntilReady();
        expect(
          await handlePushBatch(
            {
              cmd: 'PUSHB',
              queue,
              jobs: [
                { customId: childId, data: { child: true }, dependsOn: [parentId] },
                { customId: parentId, data: { parent: true } },
              ],
            },
            context(queueManager)
          )
        ).toMatchObject({ ok: true, ids: [childId, parentId] });
        expect(await queueManager.getJobState(childId)).toBe('waiting-children');
        const claimed = await queueManager.pullWithLock(queue, 'reverse-worker', 0);
        expect(claimed.job?.id).toBe(parentId);
        await queueManager.ack(parentId, undefined, claimed.token!);
        expect(await queueManager.getJobState(childId)).toBe('waiting');
      } finally {
        await queueManager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'rolls back an intra-batch child when its planned parent deduplicates to another ID',
    async () => {
      const value = namespace('deduplicated-parent');
      const queue = `deduplicated-${crypto.randomUUID()}`;
      const queueManager = manager(value, 'deduplicated-parent');
      const parentId = jobId(`parent-${crypto.randomUUID()}`);
      const childId = jobId(`child-${crypto.randomUUID()}`);
      try {
        await queueManager.waitUntilReady();
        const existing = await queueManager.push(queue, {
          data: { existing: true },
          uniqueKey: 'shared-parent-key',
        });
        const response = await handlePushBatch(
          {
            cmd: 'PUSHB',
            queue,
            jobs: [
              { customId: childId, data: { child: true }, dependsOn: [parentId] },
              {
                customId: parentId,
                data: { parent: true },
                uniqueKey: 'shared-parent-key',
              },
            ],
          },
          context(queueManager)
        );

        expect(response).toEqual({
          ok: false,
          error: `Dependency job not found: ${parentId}`,
        });
        expect(await queueManager.getJob(childId)).toBeNull();
        expect(await queueManager.getJob(parentId)).toBeNull();
        expect(await queueManager.getJob(existing.id)).not.toBeNull();
      } finally {
        await queueManager.shutdownPostgres();
      }
    }
  );
});
