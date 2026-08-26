import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  createPostgresFastCheckScope,
  postgresFastCheckParameters,
  postgresTestUrl,
} from './support/postgres-fast-check';

const scope = createPostgresFastCheckScope('generation');

async function withStores(
  label: string,
  run: (a: PostgresQueueStore, b: PostgresQueueStore) => Promise<void>
): Promise<void> {
  const namespace = scope.namespace(label);
  const a = new PostgresQueueStore({
    url: postgresTestUrl!,
    namespace,
    brokerId: `${label}-a`,
  });
  const b = new PostgresQueueStore({
    url: postgresTestUrl!,
    namespace,
    brokerId: `${label}-b`,
  });
  try {
    await Promise.all([a.initialize(), b.initialize()]);
    await run(a, b);
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}

function queue(label: string, run: number): string {
  return `${label}-${run}`;
}

function normalizeJsonValue(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value)) as unknown;
}

afterAll(() => scope.cleanup(), { timeout: 30_000 });

describe('PostgreSQL fast-check generation invariants', () => {
  test.skipIf(!postgresTestUrl)(
    'preserves arbitrary progress messages through omitted-message updates',
    async () => {
      await withStores('progress-message', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.string({ maxLength: 80 }),
            fc.array(fc.integer({ min: -100, max: 200 }), { minLength: 1, maxLength: 12 }),
            async (message, updates) => {
              const name = queue('progress-message', run++);
              const id = jobId(`${name}-job`);
              await a.insert(createJob(id, name, { data: {} }));
              const [claim] = await b.claim(name, 1, 'progress-worker');
              expect(await a.updateProgress(id, updates[0], message)).toBe(true);
              for (const progress of updates.slice(1)) {
                expect(await b.updateProgress(id, progress)).toBe(true);
              }
              const row = await a.getJob(id);
              expect(row?.job.progress).toBe(Math.max(0, Math.min(100, updates.at(-1)!)));
              expect(row?.job.progressMessage).toBe(message);
              expect((await b.complete(id, claim.token, undefined, true)).applied).toBe(true);
            }
          ),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'preserves generated completion proofs for every deduplicated admission path',
    async () => {
      await withStores('dedup-proof', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.constantFrom('single' as const, 'serial' as const, 'fast' as const),
            fc.boolean(),
            fc.jsonValue(),
            async (mode, pinned, result) => {
              const name = queue('dedup-proof', run++);
              const oldId = jobId(`${name}-old`);
              await a.insert(createJob(oldId, name, { data: {} }));
              const [oldClaim] = await b.claim(name, 1, 'old-worker');
              expect((await b.complete(oldId, oldClaim.token, result, true)).applied).toBe(true);
              if (pinned) {
                await a.insert(
                  createJob(jobId(`${name}-consumer`), name, {
                    data: {},
                    dependsOn: [oldId],
                  })
                );
              }
              const owner = createJob(jobId(`${name}-owner`), name, {
                data: {},
                uniqueKey: `${name}-key`,
              });
              await a.insert(owner);
              const candidate = createJob(oldId, name, {
                data: {},
                uniqueKey: `${name}-key`,
                ...(mode === 'serial' && { dependsOn: [owner.id] }),
              });

              const returned =
                mode === 'single'
                  ? [(await b.insert(candidate)).job]
                  : await b.insertMany([
                      candidate,
                      createJob(jobId(`${name}-peer`), name, { data: {} }),
                    ]);

              expect(returned[0].id).toBe(owner.id);
              expect(await a.getResult(oldId)).toEqual({
                found: true,
                result: normalizeJsonValue(result),
              });
              if (pinned) {
                expect((await a.getJob(jobId(`${name}-consumer`)))?.state).toBe('waiting');
              }
            }
          ),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'reconciles reverse-order reuse against retained and completion-only generations',
    async () => {
      await withStores('reverse-reuse', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.boolean(), fc.jsonValue(), async (removeOld, oldResult) => {
            const name = queue('reverse-reuse', run++);
            const parentId = jobId(`${name}-parent`);
            const childId = jobId(`${name}-child`);
            await a.insert(createJob(parentId, name, { data: { generation: 1 } }));
            const [oldClaim] = await b.claim(name, 1, 'old-parent-worker');
            expect((await a.complete(parentId, oldClaim.token, oldResult, removeOld)).applied).toBe(
              true
            );

            const stored = await b.insertMany([
              createJob(childId, name, { data: {}, dependsOn: [parentId] }),
              createJob(parentId, name, { data: { generation: 2 } }),
            ]);
            expect(stored.map((job) => job.id)).toEqual([childId, parentId]);
            expect((await a.getJob(childId))?.state).toBe('waiting-children');
            expect((await a.getJob(parentId))?.state).toBe('waiting');

            const [newClaim] = await a.claim(name, 2, 'new-parent-worker');
            expect(newClaim.job.id).toBe(parentId);
            expect((await b.complete(parentId, newClaim.token, 'new-result')).applied).toBe(true);
            expect((await a.getJob(childId))?.state).toBe('waiting');
          }),
          postgresFastCheckParameters(25)
        );
      });
    },
    130_000
  );
});
