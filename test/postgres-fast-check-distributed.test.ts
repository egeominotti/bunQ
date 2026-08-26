import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { createJob, jobId, type JobId } from '../src/domain/types/job';
import {
  PostgresQueueStore,
  type ClaimedPostgresJob,
} from '../src/infrastructure/persistence/postgres';
import {
  createPostgresFastCheckScope,
  postgresFastCheckParameters,
  postgresTestUrl,
} from './support/postgres-fast-check';

const scope = createPostgresFastCheckScope('distributed');

async function withStores(
  label: string,
  run: (a: PostgresQueueStore, b: PostgresQueueStore) => Promise<void>
): Promise<void> {
  const namespace = scope.namespace(label);
  const a = new PostgresQueueStore({ url: postgresTestUrl!, namespace, brokerId: `${label}-a` });
  const b = new PostgresQueueStore({ url: postgresTestUrl!, namespace, brokerId: `${label}-b` });
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

function createJobs(queue: string, count: number, groupCount = 0) {
  return Array.from({ length: count }, (_, index) =>
    createJob(jobId(`${queue}-${String(index).padStart(3, '0')}`), queue, {
      data: { index },
      ...(groupCount > 0 && { groupId: `group-${index % groupCount}` }),
    })
  );
}

afterAll(() => scope.cleanup(), { timeout: 30_000 });

describe('PostgreSQL fast-check distributed invariants', () => {
  test.skipIf(!postgresTestUrl)(
    'delivers every row once under competing claims',
    async () => {
      await withStores('claims', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.record({
              contenders: fc.integer({ min: 2, max: 16 }),
              jobs: fc.integer({ min: 1, max: 48 }),
            }),
            async ({ contenders, jobs: jobCount }) => {
              const name = queue('claims', run++);
              const jobs = createJobs(name, jobCount);
              await a.insertMany(jobs);
              const requested = Math.max(1, Math.ceil(jobCount / contenders));
              const waves = await Promise.all(
                Array.from({ length: contenders }, (_, index) =>
                  (index % 2 === 0 ? a : b)
                    .claim(name, requested, `owner-${index}`)
                    .then((claims) => ({ claims, store: index % 2 === 0 ? a : b }))
                )
              );
              const claimed = waves.flatMap(({ claims, store }) =>
                claims.map((claim) => ({ claim, store }))
              );
              expect(claimed).toHaveLength(jobCount);
              expect(new Set(claimed.map(({ claim }) => claim.job.id)).size).toBe(jobCount);
              await Promise.all(
                claimed.map(({ claim, store }) => store.complete(claim.job.id, claim.token))
              );
              expect(await a.getCounts(name)).toMatchObject({ active: 0, completed: jobCount });
            }
          ),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'shares one concurrency budget across brokers',
    async () => {
      await withStores('concurrency', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.record({
              jobs: fc.integer({ min: 1, max: 40 }),
              limit: fc.integer({ min: 1, max: 12 }),
            }),
            async ({ jobs: jobCount, limit }) => {
              const name = queue('concurrency', run++);
              await a.insertMany(createJobs(name, jobCount));
              await b.setConcurrency(name, limit);
              const [left, right] = await Promise.all([
                a.claim(name, jobCount, 'concurrency-a'),
                b.claim(name, jobCount, 'concurrency-b'),
              ]);
              const firstWave = [...left, ...right];
              expect(firstWave).toHaveLength(Math.min(jobCount, limit));
              expect(new Set(firstWave.map(({ job }) => job.id)).size).toBe(firstWave.length);
              expect((await a.getCounts(name)).active).toBe(firstWave.length);
              await Promise.all(firstWave.map((claim) => a.complete(claim.job.id, claim.token)));

              const secondWave = await b.claim(name, jobCount, 'concurrency-next');
              expect(secondWave).toHaveLength(Math.min(jobCount - firstWave.length, limit));
            }
          ),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'shares one fixed-window rate budget across brokers',
    async () => {
      await withStores('rate', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.record({
              jobs: fc.integer({ min: 1, max: 36 }),
              limit: fc.integer({ min: 1, max: 10 }),
            }),
            async ({ jobs: jobCount, limit }) => {
              const name = queue('rate', run++);
              await a.insertMany(createJobs(name, jobCount));
              await a.setRateLimit(name, limit, 60_000);
              const [left, right] = await Promise.all([
                a.claim(name, jobCount, 'rate-a'),
                b.claim(name, jobCount, 'rate-b'),
              ]);
              const claimed = [...left, ...right];
              expect(claimed).toHaveLength(Math.min(jobCount, limit));
              expect(new Set(claimed.map(({ job }) => job.id)).size).toBe(claimed.length);
              await Promise.all(claimed.map((item) => b.complete(item.job.id, item.token)));
              expect(await a.claim(name, jobCount, 'rate-extra')).toHaveLength(0);
            }
          ),
          postgresFastCheckParameters(25)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'keeps group ownership exclusive in every wave',
    async () => {
      await withStores('groups', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.record({
              groups: fc.integer({ min: 1, max: 8 }),
              perGroup: fc.integer({ min: 1, max: 6 }),
            }),
            async ({ groups, perGroup }) => {
              const name = queue('groups', run++);
              const total = groups * perGroup;
              await a.insertMany(createJobs(name, total, groups));
              const delivered = new Set<JobId>();
              while (delivered.size < total) {
                const [left, right] = await Promise.all([
                  a.claim(name, total, `group-a-${delivered.size}`),
                  b.claim(name, total, `group-b-${delivered.size}`),
                ]);
                const wave = [...left, ...right];
                expect(wave.length).toBeGreaterThan(0);
                const groupIds = wave.map(({ job }) => job.groupId);
                expect(new Set(groupIds).size).toBe(groupIds.length);
                for (const { job } of wave) {
                  expect(delivered.has(job.id)).toBe(false);
                  delivered.add(job.id);
                }
                await Promise.all(wave.map((item) => a.complete(item.job.id, item.token)));
              }
              expect(delivered.size).toBe(total);
            }
          ),
          postgresFastCheckParameters(20)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'preserves conservation through generated histories',
    async () => {
      type Active = { claim: ClaimedPostgresJob; store: PostgresQueueStore };
      const action = fc.oneof(
        fc.record({ kind: fc.constant('push' as const), priority: fc.integer({ min: 0, max: 5 }) }),
        fc.record({
          broker: fc.boolean(),
          count: fc.integer({ min: 1, max: 4 }),
          kind: fc.constant('claim' as const),
        }),
        fc.record({
          broker: fc.boolean(),
          index: fc.nat(),
          kind: fc.constantFrom('complete' as const, 'fail' as const, 'stale-ack' as const),
        }),
        fc.record({
          broker: fc.boolean(),
          kind: fc.constantFrom('pause' as const, 'resume' as const),
        })
      );
      await withStores('histories', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.array(action, { minLength: 1, maxLength: 50 }), async (actions) => {
            const name = queue('histories', run++);
            const active = new Map<JobId, Active>();
            const terminal = new Set<JobId>();
            let accepted = 0;
            let paused = false;
            for (const [step, action] of actions.entries()) {
              const store = action.broker === true ? b : a;
              if (action.kind === 'push') {
                const id = jobId(`${name}-${String(accepted).padStart(3, '0')}`);
                await store.insert(
                  createJob(id, name, {
                    data: { step },
                    maxAttempts: 2,
                    priority: action.priority,
                  })
                );
                accepted++;
              } else if (action.kind === 'claim') {
                const claims = await store.claim(name, action.count, `history-${step}`);
                if (paused) expect(claims).toHaveLength(0);
                for (const claim of claims) {
                  expect(active.has(claim.job.id)).toBe(false);
                  expect(terminal.has(claim.job.id)).toBe(false);
                  active.set(claim.job.id, { claim, store });
                }
              } else if (action.kind === 'pause' || action.kind === 'resume') {
                paused = action.kind === 'pause';
                await store.pause(name, paused);
              } else if (active.size > 0) {
                const selected = [...active.values()][action.index % active.size];
                if (action.kind === 'stale-ack') {
                  expect(
                    (await store.complete(selected.claim.job.id, 'invalid-token')).applied
                  ).toBe(false);
                } else if (action.kind === 'complete') {
                  const outcome = await store.complete(selected.claim.job.id, selected.claim.token);
                  expect(outcome.applied).toBe(true);
                  active.delete(selected.claim.job.id);
                  terminal.add(selected.claim.job.id);
                } else {
                  const outcome = await store.fail({
                    id: selected.claim.job.id,
                    token: selected.claim.token,
                    error: `failure-${step}`,
                  });
                  expect(outcome.applied).toBe(true);
                  active.delete(selected.claim.job.id);
                  if (outcome.state === 'failed') terminal.add(selected.claim.job.id);
                }
              }

              const rows = await a.list(name, { limit: 1000 });
              const counts = await b.getCounts(name);
              const conserved =
                counts.waiting +
                counts.prioritized +
                counts.delayed +
                counts.waitingChildren +
                counts.active +
                counts.completed +
                counts.failed;
              expect(rows).toHaveLength(accepted);
              expect(new Set(rows.map(({ job }) => job.id)).size).toBe(accepted);
              expect(conserved).toBe(accepted);
              for (const id of active.keys()) {
                expect(rows.find(({ job }) => job.id === id)?.state).toBe('active');
              }
            }
          }),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );
});
