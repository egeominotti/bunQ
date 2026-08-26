import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { createJob, jobId, type JobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  createPostgresFastCheckScope,
  postgresFastCheckParameters,
  postgresTestUrl,
} from './support/postgres-fast-check';

const scope = createPostgresFastCheckScope('lifecycle');

async function withStores(
  label: string,
  run: (a: PostgresQueueStore, b: PostgresQueueStore) => Promise<void>
): Promise<void> {
  const namespace = scope.namespace(label);
  const a = new PostgresQueueStore({
    url: postgresTestUrl!,
    namespace,
    brokerId: `${label}-a`,
    leaseDurationMs: 1000,
  });
  const b = new PostgresQueueStore({
    url: postgresTestUrl!,
    namespace,
    brokerId: `${label}-b`,
    leaseDurationMs: 1000,
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

async function waitForRecoveredJob(store: PostgresQueueStore, id: JobId): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const row = await store.getJob(id);
    if (row?.state === 'waiting') return;
    if (row?.state === 'failed') {
      throw new Error(`Expired lease ${id} became terminal instead of retryable`);
    }
    await Bun.sleep(20);
  }
  throw new Error(`Timed out waiting for expired lease ${id} to become waiting`);
}

afterAll(() => scope.cleanup(), { timeout: 30_000 });

describe('PostgreSQL fast-check lifecycle invariants', () => {
  test.skipIf(!postgresTestUrl)(
    'honors generated retry bounds and idempotent DLQ retry',
    async () => {
      await withStores('retry', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.integer({ min: 1, max: 6 }), async (maxAttempts) => {
            const name = queue('retry', run++);
            const id = jobId(`${name}-job`);
            await a.insert(
              createJob(id, name, { data: {}, maxAttempts, backoff: 0, customId: String(id) })
            );
            for (let attempt = 1; attempt <= maxAttempts; attempt++) {
              const store = attempt % 2 === 0 ? b : a;
              const [claim] = await store.claim(name, 1, `retry-${attempt}`);
              expect(claim.job.id).toBe(id);
              const outcome = await store.fail({
                id,
                token: claim.token,
                error: `failure-${attempt}`,
              });
              expect(outcome.applied).toBe(true);
              expect(outcome.state).toBe(attempt === maxAttempts ? 'failed' : 'waiting');
            }
            expect((await a.getDlq(name)).map((entry) => entry.job.id)).toEqual([id]);
            const retried = await Promise.all([a.retry(id), b.retry(id)]);
            expect(retried.filter(Boolean)).toHaveLength(1);
            expect((await b.getJob(id))?.job.attempts).toBe(0);
            expect((await a.claim(name, 2, 'retried-worker')).map(({ job }) => job.id)).toEqual([
              id,
            ]);
          }),
          postgresFastCheckParameters(25)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'fences every generated stale lease outcome',
    async () => {
      await withStores('lease-fencing', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.jsonValue(), async (result) => {
            const name = queue('lease-fencing', run++);
            const id = jobId(`${name}-job`);
            await a.insert(createJob(id, name, { data: {}, maxAttempts: 3, backoff: 0 }));
            const [first] = await a.claim(name, 1, 'stale-owner', 1000);
            await Bun.sleep(1050);
            expect((await a.complete(id, first.token, result)).applied).toBe(false);
            await b.recoverExpired();
            await waitForRecoveredJob(b, id);
            const [second] = await b.claim(name, 1, 'fresh-owner', 1000);
            expect(second.job.id).toBe(id);
            expect(second.token).not.toBe(first.token);
            expect((await a.complete(id, first.token, result)).applied).toBe(false);
            expect((await b.complete(id, second.token, result)).applied).toBe(true);
            expect(await a.getResult(id)).toEqual({
              found: true,
              result: normalizeJsonValue(result),
            });
          }),
          postgresFastCheckParameters(5)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'expires arbitrary pending TTL batches without resurrection',
    async () => {
      await withStores('ttl', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.record({
              count: fc.integer({ min: 1, max: 12 }),
              ttl: fc.integer({ min: 20, max: 80 }),
            }),
            async ({ count, ttl }) => {
              const name = queue('ttl', run++);
              const jobs = Array.from({ length: count }, (_, index) =>
                createJob(jobId(`${name}-${index}`), name, { data: { index }, ttl })
              );
              await a.insertMany(jobs);
              await Bun.sleep(ttl + 20);
              expect(await b.claim(name, count, 'ttl-worker')).toHaveLength(0);
              expect(await a.list(name)).toHaveLength(0);
              expect(await b.getCounts(name)).toMatchObject({
                active: 0,
                delayed: 0,
                prioritized: 0,
                waiting: 0,
              });
              expect(await Promise.all(jobs.map((job) => a.getJob(job.id)))).toEqual(
                jobs.map(() => null)
              );
            }
          ),
          postgresFastCheckParameters(20)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'keeps generated deduplication TTLs before expiry and releases them after database expiry',
    async () => {
      await withStores('dedup-ttl', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.integer({ min: 60_000, max: 120_000 }), async (ttl) => {
            const name = queue('dedup-ttl', run++);
            const first = createJob(jobId(`${name}-first`), name, {
              data: { generation: 1 },
              uniqueKey: `${name}-key`,
              dedup: { ttl },
            });
            expect((await a.insert(first)).inserted).toBe(true);
            const duplicate = await b.insert(
              createJob(jobId(`${name}-duplicate`), name, {
                data: { generation: 2 },
                uniqueKey: `${name}-key`,
              })
            );
            expect(duplicate.inserted).toBe(false);
            expect(duplicate.job.id).toBe(first.id);
            await a.context.sql`
              UPDATE bunqueue_jobs
              SET unique_expires_at =
                floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint - 1
              WHERE namespace = ${a.context.config.namespace} AND id = ${String(first.id)}
            `;
            const successor = await b.insert(
              createJob(jobId(`${name}-successor`), name, {
                data: { generation: 3 },
                uniqueKey: `${name}-key`,
              })
            );
            expect(successor.inserted).toBe(true);
            expect(successor.job.id).not.toBe(first.id);
            expect((await a.getJob(first.id))?.job.uniqueKey).toBeNull();
          }),
          postgresFastCheckParameters(15)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'persists arbitrary results, progress, and bounded logs',
    async () => {
      await withStores('payloads', async (a, b) => {
        let run = 0;
        const log = fc.record({
          level: fc.constantFrom('info' as const, 'warn' as const, 'error' as const),
          message: fc.string({ maxLength: 80 }),
        });
        await fc.assert(
          fc.asyncProperty(
            fc.record({
              keep: fc.integer({ min: 1, max: 8 }),
              logs: fc.array(log, { minLength: 1, maxLength: 16 }),
              progress: fc.integer({ min: 0, max: 100 }),
              result: fc.jsonValue(),
            }),
            async ({ keep, logs, progress, result }) => {
              const name = queue('payloads', run++);
              const id = jobId(`${name}-job`);
              await a.insert(createJob(id, name, { data: {} }));
              const [claim] = await b.claim(name, 1, 'payload-worker');
              expect(await a.updateProgress(id, progress, `progress-${progress}`)).toBe(true);
              for (const entry of logs) {
                expect(await b.addLog(id, entry.message, entry.level)).toBe(true);
              }
              await a.clearLogs(id, keep);
              expect(
                (await b.getLogs(id)).map(({ level, message }) => ({ level, message }))
              ).toEqual(logs.slice(-keep));
              expect((await a.complete(id, claim.token, result)).applied).toBe(true);
              expect(await b.getResult(id)).toEqual({
                found: true,
                result: normalizeJsonValue(result),
              });
            }
          ),
          postgresFastCheckParameters(25)
        );
      });
    },
    130_000
  );
});
