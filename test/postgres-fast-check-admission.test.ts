import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { createJob, jobId, type Job } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  createPostgresFastCheckScope,
  postgresFastCheckParameters,
  postgresTestUrl,
} from './support/postgres-fast-check';

const scope = createPostgresFastCheckScope('admission');

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

describe('PostgreSQL fast-check admission and ordering', () => {
  test.skipIf(!postgresTestUrl)(
    'round-trips arbitrary JSON batches without loss',
    async () => {
      await withStores('batch-roundtrip', async (a) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.array(
              fc.record({
                data: fc.jsonValue(),
                lifo: fc.boolean(),
                priority: fc.integer({ min: 0, max: 32 }),
              }),
              { minLength: 1, maxLength: 24 }
            ),
            async (inputs) => {
              const name = queue('batch-roundtrip', run++);
              const now = await a.now();
              const jobs = inputs.map((input, index) =>
                createJob(
                  jobId(`${name}-${String(index).padStart(3, '0')}`),
                  name,
                  { ...input },
                  now
                )
              );
              const stored = await a.insertMany(jobs);
              expect(stored.map((job) => job.id)).toEqual(jobs.map((job) => job.id));

              const rows = await a.getJobs(jobs.map((job) => job.id));
              const byId = new Map(rows.map((row) => [row.job.id, row]));
              expect(jobs.map((job) => byId.get(job.id)?.job.data)).toEqual(
                jobs.map((job) => normalizeJsonValue(job.data))
              );
              const counts = await a.getCounts(name);
              expect(counts.waiting).toBe(inputs.filter((input) => input.priority === 0).length);
              expect(counts.prioritized).toBe(inputs.filter((input) => input.priority > 0).length);
            }
          ),
          postgresFastCheckParameters(35)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'linearizes concurrent custom-ID admission',
    async () => {
      await withStores('custom-id', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 2, max: 20 }),
            fc.jsonValue(),
            async (attempts, data) => {
              const name = queue('custom-id', run++);
              const id = jobId(`${name}-stable`);
              const results = await Promise.all(
                Array.from({ length: attempts }, (_, index) =>
                  (index % 2 === 0 ? a : b).insert(
                    createJob(id, name, { data: { attempt: index, value: data } })
                  )
                )
              );
              expect(results.filter((result) => result.inserted)).toHaveLength(1);
              expect(new Set(results.map((result) => result.job.id))).toEqual(new Set([id]));
              expect((await a.list(name)).map((row) => row.job.id)).toEqual([id]);
            }
          ),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'linearizes concurrent deduplication keys',
    async () => {
      await withStores('unique-key', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.integer({ min: 2, max: 20 }), async (attempts) => {
            const name = queue('unique-key', run++);
            const results = await Promise.all(
              Array.from({ length: attempts }, (_, index) =>
                (index % 2 === 0 ? a : b).insert(
                  createJob(jobId(`${name}-${index}`), name, {
                    data: { attempt: index },
                    uniqueKey: `${name}-key`,
                  })
                )
              )
            );
            expect(results.filter((result) => result.inserted)).toHaveLength(1);
            expect(new Set(results.map((result) => result.job.id)).size).toBe(1);
            expect(await a.list(name)).toHaveLength(1);
          }),
          postgresFastCheckParameters(30)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'matches the priority/FIFO/LIFO ordering model',
    async () => {
      await withStores('ordering', async (a) => {
        let run = 0;
        const item = fc.record({
          lifo: fc.boolean(),
          priority: fc.integer({ min: 0, max: 8 }),
        });
        await fc.assert(
          fc.asyncProperty(fc.array(item, { minLength: 1, maxLength: 24 }), async (inputs) => {
            const name = queue('ordering', run++);
            const now = await a.now();
            const jobs = inputs.map((input, index) =>
              createJob(
                jobId(`${name}-${String(index).padStart(3, '0')}`),
                name,
                {
                  data: { index },
                  ...input,
                },
                now
              )
            );
            await a.insertMany(jobs);
            const claimed = await a.claim(name, jobs.length, 'ordering-worker');
            const expected = inputs
              .map((input, index) => ({ ...input, index }))
              .sort(
                (left, right) =>
                  right.priority - left.priority ||
                  Number(right.lifo) - Number(left.lifo) ||
                  (left.lifo ? right.index - left.index : left.index - right.index)
              )
              .map(({ index }) => index);
            expect(claimed.map(({ job }) => (job.data as { index: number }).index)).toEqual(
              expected
            );
          }),
          postgresFastCheckParameters(35)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'releases a generated dependency fan-in exactly once',
    async () => {
      await withStores('dependencies', async (a, b) => {
        let run = 0;
        const scenario = fc.integer({ min: 1, max: 8 }).chain((count) =>
          fc
            .shuffledSubarray(
              Array.from({ length: count }, (_, index) => index),
              { minLength: count, maxLength: count }
            )
            .map((order) => ({ count, order }))
        );
        await fc.assert(
          fc.asyncProperty(scenario, async ({ count, order }) => {
            const name = queue('dependencies', run++);
            const children = Array.from({ length: count }, (_, index) =>
              createJob(jobId(`${name}-child-${index}`), name, { data: { index } })
            );
            const parent = createJob(jobId(`${name}-parent`), name, {
              data: { role: 'parent' },
              dependsOn: children.map((child) => child.id),
            });
            await a.insertMany([...children, parent]);
            const claimed = await b.claim(name, count + 1, 'dependency-worker');
            expect(claimed.map(({ job }) => job.id).sort()).toEqual(
              children.map((child) => child.id).sort()
            );
            const byId = new Map(claimed.map((item) => [item.job.id, item]));
            for (let position = 0; position < order.length; position++) {
              const index = order[position];
              const item = byId.get(children[index].id)!;
              const completer = position % 2 === 0 ? a : b;
              expect((await completer.complete(item.job.id, item.token, { index })).applied).toBe(
                true
              );
              expect((await a.getJob(parent.id))?.state).toBe(
                position === order.length - 1 ? 'waiting' : 'waiting-children'
              );
            }
            const released = await b.claim(name, 2, 'parent-worker');
            expect(released.map(({ job }) => job.id)).toEqual([parent.id]);
            expect(await a.getChildrenValues(parent.id)).toEqual(
              Object.fromEntries(children.map((child, index) => [`${name}:${child.id}`, { index }]))
            );
          }),
          postgresFastCheckParameters(25)
        );
      });
    },
    130_000
  );

  test.skipIf(!postgresTestUrl)(
    'preserves fresh rows through batch conflict fallback',
    async () => {
      await withStores('batch-conflict', async (a, b) => {
        let run = 0;
        await fc.assert(
          fc.asyncProperty(fc.integer({ min: 1, max: 16 }), async (freshCount) => {
            const name = queue('batch-conflict', run++);
            const existing = createJob(jobId(`${name}-existing`), name, {
              data: { generation: 1 },
              uniqueKey: `${name}-shared`,
            });
            await a.insert(existing);
            const batch: Job[] = [
              createJob(jobId(`${name}-duplicate`), name, {
                data: { generation: 2 },
                uniqueKey: `${name}-shared`,
              }),
              ...Array.from({ length: freshCount }, (_, index) =>
                createJob(jobId(`${name}-fresh-${index}`), name, {
                  data: { index },
                  uniqueKey: `${name}-fresh-key-${index}`,
                })
              ),
            ];
            const stored = await b.insertMany(batch);
            expect(stored[0].id).toBe(existing.id);
            expect(stored.slice(1).map((job) => job.id)).toEqual(
              batch.slice(1).map((job) => job.id)
            );
            expect(await a.list(name)).toHaveLength(freshCount + 1);
          }),
          postgresFastCheckParameters(25)
        );
      });
    },
    130_000
  );
});
