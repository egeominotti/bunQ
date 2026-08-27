import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { PostgresPublicApiHarness } from './support/postgres-public-api-harness';
import { expectedPostgresVersionPrefix } from './support/postgres-version';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
let harness: PostgresPublicApiHarness | null = null;

beforeAll(async () => {
  if (!postgresUrl) return;
  harness = await PostgresPublicApiHarness.start(postgresUrl, expectedPostgresVersionPrefix);
});

afterEach(async () => {
  await harness?.closeClients();
});

afterAll(async () => {
  await harness?.close();
  harness = null;
});

function fixture(): PostgresPublicApiHarness {
  if (!harness) throw new Error('PostgreSQL public API harness is not running');
  return harness;
}

describe(`PostgreSQL ${expectedPostgresVersionPrefix} public FlowProducer API`, () => {
  test.skipIf(!postgresUrl)(
    'executes a durable three-level cross-queue flow exactly once across four brokers',
    async () => {
      const current = fixture();
      const leafQueue = current.unique('flow-leaf');
      const transformQueue = current.unique('flow-transform');
      const rootQueue = current.unique('flow-root');
      const effects: string[] = [];
      const executions = new Map<string, number>();
      const mark = (id: string): void => {
        effects.push(id);
        executions.set(id, (executions.get(id) ?? 0) + 1);
      };

      current.worker<Record<string, unknown>, number>(leafQueue, 1, async (job) => {
        mark(job.id);
        return Number(job.data.value);
      });
      current.worker<Record<string, unknown>, number>(transformQueue, 2, async (job) => {
        mark(job.id);
        const values = await job.getChildrenValues<number>();
        return Object.values(values).reduce((sum, value) => sum + value, 0) * 2;
      });
      current.worker<Record<string, unknown>, number>(rootQueue, 3, async (job) => {
        mark(job.id);
        const values = await job.getChildrenValues<number>();
        return Object.values(values).reduce((sum, value) => sum + value, 0);
      });

      const producer = current.flow(0);
      await producer.waitUntilReady();
      const suffix = crypto.randomUUID();
      const root = await producer.add<Record<string, unknown>>({
        children: [
          {
            children: [
              {
                data: { value: 2 },
                name: 'extract-a',
                opts: { durable: true, jobId: `extract-a-${suffix}` },
                queueName: leafQueue,
              },
              {
                data: { value: 3 },
                name: 'extract-b',
                opts: { durable: true, jobId: `extract-b-${suffix}` },
                queueName: leafQueue,
              },
            ],
            data: { stage: 'transform' },
            name: 'transform',
            opts: { durable: true, jobId: `transform-${suffix}` },
            queueName: transformQueue,
          },
          {
            data: { value: 5 },
            name: 'audit',
            opts: { durable: true, jobId: `audit-${suffix}` },
            queueName: leafQueue,
          },
        ],
        data: { stage: 'aggregate' },
        name: 'aggregate',
        opts: { durable: true, jobId: `aggregate-${suffix}` },
        queueName: rootQueue,
      });

      expect(await root.job.waitUntilFinished(null, 20_000)).toBe(15);
      expect(executions.size).toBe(5);
      expect([...executions.values()].every((count) => count === 1)).toBe(true);

      const transformId = root.children![0].job.id;
      const rootId = root.job.id;
      for (const leaf of root.children![0].children ?? []) {
        expect(effects.indexOf(leaf.job.id)).toBeLessThan(effects.indexOf(transformId));
      }
      expect(effects.indexOf(transformId)).toBeLessThan(effects.indexOf(rootId));
      expect(effects.indexOf(root.children![1].job.id)).toBeLessThan(effects.indexOf(rootId));

      const tree = await producer.getFlow({ depth: 3, id: rootId, queueName: rootQueue });
      expect(tree?.children).toHaveLength(2);
      expect(tree?.children?.[0].children).toHaveLength(2);
      expect(await producer.getParentResult<number>(rootId)).toBe(15);
      expect(
        await producer.getParentResults<number>([transformId, root.children![1].job.id, rootId])
      ).toEqual(
        new Map([
          [transformId, 10],
          [root.children![1].job.id, 5],
          [rootId, 15],
        ])
      );

      const observer = current.queue(rootQueue, 1);
      expect(await observer.getJobState(rootId)).toBe('completed');
      expect(await observer.getChildrenValues<number>(rootId)).toEqual({
        [`${transformQueue}:${transformId}`]: 10,
        [`${leafQueue}:${root.children![1].job.id}`]: 5,
      });
    },
    30_000
  );
});
