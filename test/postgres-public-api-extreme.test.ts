import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { PostgresPublicApiHarness, waitFor } from './support/postgres-public-api-harness';
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

describe(`PostgreSQL ${expectedPostgresVersionPrefix} extreme public API concurrency`, () => {
  test.skipIf(!postgresUrl)(
    'preserves immediate pause/resume read-your-write state under rapid public API control',
    async () => {
      const current = fixture();
      const queue = current.queue(current.unique('extreme-pause-readback'), 0);
      let pauseMisses = 0;
      let resumeMisses = 0;

      for (let iteration = 0; iteration < 128; iteration++) {
        await queue.pauseAsync();
        if (!(await queue.isPausedAsync())) pauseMisses++;
        await queue.resumeAsync();
        if (await queue.isPausedAsync()) resumeMisses++;
      }

      expect({ pauseMisses, resumeMisses }).toEqual({ pauseMisses: 0, resumeMisses: 0 });
    },
    120_000
  );

  test.skipIf(!postgresUrl)(
    'linearizes a 256-request custom-ID collision storm across four brokers',
    async () => {
      const current = fixture();
      const name = current.unique('extreme-id-storm');
      const queues = Array.from({ length: 4 }, (_, broker) =>
        current.queue<{ contender: number }>(name, broker)
      );
      let executions = 0;
      const worker = current.worker<{ contender: number }, number>(name, 2, async (job) => {
        executions++;
        return job.data.contender;
      });
      await worker.waitUntilReady();

      await queues[0].pauseAsync();
      await waitFor('the collision queue to pause globally', () => queues[3].isPausedAsync());
      const jobId = current.unique('extreme-shared-id');
      const jobs = await Promise.all(
        Array.from({ length: 256 }, (_, contender) =>
          queues[contender % queues.length].add(
            'collision',
            { contender },
            { durable: true, jobId }
          )
        )
      );

      expect(jobs.every((job) => job.id === jobId)).toBe(true);
      expect(await queues[1].getJobCountsAsync()).toMatchObject({ paused: 1, waiting: 0 });
      await queues[3].resumeAsync();
      const result = await jobs[127].waitUntilFinished(null, 15_000);

      expect(result).toBeGreaterThanOrEqual(0);
      expect(result).toBeLessThan(256);
      expect(executions).toBe(1);
      expect(await queues[0].getJobState(jobId)).toBe('completed');
    },
    120_000
  );

  test.skipIf(!postgresUrl)(
    'resolves 256 remote waiters after a removeOnComplete transaction',
    async () => {
      const current = fixture();
      const name = current.unique('extreme-waiters');
      const queues = Array.from({ length: 4 }, (_, broker) =>
        current.queue<{ value: number }>(name, broker)
      );
      const started = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const worker = current.worker<{ value: number }, number>(name, 3, async (job) => {
        started.resolve(undefined);
        await release.promise;
        return job.data.value * 11;
      });
      await worker.waitUntilReady();

      const job = await queues[0].add(
        'fanout-waiters',
        { value: 7 },
        { durable: true, removeOnComplete: true }
      );
      await started.promise;
      const remoteJobs = await Promise.all(queues.map((queue) => queue.getJob(job.id)));
      expect(remoteJobs.every(Boolean)).toBe(true);
      const waiters = Array.from({ length: 256 }, (_, index) =>
        remoteJobs[index % remoteJobs.length]!.waitUntilFinished(null, 15_000)
      );
      release.resolve(undefined);

      expect(new Set(await Promise.all(waiters))).toEqual(new Set([77]));
      await waitFor('every broker to observe the removed row', async () => {
        return (await Promise.all(queues.map((queue) => queue.getJobState(job.id)))).every(
          (state) => state === 'unknown'
        );
      });
      expect(await current.flow(2).getParentResult<number>(job.id)).toBe(77);
    },
    120_000
  );

  test.skipIf(!postgresUrl)(
    'executes 32 concurrent eight-way flows exactly once across four brokers',
    async () => {
      const current = fixture();
      const leafQueue = current.unique('extreme-flow-leaf');
      const rootQueue = current.unique('extreme-flow-root');
      const executions = new Map<string, number>();
      const mark = (id: string): void => executions.set(id, (executions.get(id) ?? 0) + 1);
      const leafProcessor = async (job: { id: string; data: { value: number } }) => {
        mark(job.id);
        return job.data.value;
      };
      const rootProcessor = async (job: {
        id: string;
        getChildrenValues<T>(): Promise<Record<string, T>>;
      }) => {
        mark(job.id);
        const values = await job.getChildrenValues<number>();
        return Object.values(values).reduce((sum, value) => sum + value, 0);
      };
      const workers = [
        current.worker(leafQueue, 0, leafProcessor, { concurrency: 24 }),
        current.worker(leafQueue, 1, leafProcessor, { concurrency: 24 }),
        current.worker(rootQueue, 2, rootProcessor, { concurrency: 16 }),
        current.worker(rootQueue, 3, rootProcessor, { concurrency: 16 }),
      ];
      await Promise.all(workers.map((worker) => worker.waitUntilReady()));

      const producer = current.flow(0);
      const batch = crypto.randomUUID();
      const roots = await Promise.all(
        Array.from({ length: 32 }, (_, flow) =>
          producer.add({
            children: Array.from({ length: 8 }, (_, leaf) => ({
              data: { value: leaf + 1 },
              name: 'leaf',
              opts: { durable: true, jobId: `${batch}-flow-${flow}-leaf-${leaf}` },
              queueName: leafQueue,
            })),
            data: { flow },
            name: 'sum',
            opts: { durable: true, jobId: `${batch}-flow-${flow}-root` },
            queueName: rootQueue,
          })
        )
      );
      const results = await Promise.all(
        roots.map(({ job }) => job.waitUntilFinished(null, 90_000))
      );

      expect(new Set(results)).toEqual(new Set([36]));
      expect(executions.size).toBe(32 * 9);
      expect([...executions.values()].every((count) => count === 1)).toBe(true);
      const durable = await producer.getParentResults<number>(roots.map(({ job }) => job.id));
      expect(durable.size).toBe(32);
      expect(new Set(durable.values())).toEqual(new Set([36]));
    },
    180_000
  );

  test.skipIf(!postgresUrl)(
    'recovers an active public Worker job after its broker is SIGKILLed',
    async () => {
      const current = fixture();
      const name = current.unique('extreme-worker-crash');
      const producer = current.queue<{ value: number }>(name, 1);
      const started = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      let victimExecutions = 0;
      let survivorExecutions = 0;
      const victim = current.worker<{ value: number }, string>(
        name,
        0,
        async (job) => {
          victimExecutions++;
          started.resolve(undefined);
          await release.promise;
          return `victim-${job.data.value}`;
        },
        { concurrency: 1, heartbeatInterval: 100, lockDuration: 750 }
      );
      victim.on('error', () => {});
      await victim.waitUntilReady();

      const job = await producer.add('crash-active', { value: 9 }, { attempts: 3, durable: true });
      await started.promise;
      await current.crashBroker(0);
      await victim.close(true);
      release.resolve(undefined);

      const survivors = [2, 3].map((broker) =>
        current.worker<{ value: number }, string>(
          name,
          broker,
          async (recovered) => {
            survivorExecutions++;
            return `survivor-${recovered.data.value}`;
          },
          { concurrency: 2, heartbeatInterval: 100, lockDuration: 750 }
        )
      );
      await Promise.all(survivors.map((worker) => worker.waitUntilReady()));

      expect(await job.waitUntilFinished(null, 60_000)).toBe('survivor-9');
      expect(victimExecutions).toBe(1);
      expect(survivorExecutions).toBe(1);
      expect(await producer.getJobState(job.id)).toBe('completed');
      expect((await producer.getJob(job.id))?.returnvalue).toBe('survivor-9');
    },
    120_000
  );
});
