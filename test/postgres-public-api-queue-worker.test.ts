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

describe(`PostgreSQL ${expectedPostgresVersionPrefix} public Queue and Worker APIs`, () => {
  test.skipIf(!postgresUrl)(
    'shares Queue, Worker, QueueEvents, progress, results, and logs across four brokers',
    async () => {
      const current = fixture();
      const name = current.unique('public-lifecycle');
      const producer = current.queue<{ value: number }>(name, 0);
      const observer = current.queue<{ value: number }>(name, 3);
      const events = current.queueEvents<number, { phase: string }>(name, 2);
      const executions = new Map<string, number>();
      const waiting = new Set<string>();
      const completed = new Map<string, number>();
      const progressed = new Set<string>();

      events.on('waiting', ({ jobId }) => waiting.add(jobId));
      events.on('completed', ({ jobId, returnvalue }) => completed.set(jobId, returnvalue));
      events.on('progress', ({ jobId }) => progressed.add(jobId));
      await events.waitUntilReady();

      const worker = current.worker<{ value: number }, number>(
        name,
        1,
        async (job) => {
          executions.set(job.id, (executions.get(job.id) ?? 0) + 1);
          await job.updateProgress({ phase: 'running' });
          await job.log(`processed-${job.data.value}`);
          return job.data.value * 2;
        },
        { concurrency: 8 }
      );
      await worker.waitUntilReady();

      const jobs = await producer.addBulk(
        Array.from({ length: 24 }, (_, value) => ({
          data: { value },
          name: 'double',
          opts: { durable: true, jobId: `${name}-${value}` },
        }))
      );
      const ids = new Set(jobs.map((job) => job.id));

      await waitFor('all public API jobs and cross-broker events to complete', async () => {
        const counts = await observer.getJobCountsAsync();
        return counts.completed === jobs.length && completed.size === jobs.length;
      });

      expect(waiting).toEqual(ids);
      expect(progressed).toEqual(ids);
      expect(new Set(completed.keys())).toEqual(ids);
      expect(executions.size).toBe(jobs.length);
      expect([...executions.values()].every((count) => count === 1)).toBe(true);
      expect(await jobs[7].waitUntilFinished(events, 5_000)).toBe(14);
      expect(await observer.getJobState(jobs[7].id)).toBe('completed');
      expect(await observer.getJobCountsAsync()).toMatchObject({
        active: 0,
        completed: jobs.length,
        failed: 0,
        waiting: 0,
      });

      let retained = await observer.getJobsAsync({ asc: true, end: -1, state: 'completed' });
      await waitFor('the observer completed-job projection to converge', async () => {
        retained = await observer.getJobsAsync({ asc: true, end: -1, state: 'completed' });
        return retained.length === jobs.length;
      });
      expect(new Set(retained.map((job) => job.id))).toEqual(ids);
      expect(retained.find((job) => job.id === jobs[7].id)?.returnvalue).toBe(14);
      expect(await observer.getJobLogs(jobs[7].id)).toMatchObject({
        count: 1,
        logs: ['[info] processed-7'],
      });
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'keeps pause, custom-ID idempotency, failure, DLQ inspection, and retry global',
    async () => {
      const current = fixture();
      const name = current.unique('public-control');
      const first = current.queue<{ value: number; fail?: boolean }>(name, 0);
      const second = current.queue<{ value: number; fail?: boolean }>(name, 3);
      const attempts = new Map<string, number>();
      let permitRetry = false;
      const worker = current.worker<{ value: number; fail?: boolean }, number>(
        name,
        1,
        async (job) => {
          attempts.set(job.id, (attempts.get(job.id) ?? 0) + 1);
          if (job.data.fail && !permitRetry) throw new Error('expected public API failure');
          return job.data.value;
        }
      );
      await worker.waitUntilReady();

      await first.pauseAsync();
      await waitFor('the paused state to reach the fourth broker', async () => {
        return await second.isPausedAsync();
      });
      const customId = current.unique('shared-custom-id');
      const [left, right] = await Promise.all([
        first.add('idempotent', { value: 1 }, { durable: true, jobId: customId }),
        second.add('idempotent', { value: 2 }, { durable: true, jobId: customId }),
      ]);
      expect(left.id).toBe(customId);
      expect(right.id).toBe(customId);
      expect((await second.getJobCountsAsync()).paused).toBe(1);

      await second.resumeAsync();
      await waitFor('the idempotent job to complete', async () => {
        return (await first.getJobState(customId)) === 'completed';
      });
      expect(attempts.get(customId)).toBe(1);

      const failed = await first.add(
        'fails-once',
        { fail: true, value: 42 },
        { attempts: 1, durable: true }
      );
      await waitFor('the failed job to enter the shared DLQ', async () => {
        return (await second.getJobState(failed.id)) === 'failed';
      });
      expect((await second.getDlqAsync()).map((entry) => entry.job.id)).toEqual([failed.id]);
      expect(await second.getDlqStatsAsync()).toMatchObject({ total: 1 });

      permitRetry = true;
      expect(await second.retryDlqAsync(failed.id)).toBe(1);
      await waitFor('the DLQ retry to complete on another broker', async () => {
        return (await first.getJobState(failed.id)) === 'completed';
      });
      expect(attempts.get(failed.id)).toBe(2);
      expect(await first.getDlqAsync()).toEqual([]);
      expect(await failed.waitUntilFinished(null, 5_000)).toBe(42);
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'returns a remote completion result after removeOnComplete deletes the job row',
    async () => {
      const current = fixture();
      const name = current.unique('public-removed-result');
      const queue = current.queue<{ value: number }>(name, 0);
      const started = Promise.withResolvers<undefined>();
      const release = Promise.withResolvers<undefined>();
      const worker = current.worker<{ value: number }, number>(name, 3, async (job) => {
        started.resolve(undefined);
        await release.promise;
        return job.data.value * 3;
      });
      await worker.waitUntilReady();

      const job = await queue.add(
        'removed-result',
        { value: 9 },
        { durable: true, removeOnComplete: true }
      );
      await started.promise;
      const waiting = job.waitUntilFinished(null, 5_000);
      release.resolve(undefined);

      expect(await waiting).toBe(27);
      expect(await queue.getJobState(job.id)).toBe('unknown');
      expect(await current.flow(2).getParentResult<number>(job.id)).toBe(27);
    },
    15_000
  );
});
