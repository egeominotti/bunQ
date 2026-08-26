import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { FailureReason } from '../src/domain/types/dlq';
import { createJob, generateJobId, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-features-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

async function waitUntilCronDue(store: PostgresQueueStore, name: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    const cron = await store.getCron(name);
    if (!cron) throw new Error(`Missing PostgreSQL cron ${name}`);
    const now = await store.now();
    if (now >= cron.nextRun) return;
    await Bun.sleep(Math.max(1, Math.min(10, cron.nextRun - now)));
  }
  throw new Error(`PostgreSQL cron ${name} did not become due`);
}

async function cleanup(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    await sql.begin(async (tx) => {
      await tx`DELETE FROM bunqueue_metric_buckets WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_metric_totals WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_workers WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_crons WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_job_logs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_repeat_links WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_flow_failures WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_dependencies WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_completions WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_jobs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_queue_state WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_prune_watermarks WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_events WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_commits WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_brokers WHERE namespace = ${value}`;
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(postgresUrl, value);
});

describe('PostgreSQL distributed queue features', () => {
  test.skipIf(!postgresUrl)(
    'coordinates priority, dependencies, groups, idempotency and terminal results',
    async () => {
      const value = namespace('scheduling');
      const a = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'a' });
      const b = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'b' });
      try {
        await Promise.all([a.initialize(), b.initialize()]);
        const dependency = createJob(generateJobId(), 'scheduling', {
          data: { role: 'dependency' },
        });
        const blocked = createJob(generateJobId(), 'scheduling', {
          data: { role: 'blocked' },
          dependsOn: [dependency.id],
        });
        const low = createJob(generateJobId(), 'scheduling', {
          data: { role: 'low' },
          priority: 1,
        });
        const high = createJob(generateJobId(), 'scheduling', {
          data: { role: 'high' },
          priority: 100,
        });
        await a.insertMany([dependency, blocked, low, high]);

        const first = (await b.claim('scheduling', 1))[0];
        expect(first.job.id).toBe(high.id);
        await b.complete(first.job.id, first.token, 'high-result');
        const ready = await a.claim('scheduling', 10);
        expect(ready.map(({ job }) => job.id)).toContain(dependency.id);
        expect(ready.map(({ job }) => job.id)).not.toContain(blocked.id);
        const dependencyClaim = ready.find(({ job }) => job.id === dependency.id)!;
        await a.complete(dependency.id, dependencyClaim.token, { dependency: true });
        const dependentClaim = (await b.claim('scheduling', 1))[0];
        expect(dependentClaim.job.id).toBe(blocked.id);
        await b.complete(blocked.id, dependentClaim.token, { blocked: false });
        expect(await a.getChildrenValues(blocked.id)).toEqual({
          [`scheduling:${dependency.id}`]: { dependency: true },
        });

        const unique = createJob(generateJobId(), 'idempotency', {
          data: { generation: 1 },
          uniqueKey: 'unique-key',
        });
        const duplicate = createJob(generateJobId(), 'idempotency', {
          data: { generation: 2 },
          uniqueKey: 'unique-key',
        });
        expect((await a.insert(unique)).inserted).toBe(true);
        const duplicateResult = await b.insert(duplicate);
        expect(duplicateResult.inserted).toBe(false);
        expect(duplicateResult.job.id).toBe(unique.id);

        const custom = createJob(jobId('stable-id'), 'idempotency', { data: { value: 1 } });
        expect((await a.insert(custom)).inserted).toBe(true);
        expect(
          (await b.insert(createJob(jobId('stable-id'), 'idempotency', { data: { value: 2 } })))
            .inserted
        ).toBe(false);

        const groupJobs = [1, 2].map((index) =>
          createJob(generateJobId(), 'groups', { data: { index }, groupId: 'serial' })
        );
        await a.insertMany(groupJobs);
        const grouped = await Promise.all([a.claim('groups', 1), b.claim('groups', 1)]);
        expect(grouped.flat()).toHaveLength(1);
        const held = grouped.flat()[0];
        await a.complete(held.job.id, held.token);
        expect(await b.claim('groups', 1)).toHaveLength(1);
      } finally {
        await Promise.allSettled([a.close(), b.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'enforces shared pause, concurrency, rate and DLQ retry state',
    async () => {
      const value = namespace('limits');
      const a = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'a' });
      const b = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'b' });
      try {
        await Promise.all([a.initialize(), b.initialize()]);
        await a.insertMany(
          [1, 2].map((index) => createJob(generateJobId(), 'limited', { data: { index } }))
        );
        await a.setConcurrency('limited', 1);
        const held = (await a.claim('limited', 1))[0];
        expect(await b.claim('limited', 1)).toHaveLength(0);
        await a.complete(held.job.id, held.token);
        const second = (await b.claim('limited', 1))[0];
        expect(second).toBeDefined();
        await b.complete(second.job.id, second.token);

        await a.insertMany(
          [1, 2].map((index) => createJob(generateJobId(), 'rated', { data: { index } }))
        );
        await a.setRateLimit('rated', 1, 10_000, 60_000);
        const [databaseClock] = await a.context.sql<{ now_ms: number | string | bigint }[]>`
          SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
        `;
        expect((await a.getQueueState('rated')).rateExpiresAt).toBeGreaterThan(
          Number(databaseClock.now_ms)
        );
        const rated = (await a.claim('rated', 1))[0];
        await a.complete(rated.job.id, rated.token);
        expect(await b.claim('rated', 1)).toHaveLength(0);
        await a.context.sql`
          UPDATE bunqueue_queue_state
          SET rate_expires_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint - 1
          WHERE namespace = ${value} AND queue = 'rated'
        `;
        const afterRateTtl = (await b.claim('rated', 1))[0];
        expect(afterRateTtl).toBeDefined();
        expect((await b.getQueueState('rated')).rateLimit).toBeNull();
        await b.complete(afterRateTtl.job.id, afterRateTtl.token);

        await a.insert(createJob(generateJobId(), 'paused', { data: {} }));
        await a.pause('paused', true);
        expect(await b.claim('paused', 1)).toHaveLength(0);
        await b.pause('paused', false);
        expect(await b.claim('paused', 1)).toHaveLength(1);

        const databaseNow = await a.now();
        const expiring = await a.insert(
          createJob(generateJobId(), 'expiring', {
            data: {},
            ttl: 20,
            timestamp: databaseNow - 21,
          })
        );
        expect(await b.claim('expiring', 1)).toHaveLength(0);
        expect(await a.getJob(expiring.job.id)).toBeNull();

        const failing = await a.insert(
          createJob(generateJobId(), 'failures', { data: {}, maxAttempts: 2, backoff: 0 })
        );
        const attemptOne = (await a.claim('failures', 1))[0];
        expect(
          (await a.fail({ id: attemptOne.job.id, token: attemptOne.token, error: 'one' })).state
        ).toBe('waiting');
        const attemptTwo = (await b.claim('failures', 1))[0];
        expect(
          (await b.fail({ id: attemptTwo.job.id, token: attemptTwo.token, error: 'two' })).state
        ).toBe('failed');
        expect((await a.getJob(failing.job.id))?.state).toBe('failed');
        expect(await a.getDlq('failures')).toHaveLength(1);

        await a.insert(
          createJob(generateJobId(), 'timeouts', {
            data: {},
            maxAttempts: 1,
            timeout: 30,
          })
        );
        await a.claim('timeouts', 1);
        await Bun.sleep(40);
        expect(await b.recoverExpired()).toBeGreaterThanOrEqual(1);
        expect((await b.getDlq('timeouts'))[0]?.reason).toBe(FailureReason.Timeout);
      } finally {
        await Promise.allSettled([a.close(), b.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'coordinates bounded DLQ eviction and automatic retries across brokers',
    async () => {
      const value = namespace('dlq-lifecycle');
      const a = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'a' });
      const b = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'b' });
      try {
        await Promise.all([a.initialize(), b.initialize()]);
        await a.setDlqConfig('bounded', {
          autoRetry: true,
          autoRetryInterval: 25,
          maxAutoRetries: 2,
          maxAge: null,
          maxEntries: 2,
        });
        const failedIds: string[] = [];
        for (let index = 0; index < 3; index++) {
          const admitted = await a.insert(
            createJob(generateJobId(), 'bounded', {
              data: { index },
              maxAttempts: 1,
            })
          );
          failedIds.push(admitted.job.id);
          const claim = (await b.claim('bounded', 1))[0];
          await b.fail({ id: claim.job.id, token: claim.token, error: `failure-${index}` });
          await Bun.sleep(2);
        }
        expect(await a.getJob(failedIds[0] as ReturnType<typeof jobId>)).toBeNull();
        expect(await a.getDlq('bounded')).toHaveLength(2);

        await Bun.sleep(30);
        const maintained = await Promise.all([a.maintainDlq(), b.maintainDlq()]);
        expect(maintained.reduce((total, result) => total + result.retried, 0)).toBe(2);
        const retried = await a.claim('bounded', 2);
        expect(retried).toHaveLength(2);
        await a.fail({
          id: retried[0].job.id,
          token: retried[0].token,
          error: 'failed-after-auto-retry',
        });
        const entry = (await b.getDlq('bounded')).find(
          (candidate) => candidate.job.id === retried[0].job.id
        );
        expect(entry?.retryCount).toBe(1);
        expect(entry?.nextRetryAt).not.toBeNull();
      } finally {
        await Promise.allSettled([a.close(), b.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'does not consume a cron execution limit while preventOverlap skips a slot',
    async () => {
      const value = namespace('cron-overlap');
      const a = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'a',
        pollIntervalMs: 60_000,
      });
      const b = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'b',
        pollIntervalMs: 60_000,
      });
      try {
        await Promise.all([a.initialize(), b.initialize()]);
        await a.addCron({
          name: 'bounded-overlap',
          queue: 'cron-overlap',
          data: { scheduled: true },
          repeatEvery: 50,
          immediately: true,
          preventOverlap: true,
          maxLimit: 2,
        });
        expect(await a.processCrons()).toBe(1);
        const held = (await a.claim('cron-overlap', 1))[0];
        expect(held).toBeDefined();

        await waitUntilCronDue(a, 'bounded-overlap');
        const skipped = await Promise.all([a.processCrons(), b.processCrons()]);
        expect(skipped.reduce((sum, count) => sum + count, 0)).toBe(0);
        expect((await b.getCron('bounded-overlap'))?.executions).toBe(1);

        await a.complete(held.job.id, held.token);
        await waitUntilCronDue(b, 'bounded-overlap');
        const fired = await Promise.all([a.processCrons(), b.processCrons()]);
        expect(fired.reduce((sum, count) => sum + count, 0)).toBe(1);
        expect((await a.getCron('bounded-overlap'))?.executions).toBe(2);
      } finally {
        await Promise.allSettled([a.close(), b.close()]);
      }
    }
  );
});
