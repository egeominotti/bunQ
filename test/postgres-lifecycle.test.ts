import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { createJob, generateJobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-lifecycle-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
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

describe('PostgreSQL lifecycle parity', () => {
  test.skipIf(!postgresUrl)(
    'supports TTL, extend, and replace deduplication atomically',
    async () => {
      const value = namespace('dedup');
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'dedup-broker',
      });
      try {
        await store.initialize();
        const expiring = createJob(generateJobId(), 'ttl', {
          data: { generation: 1 },
          uniqueKey: 'ttl-key',
          dedup: { ttl: 60_000 },
        });
        expect((await store.insert(expiring)).inserted).toBe(true);
        expect(
          (
            await store.insert(
              createJob(generateJobId(), 'ttl', {
                data: { generation: 2 },
                uniqueKey: 'ttl-key',
              })
            )
          ).job.id
        ).toBe(expiring.id);
        await store.context.sql`
          UPDATE bunqueue_jobs
          SET unique_expires_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint - 1
          WHERE namespace = ${value} AND id = ${String(expiring.id)}
        `;
        const afterTtl = await store.insert(
          createJob(generateJobId(), 'ttl', { data: { generation: 3 }, uniqueKey: 'ttl-key' })
        );
        expect(afterTtl.inserted).toBe(true);
        expect(afterTtl.job.id).not.toBe(expiring.id);

        const extend = createJob(generateJobId(), 'extend', {
          data: { generation: 1 },
          uniqueKey: 'extend-key',
          dedup: { ttl: 60_000 },
        });
        await store.insert(extend);
        const [beforeExtend] = await store.context.sql<
          { unique_expires_at: number | string | bigint }[]
        >`
          SELECT unique_expires_at FROM bunqueue_jobs
          WHERE namespace = ${value} AND id = ${String(extend.id)}
        `;
        const extended = await store.insert(
          createJob(generateJobId(), 'extend', {
            data: { generation: 2 },
            uniqueKey: 'extend-key',
            dedup: { ttl: 120_000, extend: true },
          })
        );
        expect(extended.inserted).toBe(false);
        expect(extended.job.id).toBe(extend.id);
        expect(extended.job.deduplicationTtl).toBe(120_000);
        const [afterExtend] = await store.context.sql<
          { unique_expires_at: number | string | bigint }[]
        >`
          SELECT unique_expires_at FROM bunqueue_jobs
          WHERE namespace = ${value} AND id = ${String(extend.id)}
        `;
        expect(Number(afterExtend.unique_expires_at)).toBeGreaterThan(
          Number(beforeExtend.unique_expires_at)
        );
        expect(
          (
            await store.insert(
              createJob(generateJobId(), 'extend', {
                data: { generation: 3 },
                uniqueKey: 'extend-key',
              })
            )
          ).job.id
        ).toBe(extend.id);

        const replaced = createJob(generateJobId(), 'replace', {
          data: { generation: 1 },
          uniqueKey: 'replace-key',
        });
        await store.insert(replaced);
        const replacement = await store.insert(
          createJob(generateJobId(), 'replace', {
            data: { generation: 2 },
            uniqueKey: 'replace-key',
            dedup: { replace: true },
          })
        );
        expect(replacement.inserted).toBe(true);
        expect(await store.getJob(replaced.id)).toBeNull();

        const active = createJob(generateJobId(), 'active-replace', {
          data: { generation: 1 },
          uniqueKey: 'active-key',
        });
        await store.insert(active);
        const activeClaim = (await store.claim('active-replace', 1))[0];
        const successor = await store.insert(
          createJob(generateJobId(), 'active-replace', {
            data: { generation: 2 },
            uniqueKey: 'active-key',
            dedup: { replace: true },
          })
        );
        expect(successor.inserted).toBe(true);
        expect((await store.complete(active.id, activeClaim.token)).applied).toBe(true);
        expect((await store.claim('active-replace', 1))[0].job.id).toBe(successor.job.id);
      } finally {
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'releases explicit worker leases when their broker shuts down',
    async () => {
      const value = namespace('shutdown');
      const first = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'shutdown-a',
        leaseDurationMs: 30_000,
      });
      const second = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'shutdown-b',
        leaseDurationMs: 30_000,
      });
      let firstClosed = false;
      try {
        await Promise.all([first.initialize(), second.initialize()]);
        const admitted = await first.insert(
          createJob(generateJobId(), 'shutdown', { data: { value: 1 } })
        );
        expect((await first.claim('shutdown', 1, 'worker-on-a'))[0].job.id).toBe(admitted.job.id);
        await first.close();
        firstClosed = true;
        const reclaimed = (await second.claim('shutdown', 1, 'worker-on-b'))[0];
        expect(reclaimed.job.id).toBe(admitted.job.id);
        expect((await second.complete(reclaimed.job.id, reclaimed.token)).applied).toBe(true);
      } finally {
        if (!firstClosed) await first.close();
        await second.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps a remotely renewed lease fenced through disconnect and broker shutdown',
    async () => {
      const value = namespace('remote-heartbeat');
      const first = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'remote-heartbeat-a',
          leaseDurationMs: 30_000,
        },
      });
      const second = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'remote-heartbeat-b',
          leaseDurationMs: 30_000,
        },
      });
      let firstClosed = false;
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const admitted = await first.push('remote-heartbeat', { data: { value: 1 } });
        const claimed = await first.pullWithLock('remote-heartbeat', 'pooled-worker');
        expect(claimed.job?.id).toBe(admitted.id);
        first.registerClientJob('pull-connection', admitted.id);
        expect(await second.extendLock(admitted.id, claimed.token, 30_000)).toBe(true);
        expect(await first.releaseClientJobs('pull-connection')).toBe(0);

        await first.shutdownPostgres();
        firstClosed = true;
        expect(await second.pull('remote-heartbeat', 80)).toBeNull();
        await second.ack(admitted.id, 'done', claimed.token ?? undefined);
        expect(await second.getJobState(admitted.id)).toBe('completed');
      } finally {
        if (!firstClosed) await first.shutdownPostgres();
        await second.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'reconciles missed cron schedules and discards protected cron leases on shutdown',
    async () => {
      const value = namespace('cron-restart');
      const setup = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'cron-setup',
      });
      await setup.initialize();
      await setup.addCron({
        name: 'skip-missed',
        queue: 'cron-skip',
        data: { skipped: true },
        repeatEvery: 60_000,
        skipMissedOnRestart: true,
      });
      await setup.addCron({
        name: 'catch-up-once',
        queue: 'cron-catch-up',
        data: { catchUp: true },
        repeatEvery: 60_000,
        skipMissedOnRestart: false,
      });
      await setup.close();

      const sql = new SQL(postgresUrl!, { max: 2 });
      try {
        await sql`
          UPDATE bunqueue_crons
          SET next_run = ${Date.now() - 120_000}
          WHERE namespace = ${value}
        `;
      } finally {
        await sql.close({ timeout: 5 });
      }

      const first = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'cron-runtime-a',
          pollIntervalMs: 25,
        },
      });
      const second = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'cron-runtime-b',
          pollIntervalMs: 25,
        },
      });
      let firstClosed = false;
      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        expect((await second.getCronDurable('skip-missed'))?.nextRun).toBeGreaterThan(Date.now());
        expect(await second.pull('cron-skip', 80)).toBeNull();

        const catchUp = await second.pullWithLock('cron-catch-up', 'cron-worker', 1000);
        expect(catchUp.job?.data).toEqual({ catchUp: true });
        await second.ack(catchUp.job!.id, undefined, catchUp.token ?? undefined);
        expect(await first.pull('cron-catch-up', 100)).toBeNull();
        expect((await first.getCronDurable('catch-up-once'))?.executions).toBe(1);
        expect((await first.getCronDurable('catch-up-once'))?.nextRun).toBeGreaterThan(Date.now());

        const protectedCron = await first.addCronDurable({
          name: 'protected-on-shutdown',
          queue: 'cron-protected',
          data: { protected: true },
          repeatEvery: 60_000,
          immediately: true,
        });
        expect(protectedCron.preventOverlap).toBe(true);
        const protectedClaim = await first.pullWithLock('cron-protected', 'cron-worker', 1000);
        expect(protectedClaim.job).not.toBeNull();
        const protectedId = protectedClaim.job!.id;
        await first.shutdownPostgres();
        firstClosed = true;
        expect(await second.getJobState(protectedId)).toBe('unknown');
        expect(await second.pull('cron-protected', 100)).toBeNull();
      } finally {
        if (!firstClosed) await first.shutdownPostgres();
        await second.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps cancel, discard, progress, and parent links state-safe',
    async () => {
      const manager = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: namespace('manager'),
          brokerId: 'manager-a',
          pollIntervalMs: 25,
        },
      });
      try {
        await manager.waitUntilReady();
        const completed = await manager.push('operations', { data: { value: 1 } });
        expect(await manager.updateProgress(completed.id, 10)).toBe(false);
        const claimed = await manager.pullWithLock('operations', 'worker');
        expect(await manager.updateProgress(completed.id, 25, 'running')).toBe(true);
        await manager.ack(completed.id, 'done', claimed.token!);
        expect(await manager.cancel(completed.id)).toBe(false);
        expect(await manager.updateJobData(completed.id, { value: 2 })).toBe(false);
        expect(await manager.getJobState(completed.id)).toBe('completed');

        const discarded = await manager.push('operations', { data: { discard: true } });
        expect(await manager.discard(discarded.id)).toBe(true);
        expect(await manager.getJobState(discarded.id)).toBe('failed');
        expect(manager.getDlqEntries('operations').map((entry) => entry.job.id)).toContain(
          discarded.id
        );

        const parent = await manager.push('relationships', { data: { role: 'parent' } });
        const child = await manager.push('relationships', { data: { role: 'child' } });
        await manager.updateJobParent(child.id, parent.id);
        expect(await manager.getJobState(parent.id)).toBe('waiting-children');
        expect((await manager.getJob(child.id))?.parentId).toBe(parent.id);
        expect(await manager.removeChildDependency(child.id)).toBe(true);
        expect((await manager.getJob(child.id))?.parentId).toBeNull();
        expect(await manager.getJobState(parent.id)).toBe('waiting');
      } finally {
        await manager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)('applies every terminal child failure policy durably', async () => {
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: namespace('flow-failures'),
        brokerId: 'flow-failures-a',
      },
    });
    try {
      await manager.waitUntilReady();
      for (const mode of ['fail', 'remove', 'ignore', 'continue'] as const) {
        const queue = `flow-${mode}`;
        const parent = await manager.push(queue, { data: { role: 'parent' } });
        const child = await manager.push(queue, {
          data: {
            role: 'child',
            __parentId: String(parent.id),
            __parentQueue: queue,
          },
          parentId: parent.id,
          maxAttempts: 1,
          ...(mode === 'fail' && { failParentOnFailure: true }),
          ...(mode === 'remove' && { removeDependencyOnFailure: true }),
          ...(mode === 'ignore' && { ignoreDependencyOnFailure: true }),
          ...(mode === 'continue' && { continueParentOnFailure: true }),
        });
        await manager.updateJobParent(child.id, parent.id);
        const claim = await manager.pullWithLock(queue, `worker-${mode}`);
        expect(claim.job?.id).toBe(child.id);
        await manager.fail(child.id, `${mode}-error`, claim.token!, true);

        expect(await manager.getJobState(parent.id)).toBe(mode === 'fail' ? 'failed' : 'waiting');
        const key = `${queue}:${String(child.id)}`;
        expect(await manager.getFailedChildrenValues(parent.id)).toEqual(
          mode === 'continue' ? { [key]: `${mode}-error` } : {}
        );
        expect(await manager.getIgnoredChildrenFailures(parent.id)).toEqual(
          mode === 'ignore' ? { [key]: `${mode}-error` } : {}
        );
      }
    } finally {
      await manager.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)('creates one durable repeat successor across brokers', async () => {
    const value = namespace('repeat');
    const a = new PostgresQueueManager({
      postgres: { url: postgresUrl!, namespace: value, brokerId: 'repeat-a' },
    });
    const b = new PostgresQueueManager({
      postgres: { url: postgresUrl!, namespace: value, brokerId: 'repeat-b' },
    });
    try {
      await Promise.all([a.waitUntilReady(), b.waitUntilReady()]);
      const original = await a.push('repeat', {
        data: { generation: 1 },
        repeat: { every: 30, limit: 1 },
      });
      const claim = await a.pullWithLock('repeat', 'repeat-worker');
      const outcomes = await Promise.all([
        a.ack(original.id, 'done', claim.token!),
        b.ack(original.id, 'duplicate', claim.token!),
      ]);
      expect(outcomes.filter((outcome) => outcome === undefined)).toHaveLength(1);
      expect(await b.updateJobData(original.id, { generation: 2 })).toBe(true);

      const successor = await b.pullWithLock('repeat', 'repeat-worker-b', 1000);
      expect(successor.job?.id).not.toBe(original.id);
      expect(successor.job?.data).toEqual({ generation: 2 });
      expect(successor.job?.repeat?.count).toBe(1);
      await b.ack(successor.job!.id, 'done-again', successor.token!);
      expect(await a.pull('repeat', 80)).toBeNull();
    } finally {
      await Promise.allSettled([a.shutdownPostgres(), b.shutdownPostgres()]);
    }
  });
});
