import { SQL } from 'bun';
import { afterAll, describe, expect, test } from 'bun:test';
import { createJob, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { prunePostgresEventCommits } from '../src/infrastructure/persistence/postgres/eventJournal';
import { PostgresEventCommitGc } from '../src/infrastructure/persistence/postgres/eventCommitGc';
import { resolvePostgresRuntimeConfig } from '../src/infrastructure/persistence/postgres/runtimeConfig';
import { heartbeatPostgresBroker } from '../src/infrastructure/persistence/postgres/brokerSessions';
import { reconcilePostgresCronsOnStartup } from '../src/infrastructure/persistence/postgres/crons';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-production-hardening-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function store(
  value: string,
  brokerId: string,
  options: Record<string, unknown> = {}
): PostgresQueueStore {
  return new PostgresQueueStore({
    url: postgresUrl!,
    namespace: value,
    brokerId,
    ...options,
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL production hardening regressions', () => {
  test('accelerates commit journal GC while a backlog remains', async () => {
    let attempts = 0;
    const gc = new PostgresEventCommitGc(
      async () => (++attempts === 1 ? 4 : 0),
      () => undefined,
      { batchSize: 2, maxBatches: 2, idleDelayMs: 1, backlogDelayMs: 1 }
    );
    gc.start();
    const deadline = Date.now() + 250;
    while (attempts < 2 && Date.now() < deadline) await Bun.sleep(2);
    gc.close();
    await gc.drain();
    expect(attempts).toBeGreaterThanOrEqual(2);
  });

  test('normalizes every non-finite direct runtime option', () => {
    const resolved = resolvePostgresRuntimeConfig({
      url: 'postgres://bunqueue:test@localhost:5432/bunqueue',
      poolSize: 20,
      leaseDurationMs: Number.POSITIVE_INFINITY,
      pollIntervalMs: Number.NaN,
      maxQueueEvents: Number.POSITIVE_INFINITY,
      maxMetricDataPoints: Number.NaN,
      maxCompletedJobs: Number.POSITIVE_INFINITY,
      maxJobResults: Number.NaN,
      maxConcurrentOperations: Number.POSITIVE_INFINITY,
      maxQueuedOperations: Number.NaN,
    });

    expect(resolved).toMatchObject({
      poolSize: 20,
      leaseDurationMs: 30_000,
      pollIntervalMs: 250,
      maxQueueEvents: 10_000,
      maxMetricDataPoints: 20_160,
      maxCompletedJobs: 50_000,
      maxJobResults: 10_000,
      maxConcurrentOperations: 16,
      maxQueuedOperations: 128,
    });
    expect(resolvePostgresRuntimeConfig({ url: resolved.url, poolSize: Number.NaN }).poolSize).toBe(
      4
    );
  });

  test.skipIf(!postgresUrl)('rejects a live duplicate broker identity', async () => {
    const value = namespace('duplicate-broker');
    const first = store(value, 'duplicate');
    const second = store(value, 'duplicate');
    try {
      await first.initialize();
      await first.insert(createJob(jobId('duplicate-job'), 'duplicate', { data: {} }));
      const [claim] = await first.claim('duplicate', 1, 'duplicate-worker', 60_000);
      await expect(second.initialize()).rejects.toThrow('brokerId "duplicate" is already active');
      await expect(second.close()).rejects.toThrow('brokerId "duplicate" is already active');
      expect((await first.getJob(claim.job.id))?.state).toBe('active');
      expect((await first.complete(claim.job.id, claim.token)).applied).toBe(true);
    } finally {
      await Promise.allSettled([first.close(), second.close()]);
    }
  });

  test.skipIf(!postgresUrl)('fences a stale broker after session takeover', async () => {
    const value = namespace('broker-takeover');
    const first = store(value, 'takeover', { leaseDurationMs: 1_000 });
    const second = store(value, 'takeover', { leaseDurationMs: 1_000 });
    try {
      await first.initialize();
      await first.insert(createJob(jobId('old-session-job'), 'takeover', { data: {} }));
      const [oldClaim] = await first.claim('takeover', 1, 'old-worker', 60_000);
      await first.context.sql`
        UPDATE bunqueue_brokers SET heartbeat_at = 0
        WHERE namespace = ${value} AND broker_id = 'takeover'
      `;

      await second.initialize();
      await expect(heartbeatPostgresBroker(first.context)).rejects.toThrow('has been fenced');
      await expect(first.claim('takeover', 1, 'zombie-worker', 60_000)).rejects.toThrow(
        'has been fenced'
      );

      await second.insert(createJob(jobId('new-session-job'), 'takeover-new', { data: {} }));
      const [newClaim] = await second.claim('takeover-new', 1, 'new-worker', 60_000);
      await first.close();
      expect((await second.getJob(oldClaim.job.id))?.state).toBe('waiting');
      expect((await second.getJob(newClaim.job.id))?.state).toBe('active');
      expect((await second.complete(newClaim.job.id, newClaim.token)).applied).toBe(true);
    } finally {
      await Promise.allSettled([first.close(), second.close()]);
    }
  });

  test.skipIf(!postgresUrl)('enforces PostgreSQL statement and lock deadlines', async () => {
    const value = namespace('sql-deadline');
    const queueStore = store(value, 'sql-deadline', {
      statementTimeoutMs: 200,
      lockTimeoutMs: 100,
    });
    const blocker = new SQL(postgresUrl!, { max: 1 });
    try {
      await queueStore.initialize();
      await queueStore.setConcurrency('locked', 1);
      const holding = blocker.begin(async (tx) => {
        await tx`
          SELECT 1 FROM bunqueue_queue_state
          WHERE namespace = ${value} AND queue = 'locked'
          FOR UPDATE
        `;
        await Bun.sleep(750);
      });
      await Bun.sleep(50);
      const startedAt = performance.now();
      await expect(queueStore.setConcurrency('locked', 2)).rejects.toThrow();
      expect(performance.now() - startedAt).toBeLessThan(600);
      await holding;
      await expect(queueStore.setConcurrency('locked', 3)).resolves.toBeUndefined();
    } finally {
      await Promise.allSettled([queueStore.close(), blocker.close({ timeout: 1 })]);
    }
  });

  test.skipIf(!postgresUrl)(
    'elects one cron reconciler after simultaneous broker startup',
    async () => {
      const value = namespace('cron-startup-election');
      const first = store(value, 'cron-election-a');
      const second = store(value, 'cron-election-b');
      try {
        await Promise.all([first.initialize(), second.initialize()]);
        await first.addCron({
          name: 'skip-missed-election',
          queue: 'cron-election',
          data: {},
          repeatEvery: 60_000,
          skipMissedOnRestart: true,
        });
        await first.context.sql`
        UPDATE bunqueue_crons SET next_run = 0
        WHERE namespace = ${value} AND name = 'skip-missed-election'
      `;

        const reconciled = await Promise.all([
          reconcilePostgresCronsOnStartup(first.context),
          reconcilePostgresCronsOnStartup(second.context),
        ]);
        expect(reconciled.reduce((total, count) => total + count, 0)).toBe(1);
        expect((await first.getCron('skip-missed-election'))?.nextRun).toBeGreaterThan(Date.now());
      } finally {
        await Promise.allSettled([first.close(), second.close()]);
      }
    }
  );

  test.skipIf(!postgresUrl)('fails safely before an oversized manager snapshot', async () => {
    const value = namespace('snapshot-budget');
    const queueStore = store(value, 'snapshot-budget', { maxSnapshotJobs: 1 });
    try {
      await queueStore.initialize();
      await queueStore.insert(createJob(jobId('snapshot-1'), 'snapshot', { data: {} }));
      await queueStore.insert(createJob(jobId('snapshot-2'), 'snapshot', { data: {} }));
      await expect(queueStore.loadManagerSnapshot()).rejects.toThrow(
        'PostgreSQL manager snapshot exceeds maxSnapshotJobs=1'
      );
    } finally {
      await queueStore.close();
    }
  });

  test.skipIf(!postgresUrl)('accepts an exact manager snapshot boundary', async () => {
    const value = namespace('snapshot-boundary');
    const queueStore = store(value, 'snapshot-boundary', { maxSnapshotJobs: 1 });
    try {
      await queueStore.initialize();
      await queueStore.insert(createJob(jobId('snapshot-boundary'), 'snapshot', { data: {} }));
      await expect(queueStore.loadManagerSnapshot()).resolves.toMatchObject({
        rows: [expect.objectContaining({ state: 'waiting' })],
      });
    } finally {
      await queueStore.close();
    }
  });

  test.skipIf(!postgresUrl)('rejects an oversized queue refresh without truncation', async () => {
    const value = namespace('queue-snapshot-budget');
    const queueStore = store(value, 'queue-snapshot-budget', { maxSnapshotJobs: 1 });
    try {
      await queueStore.initialize();
      await queueStore.insert(createJob(jobId('queue-snapshot-1'), 'snapshot', { data: {} }));
      await queueStore.insert(createJob(jobId('queue-snapshot-2'), 'snapshot', { data: {} }));
      await expect(queueStore.loadQueueReadModel('snapshot')).rejects.toThrow(
        'PostgreSQL queue "snapshot" snapshot exceeds maxSnapshotJobs=1'
      );
    } finally {
      await queueStore.close();
    }
  });

  test.skipIf(!postgresUrl)('accounts for encoded payload bytes in snapshot budgets', async () => {
    const value = namespace('snapshot-payload-budget');
    const queueStore = store(value, 'snapshot-payload-budget', {
      maxSnapshotJobs: 10,
      maxSnapshotPayloadBytes: 1,
    });
    try {
      await queueStore.initialize();
      await queueStore.insert(
        createJob(jobId('snapshot-payload'), 'snapshot', { data: { value: 'payload' } })
      );
      await expect(queueStore.loadManagerSnapshot()).rejects.toThrow(
        'PostgreSQL manager snapshot exceeds maxSnapshotPayloadBytes=1'
      );
    } finally {
      await queueStore.close();
    }
  });

  test.skipIf(!postgresUrl)('counts retained completion results in snapshot budgets', async () => {
    const value = namespace('snapshot-result-budget');
    const queueStore = store(value, 'snapshot-result-budget', {
      maxSnapshotJobs: 1,
      maxJobResults: 2,
    });
    try {
      await queueStore.initialize();
      for (const id of ['result-1', 'result-2']) {
        const job = createJob(jobId(id), 'results', { data: {}, removeOnComplete: true });
        await queueStore.insert(job);
        const [claim] = await queueStore.claim('results', 1, 'result-worker', 60_000);
        expect((await queueStore.complete(claim.job.id, claim.token, id, true)).applied).toBe(true);
      }
      await expect(queueStore.loadManagerSnapshot()).rejects.toThrow(
        'PostgreSQL manager snapshot exceeds maxSnapshotJobs=1'
      );
    } finally {
      await queueStore.close();
    }
  });

  test.skipIf(!postgresUrl)('drains commit envelopes beyond one batch', async () => {
    const value = namespace('commit-gc');
    const queueStore = store(value, 'commit-gc');
    try {
      await queueStore.initialize();
      await queueStore.context.sql`
        INSERT INTO bunqueue_event_commits (namespace, transaction_id, commit_seq)
        SELECT ${value}, source.id, nextval('bunqueue_event_commit_seq')
        FROM generate_series(1000001, 1000005) AS source(id)
      `;

      expect(await prunePostgresEventCommits(queueStore.context, 2)).toBe(5);
      const [remaining] = await queueStore.context.sql<{ count: number }[]>`
        SELECT COUNT(*)::int AS count FROM bunqueue_event_commits
        WHERE namespace = ${value}
      `;
      expect(remaining.count).toBe(0);
    } finally {
      await queueStore.close();
    }
  });
});
