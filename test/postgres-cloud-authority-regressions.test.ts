import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import { handleCommand as handleCloudCommand } from '../src/infrastructure/cloud/commandHandler';
import { collectSnapshot } from '../src/infrastructure/cloud/snapshotCollector';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-cloud-authority-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

async function complete(manager: PostgresQueueManager, queue: string, id: string): Promise<JobId> {
  const job = await manager.push(queue, { customId: id, data: {} });
  const claim = await manager.pullWithLock(queue, `${id}-worker`);
  await manager.ack(job.id, { id }, claim.token!);
  return job.id;
}

async function fail(manager: PostgresQueueManager, queue: string, id: string): Promise<JobId> {
  const job = await manager.push(queue, { customId: id, data: {}, maxAttempts: 1 });
  const claim = await manager.pullWithLock(queue, `${id}-worker`);
  await manager.fail(job.id, `${id}-failure`, claim.token!);
  return job.id;
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL Cloud authority regressions', () => {
  test.skipIf(!postgresUrl)('returns exactly the requested Cloud job page size', async () => {
    const value = namespace('pagination');
    const queueManager = manager(value, 'cloud-pagination');
    const queue = 'cloud-pagination';
    try {
      await queueManager.waitUntilReady();
      for (let index = 0; index < 3; index++) {
        await queueManager.push(queue, { data: { index } });
      }

      const response = await handleCloudCommand(queueManager, {
        type: 'command',
        id: 'postgres-cloud-page',
        action: 'job:list',
        queue,
        state: 'waiting',
        offset: 0,
        limit: 2,
      });

      expect(response.success).toBe(true);
      expect((response.data as { jobs: unknown[] }).jobs).toHaveLength(2);

      const zero = await handleCloudCommand(queueManager, {
        type: 'command',
        id: 'postgres-cloud-zero-page',
        action: 'job:list',
        queue,
        state: 'waiting',
        offset: -10,
        limit: -5,
      });
      expect(zero.data).toMatchObject({ jobs: [], offset: 0, limit: 0 });

      const secondPage = await handleCloudCommand(queueManager, {
        type: 'command',
        id: 'postgres-cloud-second-page',
        action: 'job:list',
        queue,
        state: 'waiting',
        offset: 1,
        limit: 2,
      });
      expect((secondPage.data as { jobs: unknown[] }).jobs).toHaveLength(2);
    } finally {
      await queueManager.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)('executes queue clean against durable PostgreSQL state', async () => {
    const value = namespace('clean');
    const writer = manager(value, 'cloud-clean-writer');
    const reader = manager(value, 'cloud-clean-reader');
    try {
      await Promise.all([writer.waitUntilReady(), reader.waitUntilReady()]);
      const job = await writer.push('cloud-clean', { data: {} });
      reader.clean = () => {
        throw new Error('synchronous clean must not be used');
      };

      const result = await handleCloudCommand(reader, {
        type: 'command',
        id: 'clean-command',
        action: 'queue:clean',
        queue: 'cloud-clean',
        graceMs: 0,
        state: 'waiting',
      });

      expect(result).toMatchObject({
        success: true,
        data: { queue: 'cloud-clean', cleaned: 1, ids: [job.id] },
      });
      expect(await writer.getJob(job.id)).toBeNull();
    } finally {
      await Promise.allSettled([writer.shutdownPostgres(), reader.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)('reads and clears durable PostgreSQL job logs', async () => {
    const value = namespace('logs');
    const queueManager = manager(value, 'cloud-logs');
    try {
      await queueManager.waitUntilReady();
      const job = await queueManager.push('cloud-logs', { data: {} });
      expect(await queueManager.addLogDurable(job.id, 'durable-log', 'warn')).toBe(true);

      const listed = await handleCloudCommand(queueManager, {
        type: 'command',
        id: 'logs-command',
        action: 'job:logs',
        jobId: String(job.id),
      });
      expect(listed).toMatchObject({
        success: true,
        data: { logs: [{ message: 'durable-log', level: 'warn' }] },
      });

      const cleared = await handleCloudCommand(queueManager, {
        type: 'command',
        id: 'clear-logs-command',
        action: 'job:clearLogs',
        jobId: String(job.id),
      });
      expect(cleared.success).toBe(true);
      expect(await queueManager.getLogsDurable(job.id)).toEqual([]);
    } finally {
      await queueManager.shutdownPostgres();
    }
  });

  test.skipIf(!postgresUrl)(
    'retries completed and failed jobs and purges the DLQ through durable commands',
    async () => {
      const value = namespace('retry-purge');
      const queueManager = manager(value, 'cloud-retry-purge');
      try {
        await queueManager.waitUntilReady();
        const completedId = await complete(queueManager, 'cloud-completed', 'completed-job');
        const retryCompleted = await handleCloudCommand(queueManager, {
          type: 'command',
          id: 'retry-completed-command',
          action: 'queue:retryCompleted',
          queue: 'cloud-completed',
        });
        expect(retryCompleted).toMatchObject({ success: true, data: { retried: 1 } });
        expect(await queueManager.getJobState(completedId)).toBe('waiting');

        const retryId = await fail(queueManager, 'cloud-dlq-retry', 'retry-job');
        const retryDlq = await handleCloudCommand(queueManager, {
          type: 'command',
          id: 'retry-dlq-command',
          action: 'dlq:retry',
          queue: 'cloud-dlq-retry',
          jobId: String(retryId),
        });
        expect(retryDlq).toMatchObject({ success: true, data: { retried: 1 } });
        expect(await queueManager.getJobState(retryId)).toBe('waiting');

        const purgeId = await fail(queueManager, 'cloud-dlq-purge', 'purge-job');
        const purge = await handleCloudCommand(queueManager, {
          type: 'command',
          id: 'purge-dlq-command',
          action: 'dlq:purge',
          queue: 'cloud-dlq-purge',
        });
        expect(purge).toMatchObject({ success: true, data: { purged: 1 } });
        expect(await queueManager.getJob(purgeId)).toBeNull();
      } finally {
        await queueManager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'collects durable totals, workers, logs, and active locks from another live broker',
    async () => {
      const value = namespace('snapshot');
      const writer = manager(value, 'cloud-snapshot-writer');
      const reader = manager(value, 'cloud-snapshot-reader');
      try {
        await Promise.all([writer.waitUntilReady(), reader.waitUntilReady()]);
        await writer.registerWorkerDurable('cloud-worker', ['cloud-snapshot'], 2, {
          workerId: 'cloud-worker-id',
          hostname: 'cloud-host',
          pid: 42,
        });
        await writer.addCronDurable({
          name: 'cloud-cron',
          queue: 'cloud-snapshot',
          data: {},
          repeatEvery: 60_000,
        });
        await complete(writer, 'cloud-snapshot', 'snapshot-completed');
        const active = await writer.push('cloud-snapshot', {
          customId: 'snapshot-active',
          data: {},
        });
        expect(await writer.addLogDurable(active.id, 'snapshot-log')).toBe(true);
        const claim = await writer.pullWithLock('cloud-snapshot', 'snapshot-worker', 0, 60_000);
        expect(claim.job?.id).toBe(active.id);

        reader.getStats = () => {
          throw new Error('synchronous stats must not be used');
        };
        reader.listQueues = () => {
          throw new Error('synchronous queues must not be used');
        };
        reader.getJobs = () => {
          throw new Error('synchronous jobs must not be used');
        };
        reader.getAllJobLogs = () => {
          throw new Error('synchronous logs must not be used');
        };
        reader.getAllJobResults = () => {
          throw new Error('synchronous results must not be used');
        };
        reader.getAllJobLocks = () => {
          throw new Error('synchronous locks must not be used');
        };
        const snapshot = await collectSnapshot({
          queueManager: reader,
          instanceId: 'cloud-snapshot-instance',
          instanceName: 'cloud-snapshot',
          startedAt: Date.now(),
          sequenceId: 1,
          includeHeavy: true,
        });

        expect(snapshot.stats.totalCompleted).toBe('1');
        expect(snapshot.workers.total).toBe(1);
        expect(snapshot.workerDetails.map(({ id }) => id)).toContain('cloud-worker-id');
        expect(snapshot.crons.map(({ name }) => name)).toContain('cloud-cron');
        expect(snapshot.jobLogEntries[String(active.id)]).toMatchObject([
          { message: 'snapshot-log' },
        ]);
        expect(snapshot.activeLocks.map(({ jobId }) => jobId)).toContain(String(active.id));
      } finally {
        await Promise.allSettled([writer.shutdownPostgres(), reader.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'restores durable Cloud jobs, results, logs, crons, and lifetime totals after a real restart',
    async () => {
      const value = namespace('real-restart');
      const writer = manager(value, 'cloud-restart-writer');
      let reader: PostgresQueueManager | null = null;
      try {
        await writer.waitUntilReady();
        await writer.addCronDurable({
          name: 'restart-cron',
          queue: 'restart-queue',
          data: {},
          repeatEvery: 60_000,
        });
        const completedId = await complete(writer, 'restart-queue', 'restart-completed');
        const logged = await writer.push('restart-queue', {
          customId: 'restart-logged',
          data: {},
        });
        await writer.addLogDurable(logged.id, 'restart-log');
        await writer.shutdownPostgres();

        reader = manager(value, 'cloud-restart-reader');
        await reader.waitUntilReady();
        const snapshot = await collectSnapshot({
          queueManager: reader,
          instanceId: 'cloud-restart-instance',
          instanceName: 'cloud-restart',
          startedAt: Date.now(),
          sequenceId: 1,
          includeHeavy: true,
        });

        expect(snapshot.stats.totalCompleted).toBe('1');
        expect(snapshot.crons.map(({ name }) => name)).toContain('restart-cron');
        expect(snapshot.jobResults[String(completedId)]).toEqual({ id: 'restart-completed' });
        expect(snapshot.jobLogEntries[String(logged.id)]).toMatchObject([
          { message: 'restart-log' },
        ]);
      } finally {
        await Promise.allSettled([writer.shutdownPostgres(), reader?.shutdownPostgres()]);
      }
    }
  );
});
