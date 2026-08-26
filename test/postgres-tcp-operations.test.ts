import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { TcpClient } from '../src/client/tcp/client';
import { createTcpServer } from '../src/infrastructure/server/tcp';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-tcp-operations-${Date.now()}-${crypto.randomUUID()}`;
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

describe('PostgreSQL TCP maintenance commands', () => {
  test.skipIf(!postgresUrl)(
    'runs durable DLQ, retry, clean, and policy commands on either broker',
    async () => {
      const value = namespace();
      const managerA = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'tcp-ops-a',
          pollIntervalMs: 25,
        },
      });
      const managerB = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'tcp-ops-b',
          pollIntervalMs: 25,
        },
      });
      await Promise.all([managerA.waitUntilReady(), managerB.waitUntilReady()]);
      const serverA = createTcpServer(managerA, { hostname: '127.0.0.1', port: 0 });
      const serverB = createTcpServer(managerB, { hostname: '127.0.0.1', port: 0 });
      const clientA = new TcpClient({
        host: '127.0.0.1',
        port: serverA.server.port,
        autoReconnect: false,
        pingInterval: 0,
        commandTimeout: 5000,
      });
      const clientB = new TcpClient({
        host: '127.0.0.1',
        port: serverB.server.port,
        autoReconnect: false,
        pingInterval: 0,
        commandTimeout: 5000,
      });
      const otherClientA = new TcpClient({
        host: '127.0.0.1',
        port: serverA.server.port,
        autoReconnect: false,
        pingInterval: 0,
        commandTimeout: 5000,
      });
      try {
        await Promise.all([clientA.connect(), clientB.connect(), otherClientA.connect()]);
        const registered = await clientA.send({
          cmd: 'RegisterWorker',
          name: 'shared-worker',
          queues: ['cron-jobs'],
          concurrency: 2,
        });
        expect(registered.ok).toBe(true);
        expect(await clientB.send({ cmd: 'ListWorkers' })).toMatchObject({
          ok: true,
          data: {
            stats: { total: 1, active: 1, concurrencySlots: 2 },
            workers: [{ name: 'shared-worker', queues: ['cron-jobs'] }],
          },
        });
        const workerId = String((registered as { data?: { workerId?: string } }).data?.workerId);
        expect(
          await clientB.send({
            cmd: 'Heartbeat',
            id: workerId,
            activeJobs: 99,
            processed: 99,
            failed: 99,
          })
        ).toMatchObject({ ok: false, error: 'Worker not found' });
        expect(
          await otherClientA.send({
            cmd: 'Heartbeat',
            id: workerId,
            activeJobs: 98,
            processed: 98,
            failed: 98,
          })
        ).toMatchObject({ ok: false, error: 'Worker not found' });
        expect(await clientB.send({ cmd: 'ListWorkers' })).toMatchObject({
          ok: true,
          data: {
            stats: { activeJobs: 0, totalProcessed: 0, totalFailed: 0 },
            workers: [{ id: workerId, activeJobs: 0, processedJobs: 0, failedJobs: 0 }],
          },
        });
        expect(
          (
            await clientA.send({
              cmd: 'Heartbeat',
              id: workerId,
              activeJobs: 1,
              processed: 7,
              failed: 2,
            })
          ).ok
        ).toBe(true);
        expect(await clientA.send({ cmd: 'ListWorkers' })).toMatchObject({
          ok: true,
          data: {
            stats: { total: 1, activeJobs: 1, totalProcessed: 7, totalFailed: 2 },
            workers: [{ id: workerId, activeJobs: 1, processedJobs: 7, failedJobs: 2 }],
          },
        });
        expect(
          (
            await clientA.send({
              cmd: 'Cron',
              name: 'shared-cron',
              queue: 'cron-jobs',
              data: { source: 'postgres' },
              repeatEvery: 60_000,
              immediately: true,
              maxLimit: 1,
            })
          ).ok
        ).toBe(true);
        expect(await clientB.send({ cmd: 'CronGet', name: 'shared-cron' })).toMatchObject({
          ok: true,
          cron: { name: 'shared-cron', executions: 0 },
        });
        const cronPull = await clientB.send({
          cmd: 'PULL',
          queue: 'cron-jobs',
          owner: 'cron-worker',
          timeout: 1000,
        });
        expect(cronPull.job).toMatchObject({ data: { source: 'postgres' } });
        await clientB.send({
          cmd: 'ACK',
          id: String((cronPull.job as { id: string }).id),
          token: cronPull.token,
        });
        expect(
          (
            await clientA.send({
              cmd: 'Cron',
              name: 'worker-gated-cron',
              queue: 'without-workers',
              data: { shouldNotRun: true },
              repeatEvery: 100,
              immediately: true,
              skipIfNoWorker: true,
            })
          ).ok
        ).toBe(true);
        await Bun.sleep(75);
        expect(
          (
            await clientB.send({
              cmd: 'PULL',
              queue: 'without-workers',
              owner: 'nobody',
            })
          ).job
        ).toBeNull();

        expect(
          (
            await clientA.send({
              cmd: 'SetDlqConfig',
              queue: 'maintenance',
              config: { autoRetry: false, maxEntries: 20 },
            })
          ).ok
        ).toBe(true);
        expect(
          (
            await clientB.send({
              cmd: 'SetStallConfig',
              queue: 'maintenance',
              config: { enabled: true, maxStalls: 2, stallInterval: 1000 },
            })
          ).ok
        ).toBe(true);

        const pushed = await clientA.send({
          cmd: 'PUSH',
          queue: 'maintenance',
          data: { stage: 'fail' },
          maxAttempts: 1,
        });
        const firstId = String(pushed.id);
        expect(
          (
            await clientA.send({
              cmd: 'AddLog',
              id: firstId,
              message: 'shared-log',
              level: 'warn',
            })
          ).ok
        ).toBe(true);
        expect(await clientB.send({ cmd: 'GetLogs', id: firstId })).toMatchObject({
          ok: true,
          data: { count: 1, logs: [{ message: 'shared-log', level: 'warn' }] },
        });
        expect((await clientB.send({ cmd: 'ClearLogs', id: firstId })).ok).toBe(true);
        expect(await clientA.send({ cmd: 'GetLogs', id: firstId })).toMatchObject({
          ok: true,
          data: { count: 0, logs: [] },
        });
        const pulled = await clientB.send({ cmd: 'PULL', queue: 'maintenance', owner: 'worker-b' });
        expect(
          (
            await clientB.send({
              cmd: 'FAIL',
              id: firstId,
              token: pulled.token,
              error: 'expected',
            })
          ).ok
        ).toBe(true);
        expect(
          await clientA.send({ cmd: 'Metrics', queue: 'maintenance', type: 'failed' })
        ).toMatchObject({
          ok: true,
          data: { meta: { count: 1, prevCount: 1 }, data: [1], count: 1 },
        });
        const retried = await clientA.send({
          cmd: 'RetryDlq',
          queue: 'maintenance',
          jobId: firstId,
        });
        expect(retried).toMatchObject({ ok: true, count: 1 });
        const retriedPull = await clientA.send({
          cmd: 'PULL',
          queue: 'maintenance',
          owner: 'worker-a',
        });
        expect(String((retriedPull.job as { id: string }).id)).toBe(firstId);
        await clientA.send({ cmd: 'ACK', id: firstId, token: retriedPull.token, result: 'done' });
        expect(
          await clientB.send({ cmd: 'Metrics', queue: 'maintenance', type: 'completed' })
        ).toMatchObject({
          ok: true,
          data: { meta: { count: 1, prevCount: 1 }, data: [1], count: 1 },
        });

        const retryCompleted = await clientB.send({
          cmd: 'RetryCompleted',
          queue: 'maintenance',
          id: firstId,
        });
        expect(retryCompleted).toMatchObject({ ok: true, count: 1 });
        const completedPull = await clientB.send({
          cmd: 'PULL',
          queue: 'maintenance',
          owner: 'worker-b',
        });
        expect(
          (await clientB.send({ cmd: 'ACK', id: firstId, token: completedPull.token })).ok
        ).toBe(true);
        expect(await clientA.send({ cmd: 'GetState', id: firstId })).toMatchObject({
          ok: true,
          state: 'completed',
        });
        const cleaned = await clientA.send({
          cmd: 'Clean',
          queue: 'maintenance',
          grace: 0,
          state: 'completed',
        });
        expect(cleaned).toMatchObject({ ok: true, count: 1 });

        const discardPush = await clientA.send({
          cmd: 'PUSH',
          queue: 'maintenance',
          data: { stage: 'discard' },
        });
        const discardId = String(discardPush.id);
        expect((await clientB.send({ cmd: 'Discard', id: discardId })).ok).toBe(true);
        expect(
          await clientA.send({ cmd: 'RemoveDlqJob', queue: 'maintenance', jobId: discardId })
        ).toMatchObject({ ok: true, data: { removed: true } });

        for (let index = 0; index < 2; index++) {
          const response = await clientA.send({
            cmd: 'PUSH',
            queue: 'maintenance',
            data: { index },
          });
          expect((await clientB.send({ cmd: 'Discard', id: String(response.id) })).ok).toBe(true);
        }
        expect(await clientA.send({ cmd: 'PurgeDlq', queue: 'maintenance' })).toMatchObject({
          ok: true,
          count: 2,
        });
        await Bun.sleep(75);
        expect(await clientA.send({ cmd: 'GetDlqConfig', queue: 'maintenance' })).toMatchObject({
          ok: true,
          config: { autoRetry: false, maxEntries: 20 },
        });
        expect(await clientB.send({ cmd: 'GetStallConfig', queue: 'maintenance' })).toMatchObject({
          ok: true,
          config: { enabled: true, maxStalls: 2, stallInterval: 1000 },
        });
        const trimmed = await clientB.send({
          cmd: 'TrimEvents',
          queue: 'maintenance',
          maxLength: 0,
        });
        const removedEvents = (trimmed as { data?: { removed?: number } }).data?.removed;
        expect(trimmed).toMatchObject({ ok: true });
        expect(typeof removedEvents).toBe('number');
        expect(removedEvents).toBeGreaterThan(0);

        expect(
          (
            await clientA.send({
              cmd: 'PUSH',
              queue: 'obliterate-shared',
              data: { stage: 'remove-everywhere' },
            })
          ).ok
        ).toBe(true);
        await Bun.sleep(75);
        expect(await clientB.send({ cmd: 'ListQueues' })).toMatchObject({
          ok: true,
          queues: expect.arrayContaining(['obliterate-shared']),
        });
        expect((await clientA.send({ cmd: 'Obliterate', queue: 'obliterate-shared' })).ok).toBe(
          true
        );
        await Bun.sleep(75);
        expect(await clientB.send({ cmd: 'ListQueues' })).toMatchObject({
          ok: true,
          queues: expect.not.arrayContaining(['obliterate-shared']),
        });
        const verification = new SQL(postgresUrl!, { max: 1 });
        try {
          const [event] = await verification<{ event_type: string }[]>`
            SELECT event_type
            FROM bunqueue_events
            WHERE namespace = ${value} AND queue = 'obliterate-shared'
            ORDER BY id DESC
            LIMIT 1
          `;
          expect(event?.event_type).toBe('queue-obliterated');
        } finally {
          await verification.close({ timeout: 5 });
        }
      } finally {
        clientA.close();
        clientB.close();
        otherClientA.close();
        serverA.stop();
        serverB.stop();
        await Promise.allSettled([managerA.shutdownPostgres(), managerB.shutdownPostgres()]);
      }
    }
  );
});
