import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { TcpClient } from '../src/client/tcp/client';
import { createTcpServer } from '../src/infrastructure/server/tcp';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

async function cleanup(url: string, namespace: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    for (const table of [
      'bunqueue_metric_buckets',
      'bunqueue_metric_totals',
      'bunqueue_workers',
      'bunqueue_crons',
      'bunqueue_job_logs',
      'bunqueue_repeat_links',
      'bunqueue_flow_failures',
      'bunqueue_dependencies',
      'bunqueue_completions',
      'bunqueue_jobs',
      'bunqueue_queue_state',
      'bunqueue_event_prune_watermarks',
      'bunqueue_events',
      'bunqueue_event_commits',
      'bunqueue_brokers',
    ]) {
      await sql.unsafe(`DELETE FROM ${table} WHERE namespace = $1`, [namespace]);
    }
  } finally {
    await sql.close({ timeout: 5 });
  }
}

async function withPostgresTcp(
  label: string,
  operation: (client: TcpClient) => Promise<void>
): Promise<void> {
  const namespace = `test-rate-parity-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(namespace);
  const manager = new PostgresQueueManager({
    postgres: {
      url: postgresUrl!,
      namespace,
      brokerId: `rate-parity-${label}`,
      pollIntervalMs: 25,
    },
  });
  await manager.waitUntilReady();
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const client = new TcpClient({
    host: '127.0.0.1',
    port: server.server.port,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 5_000,
  });
  try {
    await client.connect();
    await operation(client);
  } finally {
    client.close();
    server.stop();
    await manager.shutdownPostgres();
  }
}

async function pushTwo(client: TcpClient, queue: string): Promise<void> {
  for (let index = 0; index < 2; index++) {
    expect((await client.send({ cmd: 'PUSH', queue, data: { index } })).ok).toBe(true);
  }
}

async function expectSecondPullBlocked(client: TcpClient, queue: string): Promise<void> {
  const first = await client.send({ cmd: 'PULL', queue, owner: `${queue}-worker` });
  expect(first).toMatchObject({ ok: true });
  expect(first.job).not.toBeNull();
  const second = await client.send({ cmd: 'PULL', queue, owner: `${queue}-worker` });
  expect(second).toMatchObject({ ok: true, job: null });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const namespace of namespaces) await cleanup(postgresUrl, namespace);
});

describe('PostgreSQL rate-limit parity with SQLite through TCP', () => {
  test.skipIf(!postgresUrl)('treats a non-positive duration as the default window', async () => {
    await withPostgresTcp('duration', async (client) => {
      for (const [label, duration] of [
        ['zero', 0],
        ['negative', -1],
      ] as const) {
        const queue = `${label}-duration`;
        await pushTwo(client, queue);
        expect((await client.send({ cmd: 'RateLimit', queue, limit: 1, duration })).ok).toBe(true);
        await expectSecondPullBlocked(client, queue);
      }
    });
  });

  test.skipIf(!postgresUrl)('treats a non-positive TTL as a permanent rate limit', async () => {
    await withPostgresTcp('ttl', async (client) => {
      for (const [label, ttl] of [
        ['zero', 0],
        ['negative', -1],
      ] as const) {
        const queue = `${label}-ttl`;
        await pushTwo(client, queue);
        expect(
          (await client.send({ cmd: 'RateLimit', queue, limit: 1, duration: 60_000, ttl })).ok
        ).toBe(true);
        await expectSecondPullBlocked(client, queue);
      }
    });
  });
});
