import { describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { TcpClient } from '../src/client/tcp/client';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

describe('PostgreSQL MoveToWait', () => {
  test.skipIf(!postgresUrl)('retries a failed job durably through the TCP command', async () => {
    const namespace = `test-move-to-wait-${Date.now()}-${crypto.randomUUID()}`;
    const manager = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace,
        brokerId: 'move-to-wait-broker',
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
      const pushed = await client.send({
        cmd: 'PUSH',
        queue: 'move-to-wait',
        data: { source: 'postgres' },
        maxAttempts: 1,
      });
      const id = String((pushed as { id: string }).id);
      const claimed = await client.send({
        cmd: 'PULL',
        queue: 'move-to-wait',
        owner: 'worker',
      });
      expect(String((claimed.job as { id: string }).id)).toBe(id);
      expect(
        (
          await client.send({
            cmd: 'FAIL',
            id,
            token: claimed.token,
            error: 'expected terminal failure',
          })
        ).ok
      ).toBe(true);
      expect(await client.send({ cmd: 'GetState', id })).toMatchObject({
        ok: true,
        state: 'failed',
      });

      expect(await client.send({ cmd: 'MoveToWait', id })).toMatchObject({ ok: true });
      expect(await client.send({ cmd: 'GetState', id })).toMatchObject({
        ok: true,
        state: 'waiting',
      });
      const retried = await client.send({
        cmd: 'PULL',
        queue: 'move-to-wait',
        owner: 'retry-worker',
      });
      expect(String((retried.job as { id: string }).id)).toBe(id);
    } finally {
      client.close();
      server.stop();
      await manager.shutdownPostgres();
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  });
});
