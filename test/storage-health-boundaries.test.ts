import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer, type HttpServer } from '../src/infrastructure/server/http';
import { buildHealthSnapshot } from '../src/infrastructure/server/ws/snapshots';

const managers: QueueManager[] = [];
const servers: HttpServer[] = [];

function manager(): QueueManager {
  const queueManager = new QueueManager();
  managers.push(queueManager);
  return queueManager;
}

function server(queueManager: QueueManager): HttpServer {
  const httpServer = createHttpServer(queueManager, { hostname: '127.0.0.1', port: 0 });
  servers.push(httpServer);
  return httpServer;
}

afterEach(() => {
  for (const httpServer of servers.splice(0)) httpServer.stop();
  for (const queueManager of managers.splice(0)) queueManager.shutdown();
});

describe('storage health boundaries', () => {
  test('keeps healthy SQLite HTTP endpoints and metrics healthy', async () => {
    const httpServer = server(manager());
    const [health, ready, metrics] = await Promise.all([
      fetch(new URL('/health', httpServer.server.url)),
      fetch(new URL('/ready', httpServer.server.url)),
      fetch(new URL('/prometheus', httpServer.server.url)),
    ]);

    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({ ok: true, status: 'healthy' });
    expect(ready.status).toBe(200);
    expect(await ready.json()).toMatchObject({ ok: true, ready: true });
    const metricsText = await metrics.text();
    expect(metricsText).toContain('bunqueue_storage_degraded 0');
    expect(metricsText).toContain('bunqueue_storage_disk_full 0');
  });

  test('surfaces degraded storage without exposing diagnostics through HTTP', async () => {
    const queueManager = manager();
    queueManager.getStorageStatus = () => ({
      diskFull: false,
      error: 'synthetic PostgreSQL event drain failure',
      since: 123,
    });
    const httpServer = server(queueManager);
    const [health, ready, metrics] = await Promise.all([
      fetch(new URL('/health', httpServer.server.url)),
      fetch(new URL('/ready', httpServer.server.url)),
      fetch(new URL('/prometheus', httpServer.server.url)),
    ]);

    expect(health.status).toBe(503);
    expect(await health.json()).toMatchObject({
      ok: false,
      status: 'degraded',
      storage: {
        diskFull: false,
        error: 'Internal server error',
        since: 123,
      },
    });
    expect(ready.status).toBe(503);
    expect(await ready.json()).toMatchObject({
      ok: false,
      ready: false,
      storage: {
        diskFull: false,
        error: 'Internal server error',
        since: 123,
      },
    });
    const metricsText = await metrics.text();
    expect(metricsText).toContain('bunqueue_storage_degraded 1');
    expect(metricsText).toContain('bunqueue_storage_disk_full 0');
  });

  test('marks a WebSocket health snapshot unhealthy for a storage error', () => {
    const queueManager = manager();
    queueManager.getStorageStatus = () => ({
      diskFull: false,
      error: 'synthetic PostgreSQL heartbeat failure',
      since: 456,
    });

    expect(buildHealthSnapshot(queueManager, 3)).toMatchObject({ ok: false });
  });
});
