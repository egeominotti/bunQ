import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { TcpClient } from '../src/client/tcp/client';
import { handleCommand } from '../src/infrastructure/server/handler';
import { createHttpServer } from '../src/infrastructure/server/http';
import { dashboardOverviewEndpoint } from '../src/infrastructure/server/httpDashboardEndpoints';
import { healthEndpoint, readinessEndpoint } from '../src/infrastructure/server/httpEndpoints';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import type { HandlerContext } from '../src/infrastructure/server/types';

const rawDiagnostic =
  'permission denied for relation bunqueue_jobs (SQLSTATE 42501) at db.internal:5432';
const managers: QueueManager[] = [];
const stops: Array<() => void> = [];

function managerWithStorage(status: {
  diskFull: boolean;
  error: string | null;
  since: number | null;
}): QueueManager {
  const manager = new QueueManager();
  manager.getStorageStatus = () => status;
  managers.push(manager);
  return manager;
}

function context(manager: QueueManager): HandlerContext {
  return { queueManager: manager, authTokens: new Set(), authenticated: true };
}

afterEach(() => {
  for (const stop of stops.splice(0)) stop();
  for (const manager of managers.splice(0)) manager.shutdown();
});

describe('client storage-status sanitization', () => {
  test('redacts PostgreSQL diagnostics from every HTTP and TCP server surface', async () => {
    const internal = { diskFull: false, error: rawDiagnostic, since: 123 };
    const manager = managerWithStorage(internal);
    const http = createHttpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const tcp = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    stops.push(
      () => http.stop(),
      () => tcp.stop()
    );
    const client = new TcpClient({
      host: '127.0.0.1',
      port: tcp.server.port,
      autoReconnect: false,
      pingInterval: 0,
      commandTimeout: 5_000,
    });
    stops.push(() => client.close());
    await client.connect();

    const [health, ready, storageHttp, dashboardHttp, storageTcp, dashboardTcp] = await Promise.all(
      [
        fetch(new URL('/health', http.server.url)).then((response) => response.json()),
        fetch(new URL('/ready', http.server.url)).then((response) => response.json()),
        fetch(new URL('/storage', http.server.url)).then((response) => response.json()),
        fetch(new URL('/dashboard', http.server.url)).then((response) => response.json()),
        client.send({ cmd: 'StorageStatus' }),
        client.send({ cmd: 'DashboardOverview' }),
      ]
    );

    for (const response of [health, ready, storageHttp, dashboardHttp, storageTcp, dashboardTcp]) {
      const serialized = JSON.stringify(response);
      expect(serialized).not.toContain('42501');
      expect(serialized).not.toContain('bunqueue_jobs');
      expect(serialized).not.toContain('db.internal');
    }
    expect(health).toMatchObject({ storage: { error: 'Internal server error' } });
    expect(ready).toMatchObject({ storage: { error: 'Internal server error' } });
    expect(storageHttp).toMatchObject({ data: { error: 'Internal server error' } });
    expect(dashboardHttp).toMatchObject({ storage: { error: 'Internal server error' } });
    expect(storageTcp).toMatchObject({ data: { error: 'Internal server error' } });
    expect(dashboardTcp).toMatchObject({ data: { storage: { error: 'Internal server error' } } });
    expect(manager.getStorageStatus()).toBe(internal);
  });

  test('preserves the useful SQLite disk-full status and message', async () => {
    const diskFull = {
      diskFull: true,
      error: 'database or disk is full',
      since: 456,
    };
    const manager = managerWithStorage(diskFull);
    const commandContext = context(manager);
    const health = await healthEndpoint(manager, 0, 0).json();
    const ready = await readinessEndpoint(manager).json();
    const storage = await handleCommand({ cmd: 'StorageStatus' }, commandContext);
    const dashboard = await handleCommand({ cmd: 'DashboardOverview' }, commandContext);
    const dashboardHttp = await (await dashboardOverviewEndpoint(manager)).json();

    expect(health).toMatchObject({ storage: diskFull });
    expect(ready).toMatchObject({ storage: diskFull });
    expect(storage).toMatchObject({ data: diskFull });
    expect(dashboard).toMatchObject({ data: { storage: diskFull } });
    expect(dashboardHttp).toMatchObject({ storage: diskFull });
  });
});
