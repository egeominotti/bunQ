import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { shutdownManager } from '../src/client/manager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { EmbeddedBackend, TcpBackend } from '../src/mcp/adapter';
import type { SerializedCron } from '../src/mcp/types/adapter';

let embedded: EmbeddedBackend;
let tcp: TcpBackend;
let manager: QueueManager;
let server: TcpServer;
let dataDir: string;

function expectIntervalCron(cron: SerializedCron | null, name: string): void {
  expect(cron).not.toBeNull();
  expect(cron?.name).toBe(name);
  expect(cron?.repeatEvery).toBe(60_000);
  expect(cron?.schedule).toBeUndefined();
  expect(cron?.nextRun).not.toBeNull();
  expect(Number.isNaN(Date.parse(cron?.nextRun ?? ''))).toBe(false);
  expect(JSON.stringify(cron)).not.toContain('"schedule"');
}

function expectPatternCron(cron: SerializedCron | null, name: string): void {
  expect(cron).not.toBeNull();
  expect(cron?.name).toBe(name);
  expect(cron?.schedule).toBe('0 * * * *');
  expect(cron?.repeatEvery).toBeUndefined();
  expect(cron?.nextRun).not.toBeNull();
  expect(Number.isNaN(Date.parse(cron?.nextRun ?? ''))).toBe(false);
  expect(JSON.stringify(cron)).not.toContain('"repeatEvery"');
}

beforeAll(async () => {
  shutdownManager();
  embedded = new EmbeddedBackend();

  dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-mcp-cron-parity-'));
  manager = new QueueManager({ dataPath: join(dataDir, 'queue.db') });
  server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  tcp = new TcpBackend({ host: '127.0.0.1', port: server.server.port });
  await tcp.connect();
});

afterAll(() => {
  tcp.shutdown();
  server.stop();
  manager.shutdown();
  embedded.shutdown();
  shutdownManager();
  rmSync(dataDir, { recursive: true, force: true });
});

describe.each([
  ['embedded', () => embedded],
  ['tcp', () => tcp],
] as const)('MCP %s cron serialization parity', (_mode, getBackend) => {
  test('rejects invalid cron definitions instead of fabricating metadata', async () => {
    const backend = getBackend();
    const invalidName = `mcp-${_mode}-invalid`;
    await expect(
      Promise.resolve().then(() =>
        backend.addCron({
          name: invalidName,
          queue: `mcp-${_mode}-queue`,
          data: { mode: _mode },
        })
      )
    ).rejects.toThrow(/schedule|repeatEvery/i);
    expect(await backend.getCron(invalidName)).toBeNull();
  });

  test('add, list and get omit schedule for interval crons', async () => {
    const backend = getBackend();
    const name = `mcp-${_mode}-interval`;
    const added = await backend.addCron({
      name,
      queue: `mcp-${_mode}-queue`,
      data: { mode: _mode },
      repeatEvery: 60_000,
    });

    expectIntervalCron(added, name);
    const listed = (await backend.listCrons()).find((cron) => cron.name === name) ?? null;
    const fetched = await backend.getCron(name);
    expectIntervalCron(listed, name);
    expectIntervalCron(fetched, name);
    expect(added.nextRun).toBe(listed?.nextRun);
    expect(added.nextRun).toBe(fetched?.nextRun);
  });

  test('add, list and get omit repeatEvery for pattern crons', async () => {
    const backend = getBackend();
    const name = `mcp-${_mode}-pattern`;
    const added = await backend.addCron({
      name,
      queue: `mcp-${_mode}-queue`,
      data: { mode: _mode },
      schedule: '0 * * * *',
    });

    expectPatternCron(added, name);
    const listed = (await backend.listCrons()).find((cron) => cron.name === name) ?? null;
    const fetched = await backend.getCron(name);
    expectPatternCron(listed, name);
    expectPatternCron(fetched, name);
    expect(added.nextRun).toBe(listed?.nextRun);
    expect(added.nextRun).toBe(fetched?.nextRun);
  });
});
