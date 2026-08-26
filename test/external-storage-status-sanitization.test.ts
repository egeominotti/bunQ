import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { getSharedManager, shutdownManager } from '../src/client/manager';
import { buildStatsRefresh } from '../src/infrastructure/cloud/statsRefresh';
import { collectSnapshot } from '../src/infrastructure/cloud/snapshotCollector';
import { EmbeddedBackend } from '../src/mcp/adapter';

const rawDiagnostic =
  'permission denied for relation bunqueue_jobs (SQLSTATE 42501) at db.internal:5432';
const managers: QueueManager[] = [];

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

afterEach(() => {
  shutdownManager();
  for (const manager of managers.splice(0)) manager.shutdown();
});

describe('external storage-status sanitization', () => {
  test('redacts non-disk diagnostics from MCP embedded and cloud payloads', async () => {
    shutdownManager();
    const embedded = new EmbeddedBackend();
    const shared = getSharedManager();
    shared.getStorageStatus = () => ({ diskFull: false, error: rawDiagnostic, since: 123 });
    const manager = managerWithStorage({ diskFull: false, error: rawDiagnostic, since: 123 });

    const mcp = await embedded.getStorageStatus();
    const refresh = buildStatsRefresh(manager);
    const snapshot = await collectSnapshot({
      queueManager: manager,
      instanceId: 'storage-boundary',
      instanceName: 'storage-boundary',
      startedAt: Date.now(),
      sequenceId: 1,
      includeHeavy: false,
    });
    embedded.shutdown();

    for (const value of [mcp, refresh.storage, snapshot.storage]) {
      expect(value).toMatchObject({ diskFull: false, error: 'Internal server error' });
      expect(JSON.stringify(value)).not.toContain('42501');
      expect(JSON.stringify(value)).not.toContain('db.internal');
    }
  });

  test('preserves disk-full messages across MCP embedded and cloud payloads', async () => {
    const diskFull = { diskFull: true, error: 'database or disk is full', since: 456 };
    shutdownManager();
    const embedded = new EmbeddedBackend();
    getSharedManager().getStorageStatus = () => diskFull;
    const manager = managerWithStorage(diskFull);

    expect(await embedded.getStorageStatus()).toEqual({
      diskFull: true,
      error: 'database or disk is full',
    });
    expect(buildStatsRefresh(manager).storage).toEqual({
      diskFull: true,
      error: 'database or disk is full',
    });
    expect(
      (
        await collectSnapshot({
          queueManager: manager,
          instanceId: 'disk-full',
          instanceName: 'disk-full',
          startedAt: Date.now(),
          sequenceId: 1,
          includeHeavy: false,
        })
      ).storage
    ).toEqual({ diskFull: true, error: 'database or disk is full' });
    embedded.shutdown();
  });
});
