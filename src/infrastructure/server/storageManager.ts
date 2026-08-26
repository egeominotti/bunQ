import { PostgresQueueManager } from '../../application/postgresQueueManager';
import { QueueManager } from '../../application/queueManager';
import type { ResolvedConfig } from '../../config';
import {
  ServerStorageManager,
  StorageAdapterRegistry,
  type ServerStorageAdapter,
} from './storageAdapter';

function localAdapter(driver: 'memory' | 'sqlite'): ServerStorageAdapter {
  return {
    driver,
    startupError: (config) =>
      driver === 'sqlite' && !config.dataPath
        ? 'SQLite storage requires storage.dataPath or BUNQUEUE_DATA_PATH.'
        : null,
    create: (config) =>
      Promise.resolve(
        new QueueManager({
          ...(driver === 'sqlite' && { dataPath: config.dataPath }),
          maxPrometheusQueues: config.maxPrometheusQueues,
        })
      ),
    shutdown: (manager) => {
      manager.shutdown();
      return Promise.resolve();
    },
    display: (config) =>
      driver === 'sqlite' && config.dataPath
        ? `SQLite · ${config.dataPath}`
        : 'in-memory · ephemeral',
  };
}

const postgresAdapter: ServerStorageAdapter = {
  driver: 'postgres',
  startupError(config) {
    if (!config.postgresUrl) {
      return 'PostgreSQL storage requires storage.url or BUNQUEUE_POSTGRES_URL.';
    }
    if (config.dataPath) {
      return 'PostgreSQL storage cannot be combined with SQLite dataPath configuration.';
    }
    return null;
  },
  async create(config) {
    if (!config.postgresUrl) throw new Error('PostgreSQL storage URL is missing');
    const manager = new PostgresQueueManager({
      maxPrometheusQueues: config.maxPrometheusQueues,
      postgres: {
        url: config.postgresUrl,
        namespace: config.postgresNamespace,
        poolSize: config.postgresPoolSize,
        leaseDurationMs: config.postgresLeaseDurationMs,
        pollIntervalMs: config.postgresPollIntervalMs,
        ...(config.postgresBrokerId && { brokerId: config.postgresBrokerId }),
      },
    });
    try {
      await manager.waitUntilReady();
      return manager;
    } catch (error) {
      await manager.shutdownPostgres().catch(() => undefined);
      throw error;
    }
  },
  async shutdown(manager) {
    if (!(manager instanceof PostgresQueueManager)) {
      throw new Error('PostgreSQL adapter received an incompatible queue manager');
    }
    await manager.shutdownPostgres();
  },
  display: (config) => `PostgreSQL · namespace ${config.postgresNamespace}`,
};

export const serverStorageAdapters = new StorageAdapterRegistry([
  localAdapter('memory'),
  localAdapter('sqlite'),
  postgresAdapter,
]);
export const serverStorageManager = new ServerStorageManager(serverStorageAdapters);

export function storageStartupError(config: ResolvedConfig): string | null {
  return serverStorageManager.startupError(config);
}

export function backupStartupError(
  config: Pick<ResolvedConfig, 's3BackupEnabled' | 'dataPath'> & {
    storageDriver?: ResolvedConfig['storageDriver'];
  }
): string | null {
  const driver = config.storageDriver ?? (config.dataPath ? 'sqlite' : 'memory');
  if (config.s3BackupEnabled && (driver !== 'sqlite' || !config.dataPath)) {
    return (
      'S3 backup requires persistent SQLite storage. Set BUNQUEUE_DATA_PATH ' +
      'or storage.dataPath before enabling S3_BACKUP_ENABLED.'
    );
  }
  return null;
}

export async function createServerQueueManager(config: ResolvedConfig): Promise<QueueManager> {
  return await serverStorageManager.create(config);
}

export async function shutdownServerQueueManager(manager: QueueManager): Promise<void> {
  if (await serverStorageManager.shutdown(manager)) return;
  if (manager instanceof PostgresQueueManager) await manager.shutdownPostgres();
  else manager.shutdown();
}

export function storageDisplay(config: ResolvedConfig): string {
  return serverStorageManager.display(config);
}
