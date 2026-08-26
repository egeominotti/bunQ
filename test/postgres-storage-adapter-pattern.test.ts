import { describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { QueueManager } from '../src/application/queueManager';
import { resolveServerConfig } from '../src/config';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import {
  ServerStorageManager,
  StorageAdapterRegistry,
  type ServerStorageAdapter,
  type StorageDriver,
} from '../src/infrastructure/server/storageAdapter';
import {
  createServerQueueManager,
  serverStorageAdapters,
  serverStorageManager,
  shutdownServerQueueManager,
  storageDisplay,
} from '../src/infrastructure/server/storageManager';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

function postgresStore(manager: QueueManager): PostgresQueueStore {
  const postgres = manager as PostgresQueueManager;
  return (postgres as unknown as { postgresStore: PostgresQueueStore }).postgresStore;
}

describe('server storage adapter pattern', () => {
  test('registers each strategy once and resolves it by driver', () => {
    const memory: ServerStorageAdapter = {
      driver: 'memory',
      startupError: () => null,
      create: async () => new QueueManager(),
      shutdown: async (manager) => manager.shutdown(),
      display: () => 'test-memory',
    };
    const registry = new StorageAdapterRegistry([memory]);
    expect(registry.resolve('memory')).toBe(memory);
    expect(registry.drivers()).toEqual(['memory']);
    expect(() => new StorageAdapterRegistry([memory, memory])).toThrow(
      'Duplicate storage adapter: memory'
    );
    expect(() => registry.resolve('future' as StorageDriver)).toThrow(
      'Unsupported storage driver: future'
    );
  });

  test('delegates the complete lifecycle to the owning strategy exactly once', async () => {
    const calls: string[] = [];
    const adapter: ServerStorageAdapter = {
      driver: 'memory',
      startupError: () => 'strategy-validation',
      async create() {
        calls.push('create');
        return new QueueManager();
      },
      async shutdown(manager) {
        calls.push('shutdown');
        manager.shutdown();
      },
      display: () => 'strategy-display',
    };
    const service = new ServerStorageManager(new StorageAdapterRegistry([adapter]));
    const config = resolveServerConfig({ storage: { driver: 'memory' } });
    expect(service.startupError(config)).toBe('strategy-validation');
    expect(service.display(config)).toBe('strategy-display');

    const manager = await service.create(config);
    expect(await service.shutdown(manager)).toBe(true);
    expect(await service.shutdown(manager)).toBe(false);
    expect(calls).toEqual(['create', 'shutdown']);
  });

  test('coalesces concurrent shutdown and permits retry after an adapter error', async () => {
    let attempts = 0;
    let releaseFirst!: () => void;
    const firstAttempt = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const adapter: ServerStorageAdapter = {
      driver: 'memory',
      startupError: () => null,
      create: async () => new QueueManager(),
      async shutdown(manager) {
        attempts++;
        if (attempts === 1) {
          await firstAttempt;
          throw new Error('transient shutdown failure');
        }
        manager.shutdown();
      },
      display: () => 'test-memory',
    };
    const service = new ServerStorageManager(new StorageAdapterRegistry([adapter]));
    const manager = await service.create(resolveServerConfig({ storage: { driver: 'memory' } }));

    const left = service.shutdown(manager);
    const right = service.shutdown(manager);
    const firstWave = Promise.allSettled([left, right]);
    releaseFirst();
    const outcomes = await firstWave;
    expect(outcomes.map((outcome) => outcome.status)).toEqual(['rejected', 'rejected']);
    expect(
      outcomes.map((outcome) =>
        outcome.status === 'rejected' ? String(outcome.reason) : 'unexpected success'
      )
    ).toEqual(['Error: transient shutdown failure', 'Error: transient shutdown failure']);
    expect(attempts).toBe(1);

    expect(await service.shutdown(manager)).toBe(true);
    expect(await service.shutdown(manager)).toBe(false);
    expect(attempts).toBe(2);
  });

  test('ships memory, SQLite, and PostgreSQL as interchangeable strategies', () => {
    expect(serverStorageAdapters.drivers().sort()).toEqual(['memory', 'postgres', 'sqlite']);
  });

  test('keeps the explicit memory strategy in memory with an inherited data path', async () => {
    const previousDataPath = Bun.env.BUNQUEUE_DATA_PATH;
    Bun.env.BUNQUEUE_DATA_PATH = ':memory:';
    let manager: QueueManager | null = null;
    try {
      const config = resolveServerConfig({ storage: { driver: 'memory' } });
      manager = await createServerQueueManager(config);
      expect(config.storageDriver).toBe('memory');
      expect(config.dataPath).toBe(':memory:');
      expect(storageDisplay(config)).toBe('in-memory · ephemeral');
      expect((manager as unknown as { storage: unknown }).storage).toBeNull();
    } finally {
      if (manager) await shutdownServerQueueManager(manager);
      if (previousDataPath === undefined) delete Bun.env.BUNQUEUE_DATA_PATH;
      else Bun.env.BUNQUEUE_DATA_PATH = previousDataPath;
    }
  });

  test.skipIf(!postgresUrl)(
    'retries a real PostgreSQL adapter shutdown after concurrent cleanup failure',
    async () => {
      const namespace = `test-adapter-shutdown-${Date.now()}-${crypto.randomUUID()}`;
      const config = resolveServerConfig({
        storage: { driver: 'postgres', url: postgresUrl!, namespace, brokerId: 'adapter-retry' },
      });
      const manager = await serverStorageManager.create(config);
      const store = postgresStore(manager);
      const originalEventClose = store.events.close.bind(store.events);
      let closeAttempts = 0;
      store.events.close = async () => {
        closeAttempts++;
        if (closeAttempts === 1) throw new Error('synthetic event close failure');
        await originalEventClose();
      };

      try {
        const firstWave = await Promise.allSettled([
          serverStorageManager.shutdown(manager),
          serverStorageManager.shutdown(manager),
        ]);
        expect(firstWave.map((outcome) => outcome.status)).toEqual(['rejected', 'rejected']);
        expect(closeAttempts).toBe(1);

        expect(await serverStorageManager.shutdown(manager)).toBe(true);
        expect(closeAttempts).toBe(2);
        expect(await serverStorageManager.shutdown(manager)).toBe(false);

        let poolClosed = false;
        try {
          await store.context.sql`SELECT 1`;
        } catch {
          poolClosed = true;
        }
        expect(poolClosed).toBe(true);
      } finally {
        store.events.close = originalEventClose;
        await Promise.allSettled([originalEventClose(), store.context.sql.close({ timeout: 5 })]);
      }
    }
  );
});
