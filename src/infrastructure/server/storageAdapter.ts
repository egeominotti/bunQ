import type { QueueManager } from '../../application/queueManager';
import type { ResolvedConfig } from '../../config';

export type StorageDriver = ResolvedConfig['storageDriver'];

/** Strategy boundary for one server persistence engine. */
export interface ServerStorageAdapter {
  readonly driver: StorageDriver;
  startupError(config: ResolvedConfig): string | null;
  create(config: ResolvedConfig): Promise<QueueManager>;
  shutdown(manager: QueueManager): Promise<void>;
  display(config: ResolvedConfig): string;
}

/** Immutable adapter registry: adding an engine does not change server bootstrap. */
export class StorageAdapterRegistry {
  private readonly adapters = new Map<StorageDriver, ServerStorageAdapter>();

  constructor(adapters: readonly ServerStorageAdapter[]) {
    for (const adapter of adapters) {
      if (this.adapters.has(adapter.driver)) {
        throw new Error(`Duplicate storage adapter: ${adapter.driver}`);
      }
      this.adapters.set(adapter.driver, adapter);
    }
  }

  resolve(driver: StorageDriver): ServerStorageAdapter {
    const adapter = this.adapters.get(driver);
    if (!adapter) throw new Error(`Unsupported storage driver: ${driver}`);
    return adapter;
  }

  drivers(): StorageDriver[] {
    return [...this.adapters.keys()];
  }
}

/** Lifecycle facade that remembers which strategy owns each created manager. */
export class ServerStorageManager {
  private readonly owners = new WeakMap<QueueManager, ServerStorageAdapter>();
  private readonly shutdowns = new WeakMap<QueueManager, Promise<void>>();

  constructor(private readonly registry: StorageAdapterRegistry) {}

  startupError(config: ResolvedConfig): string | null {
    return this.registry.resolve(config.storageDriver).startupError(config);
  }

  async create(config: ResolvedConfig): Promise<QueueManager> {
    const adapter = this.registry.resolve(config.storageDriver);
    const manager = await adapter.create(config);
    this.owners.set(manager, adapter);
    return manager;
  }

  async shutdown(manager: QueueManager): Promise<boolean> {
    const pending = this.shutdowns.get(manager);
    if (pending) {
      await pending;
      return true;
    }
    const adapter = this.owners.get(manager);
    if (!adapter) return false;
    const shutdown = adapter
      .shutdown(manager)
      .then(() => {
        this.owners.delete(manager);
      })
      .finally(() => this.shutdowns.delete(manager));
    this.shutdowns.set(manager, shutdown);
    await shutdown;
    return true;
  }

  display(config: ResolvedConfig): string {
    return this.registry.resolve(config.storageDriver).display(config);
  }
}
