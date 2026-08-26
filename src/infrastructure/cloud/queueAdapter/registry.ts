import type { QueueManager } from '../../../application/queueManager';
import { PostgresQueueManager } from '../../../application/postgresQueueManager';
import { LocalCloudQueueAdapter } from './local';
import { PostgresCloudQueueAdapter } from './postgres';
import type { CloudQueueAdapter } from './types';

const adapters = new WeakMap<QueueManager, CloudQueueAdapter>();

/** Resolve one complete engine strategy and cache it for the manager lifetime. */
export function resolveCloudQueueAdapter(manager: QueueManager): CloudQueueAdapter {
  const existing = adapters.get(manager);
  if (existing) return existing;
  const adapter =
    manager instanceof PostgresQueueManager
      ? new PostgresCloudQueueAdapter(manager)
      : new LocalCloudQueueAdapter(manager);
  adapters.set(manager, adapter);
  return adapter;
}
