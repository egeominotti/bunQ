import type { PostgresStorageConfig } from '../../infrastructure/persistence/postgres';
import type { QueueManagerConfig } from '../types';

export interface PostgresQueueManagerConfig extends Omit<QueueManagerConfig, 'dataPath'> {
  readonly postgres: PostgresStorageConfig;
}
