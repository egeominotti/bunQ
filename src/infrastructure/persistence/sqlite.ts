import { SqliteLifecycle } from './sqlite/lifecycle';

export type {
  SqliteConfig,
  SqliteCriticalLoss,
  SqliteCriticalLossCallback,
} from './types/sqlite';
export type { DurableAdmissionMetadata } from './types/admission';

/** SQLite persistence façade. Implementation is split by responsibility. */
export class SqliteStorage extends SqliteLifecycle {}
