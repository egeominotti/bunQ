import { SqliteLifecycle } from './sqlite/lifecycle';

export type {
  SqliteConfig,
  SqliteCriticalLoss,
  SqliteCriticalLossCallback,
} from './types/sqlite';

/** SQLite persistence façade. Implementation is split by responsibility. */
export class SqliteStorage extends SqliteLifecycle {}
