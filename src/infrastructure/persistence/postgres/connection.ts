import { SQL } from 'bun';
import type { ResolvedPostgresStorageConfig } from './types';

export function createPostgresConnection(config: ResolvedPostgresStorageConfig): SQL {
  return new SQL(config.url, {
    max: config.poolSize,
    connectionTimeout: 10,
    idleTimeout: 30,
    maxLifetime: 3600,
    connection: {
      application_name: `bunqueue:${config.brokerId}`.slice(0, 63),
      statement_timeout: config.statementTimeoutMs,
      lock_timeout: config.lockTimeoutMs,
      idle_in_transaction_session_timeout: config.idleTransactionTimeoutMs,
    },
  });
}
