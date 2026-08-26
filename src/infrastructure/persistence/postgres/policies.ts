import type { SQL, TransactionSQL } from 'bun';
import { DEFAULT_DLQ_CONFIG, type DlqConfig } from '../../../domain/types/dlq';
import { DEFAULT_STALL_CONFIG, type StallConfig } from '../../../domain/types/stall';
import { decodePostgresValue } from './codec';
import type { PostgresContext } from './context';

interface PolicyRow {
  stall_config: Uint8Array | null;
  dlq_config: Uint8Array | null;
}

export interface PostgresQueuePolicies {
  readonly stall: StallConfig;
  readonly dlq: DlqConfig;
}

export function decodePostgresDlqConfig(queue: string, value: Uint8Array | null): DlqConfig {
  return decodePostgresValue(value, DEFAULT_DLQ_CONFIG, `dlqConfig:${queue}`);
}

/** Read effective queue policies inside an existing transaction when supplied. */
export async function getPostgresQueuePolicies(
  sql: SQL | TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<PostgresQueuePolicies> {
  const rows = await sql<PolicyRow[]>`
    SELECT stall_config, dlq_config
    FROM bunqueue_queue_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
  return {
    stall: decodePostgresValue(
      rows[0]?.stall_config ?? null,
      DEFAULT_STALL_CONFIG,
      `stallConfig:${queue}`
    ),
    dlq: decodePostgresDlqConfig(queue, rows[0]?.dlq_config ?? null),
  };
}
