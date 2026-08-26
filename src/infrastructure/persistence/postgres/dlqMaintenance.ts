import type { PostgresContext } from './context';
import {
  enforcePostgresDlqLimitInTransaction,
  maintainPostgresDlq as maintainPostgresDlqLifecycle,
  type PostgresDlqMaintenanceResult,
} from './dlqLifecycle';
import { decodePostgresDlqConfig } from './policies';

interface DlqRepairCandidate {
  readonly queue: string;
  readonly dlq_config: Uint8Array | null;
  readonly failed_count: number;
}

/** Reconcile persisted queue limits after interrupted post-commit maintenance. */
export async function repairPostgresDlq(ctx: PostgresContext): Promise<number> {
  const candidates = await ctx.sql<DlqRepairCandidate[]>`
    SELECT jobs.queue, state.dlq_config, COUNT(*)::int AS failed_count
    FROM bunqueue_jobs AS jobs
    JOIN bunqueue_queue_state AS state
      ON state.namespace = jobs.namespace AND state.queue = jobs.queue
    WHERE jobs.namespace = ${ctx.config.namespace} AND jobs.state = 'failed'
    GROUP BY jobs.queue, state.dlq_config
    ORDER BY jobs.queue
  `;
  let removed = 0;
  for (const candidate of candidates) {
    const snapshotConfig = decodePostgresDlqConfig(candidate.queue, candidate.dlq_config);
    if (candidate.failed_count <= snapshotConfig.maxEntries) continue;
    removed += await ctx.sql.begin(async (tx) => {
      const [state] = await tx<{ dlq_config: Uint8Array | null }[]>`
        SELECT dlq_config FROM bunqueue_queue_state
        WHERE namespace = ${ctx.config.namespace} AND queue = ${candidate.queue}
        FOR SHARE
      `;
      const config = decodePostgresDlqConfig(candidate.queue, state?.dlq_config ?? null);
      return (
        await enforcePostgresDlqLimitInTransaction(tx, ctx, candidate.queue, config.maxEntries)
      ).length;
    });
  }
  return removed;
}

/** Periodic DLQ work also repairs any retention skipped by a stopped broker. */
export async function maintainPostgresDlq(
  ctx: PostgresContext,
  limit = 1000
): Promise<PostgresDlqMaintenanceResult> {
  const result = await maintainPostgresDlqLifecycle(ctx, limit);
  await repairPostgresDlq(ctx);
  return result;
}
