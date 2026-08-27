import { postgresAdvisoryLockName } from './advisoryLocks';
import type { PostgresContext } from './context';
import {
  loadPostgresLifetimeMetrics,
  type PostgresLifetimeMetricsSnapshot,
} from './lifetimeMetrics';

/** Fence terminal counters against the same commit sequencer used by journal writers. */
export async function finalizePostgresLifetimeMetrics(
  ctx: PostgresContext,
  apply: (snapshot: PostgresLifetimeMetricsSnapshot) => void
): Promise<void> {
  const lockName = postgresAdvisoryLockName('event-commit', ctx.config.namespace);
  await ctx.sql.begin(async (tx) => {
    await tx`
      SELECT pg_advisory_xact_lock(hashtextextended(${lockName}, 0))
    `;
    const snapshot = await loadPostgresLifetimeMetrics(ctx, tx);
    apply(snapshot);
  });
}
