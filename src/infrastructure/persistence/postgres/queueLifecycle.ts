import type { TransactionSQL } from 'bun';
import { postgresAdvisoryLockName } from './advisoryLocks';
import type { PostgresContext } from './context';

function queueNames(queues: readonly string[]): string[] {
  return [...new Set(queues)].sort();
}

/** Acquire queue lifecycle locks before admission identity or row locks. */
export async function lockPostgresAdmissionQueues(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[]
): Promise<void> {
  const lockNames = queueNames(queues).map((queue) =>
    postgresAdvisoryLockName('queue-lifecycle', ctx.config.namespace, queue)
  );
  if (lockNames.length === 0) return;
  await tx`
    WITH ordered_queues AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS requested(lock_name)
      ORDER BY lock_key
    ), locked_queues AS MATERIALIZED (
      SELECT lock_key, pg_advisory_xact_lock_shared(lock_key) AS acquired
      FROM ordered_queues
      ORDER BY lock_key
    )
    SELECT COUNT(*) FROM locked_queues
  `;
}

/** Persist queue identity only after admission has proved it inserted a generation. */
export async function registerPostgresAdmissionQueues(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[]
): Promise<void> {
  const names = queueNames(queues);
  if (names.length === 0) return;
  await tx`
    INSERT INTO bunqueue_queue_state (namespace, queue)
    SELECT ${ctx.config.namespace}, queue
    FROM unnest(${tx.array(names, 'TEXT')}) AS admitted(queue)
    ON CONFLICT (namespace, queue) DO NOTHING
  `;
}

/** Exclusively serialize queue deletion against every new admission. */
export async function lockPostgresQueueLifecycleExclusive(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<void> {
  const lockName = postgresAdvisoryLockName('queue-lifecycle', ctx.config.namespace, queue);
  await tx`
    SELECT pg_advisory_xact_lock(hashtextextended(${lockName}, 0))
  `;
}

/** Try a late shared lock so repeat ACKs roll back instead of deadlocking deletion. */
export async function tryLockPostgresRepeatQueues(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[]
): Promise<boolean> {
  const lockNames = queueNames(queues).map((queue) =>
    postgresAdvisoryLockName('queue-lifecycle', ctx.config.namespace, queue)
  );
  if (lockNames.length === 0) return true;
  const [result] = await tx<{ acquired: boolean }[]>`
    WITH ordered_queues AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS requested(lock_name)
      ORDER BY lock_key
    ), attempted AS MATERIALIZED (
      SELECT lock_key, pg_try_advisory_xact_lock_shared(lock_key) AS acquired
      FROM ordered_queues
      ORDER BY lock_key
    )
    SELECT COALESCE(bool_and(acquired), TRUE) AS acquired FROM attempted
  `;
  return result.acquired;
}
