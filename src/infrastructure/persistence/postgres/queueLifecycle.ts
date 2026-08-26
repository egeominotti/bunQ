import type { TransactionSQL } from 'bun';
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
  const names = queueNames(queues);
  if (names.length === 0) return;
  await tx`
    WITH ordered_queues AS MATERIALIZED (
      SELECT queue
      FROM unnest(${tx.array(names, 'TEXT')}) AS requested(queue)
      ORDER BY queue
    ), locked_queues AS MATERIALIZED (
      SELECT queue, pg_advisory_xact_lock_shared(
        hashtext(${ctx.config.namespace}), hashtext(queue)
      ) AS acquired
      FROM ordered_queues
      ORDER BY queue
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
  await tx`
    SELECT pg_advisory_xact_lock(
      hashtext(${ctx.config.namespace}), hashtext(${queue})
    )
  `;
}

/** Try a late shared lock so repeat ACKs roll back instead of deadlocking deletion. */
export async function tryLockPostgresRepeatQueues(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[]
): Promise<boolean> {
  const names = queueNames(queues);
  if (names.length === 0) return true;
  const [result] = await tx<{ acquired: boolean }[]>`
    WITH ordered_queues AS MATERIALIZED (
      SELECT queue
      FROM unnest(${tx.array(names, 'TEXT')}) AS requested(queue)
      ORDER BY queue
    ), attempted AS MATERIALIZED (
      SELECT queue, pg_try_advisory_xact_lock_shared(
        hashtext(${ctx.config.namespace}), hashtext(queue)
      ) AS acquired
      FROM ordered_queues
      ORDER BY queue
    )
    SELECT COALESCE(bool_and(acquired), TRUE) AS acquired FROM attempted
  `;
  return result.acquired;
}
