import type { TransactionSQL } from 'bun';
import type { GroupPullOptions } from '../../../domain/types/group';
import type { PostgresContext } from './context';
import type { PostgresJobRow } from './types';

/** Assign durable rotation positions to every pending group lacking one. */
export async function ensurePostgresGroupStates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  sequence: number
): Promise<number> {
  const assigned = await tx<{ group_id: string }[]>`
    WITH unsequenced AS (
      SELECT job.group_id, MIN(job.group_order) AS first_order
      FROM bunqueue_jobs AS job
      LEFT JOIN bunqueue_group_state AS groups
        ON groups.namespace = job.namespace AND groups.queue = job.queue
       AND groups.group_id = job.group_id
      WHERE job.namespace = ${ctx.config.namespace} AND job.queue = ${queue}
        AND job.group_id IS NOT NULL AND groups.last_served IS NULL
        AND job.state IN ('waiting', 'prioritized', 'delayed')
      GROUP BY job.group_id
    ), ordered AS (
      SELECT group_id,
             row_number() OVER (ORDER BY first_order, group_id) AS position
      FROM unsequenced
    )
    INSERT INTO bunqueue_group_state (namespace, queue, group_id, last_served)
    SELECT ${ctx.config.namespace}, ${queue}, group_id, ${sequence} + position
    FROM ordered
    ON CONFLICT (namespace, queue, group_id) DO UPDATE
    SET last_served = EXCLUDED.last_served
    WHERE bunqueue_group_state.last_served IS NULL
    RETURNING group_id
  `;
  if (assigned.length === 0) return sequence;
  const next = sequence + assigned.length;
  await tx`
    UPDATE bunqueue_queue_state SET group_sequence = ${next}
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
  return next;
}

/** Reset fixed windows when they expire or a worker changes the effective defaults. */
export async function refreshPostgresGroupRateWindows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  options: GroupPullOptions | undefined,
  now: number
): Promise<void> {
  if (!options?.limit) return;
  const { max, duration } = options.limit;
  await tx`
    UPDATE bunqueue_group_state AS groups
    SET rate_window_started_at = ${now}, rate_count = 0,
        rate_effective_max = COALESCE(groups.rate_limit, ${max}),
        rate_effective_duration_ms = COALESCE(groups.rate_duration_ms, ${duration})
    WHERE groups.namespace = ${ctx.config.namespace} AND groups.queue = ${queue}
      AND EXISTS (
        SELECT 1 FROM bunqueue_jobs AS job
        WHERE job.namespace = groups.namespace AND job.queue = groups.queue
          AND job.group_id = groups.group_id
          AND job.state IN ('waiting', 'prioritized', 'delayed')
      )
      AND (
        groups.rate_window_started_at IS NULL
        OR groups.rate_effective_max IS DISTINCT FROM COALESCE(groups.rate_limit, ${max})
        OR groups.rate_effective_duration_ms IS DISTINCT FROM
             COALESCE(groups.rate_duration_ms, ${duration})
        OR groups.rate_window_started_at + COALESCE(groups.rate_duration_ms, ${duration}) <= ${now}
      )
  `;
}

/** Consume group budgets and move the durable round-robin cursor after row locks are secured. */
export async function commitPostgresGroupClaims(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: {
    queue: string;
    rows: readonly PostgresJobRow[];
    options?: GroupPullOptions;
    sequence: number;
  }
): Promise<void> {
  const { queue, rows, options, sequence } = input;
  const grouped = rows.filter((row) => row.group_id !== null);
  if (grouped.length === 0) return;
  const turns = new Map<string, { last: number; count: number }>();
  for (let index = 0; index < grouped.length; index++) {
    const groupId = grouped[index].group_id!;
    const current = turns.get(groupId);
    turns.set(groupId, {
      last: sequence + index + 1,
      count: (current?.count ?? 0) + 1,
    });
  }
  const entries = [...turns.entries()];
  await tx`
    UPDATE bunqueue_group_state AS groups
    SET last_served = batch.last_served,
        rate_count = groups.rate_count + CASE WHEN ${options?.limit !== undefined}
          THEN batch.claimed_count ELSE 0 END
    FROM unnest(
      ${tx.array(
        entries.map(([groupId]) => groupId),
        'TEXT'
      )},
      ${tx.array(
        entries.map(([, value]) => value.last),
        'BIGINT'
      )},
      ${tx.array(
        entries.map(([, value]) => value.count),
        'INTEGER'
      )}
    ) AS batch(group_id, last_served, claimed_count)
    WHERE groups.namespace = ${ctx.config.namespace} AND groups.queue = ${queue}
      AND groups.group_id = batch.group_id
  `;
  await tx`
    UPDATE bunqueue_queue_state SET group_sequence = ${sequence + grouped.length}
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
}
