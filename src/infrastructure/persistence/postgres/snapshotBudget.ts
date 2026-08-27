import type { PostgresContext, PostgresReadSql } from './context';

interface SnapshotWeightRow {
  readonly items: number | string | bigint;
  readonly payload_bytes: number | string | bigint;
}

/** Reject an unsafe local read model before allocating or decoding its payloads. */
export async function assertPostgresSnapshotBudget(
  ctx: PostgresContext,
  sql: PostgresReadSql,
  queue?: string
): Promise<void> {
  const queueFilter = queue ?? null;
  const [weight] = await sql<SnapshotWeightRow[]>`
    WITH retained_jobs AS MATERIALIZED (
      SELECT payload, result, dlq_entry, dlq_retry_state
      FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND (${queueFilter}::text IS NULL OR queue = ${queueFilter})
        AND (${queueFilter}::text IS NOT NULL OR state <> 'completed')
      UNION ALL
      (
        SELECT payload, result, dlq_entry, dlq_retry_state
        FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace}
          AND ${queueFilter}::text IS NULL
          AND state = 'completed'
        ORDER BY completed_at DESC NULLS LAST, id DESC
        LIMIT ${ctx.config.maxCompletedJobs}
      )
    ), pinned AS MATERIALIZED (
      SELECT DISTINCT dependency.dependency_id AS job_id
      FROM bunqueue_dependencies AS dependency
      JOIN bunqueue_jobs AS consumer
        ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
      WHERE dependency.namespace = ${ctx.config.namespace}
        AND consumer.state = ANY(${sql.array(
          ['waiting', 'prioritized', 'delayed', 'waiting-children', 'active'],
          'TEXT'
        )})
    ), recent_completions AS MATERIALIZED (
      SELECT completion.job_id, completion.result
      FROM bunqueue_completions AS completion
      WHERE completion.namespace = ${ctx.config.namespace}
        AND (${queueFilter}::text IS NULL OR completion.queue = ${queueFilter})
        AND NOT EXISTS (
          SELECT 1 FROM pinned WHERE pinned.job_id = completion.job_id
        )
      ORDER BY completion.completed_at DESC, completion.job_id DESC
      LIMIT ${ctx.config.maxJobResults}
    ), selected_completions AS MATERIALIZED (
      SELECT completion.job_id, completion.result
      FROM bunqueue_completions AS completion
      JOIN pinned ON pinned.job_id = completion.job_id
      WHERE completion.namespace = ${ctx.config.namespace}
        AND (${queueFilter}::text IS NULL OR completion.queue = ${queueFilter})
      UNION ALL
      SELECT job_id, result FROM recent_completions
    )
    SELECT
      (
        (SELECT COUNT(*) FROM retained_jobs) +
        (SELECT COUNT(*) FROM selected_completions)
      )::bigint AS items,
      (
        COALESCE((
          SELECT SUM(
            octet_length(payload) + COALESCE(octet_length(result), 0) +
            COALESCE(octet_length(dlq_entry), 0) +
            COALESCE(octet_length(dlq_retry_state), 0)
          ) FROM retained_jobs
        ), 0) +
        COALESCE((SELECT SUM(COALESCE(octet_length(result), 0)) FROM selected_completions), 0) +
        COALESCE((
          SELECT SUM(octet_length(payload)) FROM bunqueue_crons
          WHERE namespace = ${ctx.config.namespace} AND ${queueFilter}::text IS NULL
        ), 0) +
        COALESCE((
          SELECT SUM(
            COALESCE(octet_length(stall_config), 0) +
            COALESCE(octet_length(dlq_config), 0)
          ) FROM bunqueue_queue_state
          WHERE namespace = ${ctx.config.namespace}
            AND (${queueFilter}::text IS NULL OR queue = ${queueFilter})
        ), 0)
      )::bigint AS payload_bytes
  `;
  const scope = queue === undefined ? 'manager' : `queue "${queue}"`;
  if (Number(weight.items) > ctx.config.maxSnapshotJobs) {
    throw new Error(
      `PostgreSQL ${scope} snapshot exceeds maxSnapshotJobs=${ctx.config.maxSnapshotJobs}`
    );
  }
  if (Number(weight.payload_bytes) > ctx.config.maxSnapshotPayloadBytes) {
    throw new Error(
      `PostgreSQL ${scope} snapshot exceeds ` +
        `maxSnapshotPayloadBytes=${ctx.config.maxSnapshotPayloadBytes}`
    );
  }
}
