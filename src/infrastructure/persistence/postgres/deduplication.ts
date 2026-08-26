import type { TransactionSQL } from 'bun';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { recordPostgresEvent, type PostgresContext } from './context';
import type { PostgresJobRow, PostgresStoredJob } from './types';

/** Clear a live deduplication owner and publish the new authoritative payload. */
export async function clearPostgresDeduplication(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresJobRow,
  now: number
): Promise<PostgresStoredJob> {
  const stored = decodePostgresJob(row);
  const updatedJob = { ...stored.job, uniqueKey: null };
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(updatedJob)}, unique_key = NULL,
        unique_expires_at = NULL, version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: updatedJob.queue,
    type: 'updated',
    jobId: updatedJob.id,
    occurredAt: now,
    job: updatedJob,
    state: row.state,
  });
  return { ...stored, job: updatedJob, version: stored.version + 1 };
}

/** Persist a newer extend window so every broker observes the same deduplication metadata. */
export async function extendPostgresDeduplication(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresJobRow,
  ttl: number,
  now: number
): Promise<PostgresStoredJob> {
  const stored = decodePostgresJob(row);
  const updatedJob = { ...stored.job, deduplicationTtl: ttl };
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(updatedJob)}, unique_expires_at = ${now + ttl},
        version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
  `;
  return { ...stored, job: updatedJob, version: stored.version + 1 };
}

/** Publish deletion of a pending generation replaced within the same transaction. */
export async function recordPostgresDeduplicationRemoval(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresJobRow,
  now: number
): Promise<void> {
  const stored = decodePostgresJob(row);
  await recordPostgresEvent(tx, ctx, {
    queue: stored.job.queue,
    type: 'removed',
    jobId: stored.job.id,
    occurredAt: now,
    job: stored.job,
    state: stored.state,
    removed: true,
  });
}
