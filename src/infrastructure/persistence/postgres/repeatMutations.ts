import type { TransactionSQL } from 'bun';
import type { JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import { updatedPostgresJobData } from './jobData';
import type { PostgresJobRow } from './types';

/** Update the pending successor addressed through a completed repeat generation. */
export async function updatePostgresRepeatSuccessorData(
  tx: TransactionSQL,
  ctx: PostgresContext,
  originalId: JobId,
  data: unknown
): Promise<boolean> {
  const links = await tx<{ successor_id: string }[]>`
    SELECT successor_id FROM bunqueue_repeat_links
    WHERE namespace = ${ctx.config.namespace} AND original_id = ${String(originalId)}
  `;
  if (!links[0]) return false;
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${links[0].successor_id}
    FOR UPDATE
  `;
  const row = rows[0];
  if (!row || ['active', 'completed', 'failed'].includes(row.state)) return false;
  const job = decodePostgresJob(row).job;
  const updated = { ...job, data: updatedPostgresJobData(job, data) };
  const now = await databaseNow(tx);
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(updated)}, version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: 'updated',
    jobId: job.id,
    occurredAt: now,
    job: updated,
    state: row.state,
  });
  return true;
}
