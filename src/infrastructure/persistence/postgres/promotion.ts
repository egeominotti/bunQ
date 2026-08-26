import { jobId, MAX_TIMELINE_ENTRIES, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import type { PostgresJobRow } from './types';

export async function promotePostgresJobs(
  ctx: PostgresContext,
  queue: string,
  count?: number
): Promise<JobId[]> {
  const limit = count === undefined ? 2_147_483_647 : Math.max(0, count);
  if (limit === 0) return [];
  return await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND queue = ${queue}
        AND state = 'delayed'
      ORDER BY created_at, id
      LIMIT ${limit}
      FOR UPDATE SKIP LOCKED
    `;
    for (const row of rows) {
      const job = decodePostgresJob(row).job;
      job.runAt = now;
      const state = job.priority > 0 ? 'prioritized' : 'waiting';
      if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
        job.timeline.push({ state, timestamp: now });
      }
      await tx`
        UPDATE bunqueue_jobs
        SET payload = ${encodePostgresValue(job)}, run_at = ${now}, state = ${state},
            version = version + 1
        WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
      `;
      await recordPostgresEvent(tx, ctx, {
        queue,
        type: 'retried',
        jobId: job.id,
        occurredAt: now,
        job,
        state,
      });
    }
    return rows.map((row) => jobId(row.id));
  });
}
