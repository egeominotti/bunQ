import type { JobId } from '../../../domain/types/job';
import type { JobLogEntry } from '../../../domain/types/worker';
import { databaseNow, type PostgresContext } from './context';

export async function addPostgresJobLog(
  ctx: PostgresContext,
  id: JobId,
  message: string,
  level: JobLogEntry['level'],
  maxLogs = 100
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const jobs = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
      FOR UPDATE
    `;
    if (!jobs[0]) return false;
    const now = await databaseNow(tx);
    const rows = await tx<{ id: number | string | bigint }[]>`
      INSERT INTO bunqueue_job_logs (namespace, job_id, timestamp, level, message)
      VALUES (${ctx.config.namespace}, ${String(id)}, ${now}, ${level}, ${message})
      RETURNING id
    `;
    if (!rows[0]) return false;
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
        AND id NOT IN (
          SELECT id FROM bunqueue_job_logs
          WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
          ORDER BY id DESC
          LIMIT ${Math.max(1, maxLogs)}
        )
    `;
    return true;
  });
}

export async function getPostgresJobLogs(ctx: PostgresContext, id: JobId): Promise<JobLogEntry[]> {
  const rows = await ctx.sql<
    Array<{
      timestamp: number | string | bigint;
      level: JobLogEntry['level'];
      message: string;
    }>
  >`
    SELECT timestamp, level, message
    FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    ORDER BY id
  `;
  return rows.map((row) => ({
    timestamp: Number(row.timestamp),
    level: row.level,
    message: row.message,
  }));
}

export async function clearPostgresJobLogs(
  ctx: PostgresContext,
  id: JobId,
  keepLogs?: number
): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    await tx`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
      FOR UPDATE
    `;
    if (keepLogs === undefined || keepLogs <= 0) {
      await tx`
        DELETE FROM bunqueue_job_logs
        WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
      `;
      return;
    }
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
        AND id NOT IN (
          SELECT id FROM bunqueue_job_logs
          WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
          ORDER BY id DESC
          LIMIT ${Math.floor(keepLogs)}
        )
    `;
  });
}
