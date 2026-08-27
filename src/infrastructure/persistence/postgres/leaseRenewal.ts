import type { JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, type PostgresContext } from './context';
import type { PostgresJobRow } from './types';
import { lockPostgresBrokerSession } from './brokerSessions';

/** Renew one fenced lease and transfer its broker responsibility atomically. */
export async function renewPostgresLease(
  ctx: PostgresContext,
  id: JobId,
  token: string,
  durationMs: number
): Promise<number | null> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresBrokerSession(tx, ctx);
    const now = await databaseNow(tx);
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
        AND state = 'active' AND lease_token = ${token} AND lease_until > ${now}
      FOR UPDATE
    `;
    const row = rows[0];
    if (!row) return null;
    const job = decodePostgresJob(row).job;
    job.lastHeartbeat = now;
    const requestedLeaseUntil = now + Math.max(1, durationMs);
    const leaseUntil =
      row.timeout === null
        ? requestedLeaseUntil
        : Math.min(requestedLeaseUntil, Number(row.started_at ?? now) + Number(row.timeout));
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(job)}, lease_until = ${leaseUntil},
          lease_broker_id = ${ctx.config.brokerId},
          lease_broker_session_id = ${ctx.config.brokerSessionId},
          lease_renewals = lease_renewals + 1,
          version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    return leaseUntil;
  });
}
