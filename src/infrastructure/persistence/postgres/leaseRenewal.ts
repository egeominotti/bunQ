import { jobId, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue, postgresByteaBase64 } from './codec';
import { databaseNow, type PostgresContext } from './context';
import type { PostgresJobRow, PostgresStoredJob } from './types';
import { lockPostgresBrokerSession } from './brokerSessions';

export interface PostgresLeaseRenewalInput {
  readonly id: JobId;
  readonly token: string;
  readonly durationMs: number;
}

export interface PostgresLeaseRenewalResult {
  readonly row: PostgresStoredJob;
  readonly token: string;
}

interface PreparedLeaseRenewal {
  readonly id: JobId;
  readonly token: string;
  readonly payload: Uint8Array;
  readonly leaseUntil: number;
  readonly row: PostgresStoredJob;
}

function renewalKey(id: JobId, token: string): string {
  return `${String(id)}\0${token}`;
}

function uniqueRenewals(inputs: readonly PostgresLeaseRenewalInput[]): PostgresLeaseRenewalInput[] {
  const unique = new Map<string, PostgresLeaseRenewalInput>();
  for (const input of inputs) {
    const key = renewalKey(input.id, input.token);
    const current = unique.get(key);
    if (!current || input.durationMs > current.durationMs) unique.set(key, input);
  }
  return [...unique.values()];
}

function prepareRenewal(
  ctx: PostgresContext,
  row: PostgresJobRow,
  input: PostgresLeaseRenewalInput,
  now: number
): PreparedLeaseRenewal {
  const stored = decodePostgresJob(row);
  stored.job.lastHeartbeat = now;
  const requestedLeaseUntil = now + Math.max(1, input.durationMs);
  const leaseUntil =
    row.timeout === null
      ? requestedLeaseUntil
      : Math.min(requestedLeaseUntil, Number(row.started_at ?? now) + Number(row.timeout));
  return {
    id: stored.job.id,
    token: input.token,
    payload: encodePostgresValue(stored.job),
    leaseUntil,
    row: {
      ...stored,
      leaseBrokerId: ctx.config.brokerId,
      leaseUntil,
      leaseRenewals: row.lease_renewals + 1,
      version: stored.version + 1,
    },
  };
}

function inputForRow(
  byKey: ReadonlyMap<string, PostgresLeaseRenewalInput>,
  row: PostgresJobRow
): PostgresLeaseRenewalInput {
  if (row.lease_token === null) {
    throw new Error(`PostgreSQL selected an unfenced active lease for job ${row.id}`);
  }
  const input = byKey.get(renewalKey(jobId(row.id), row.lease_token));
  if (!input) throw new Error(`PostgreSQL returned an unrequested lease for job ${row.id}`);
  return input;
}

/** Renew a fenced lease batch with one broker-session lock and one transaction. */
export async function renewPostgresLeases(
  ctx: PostgresContext,
  inputs: readonly PostgresLeaseRenewalInput[]
): Promise<PostgresLeaseRenewalResult[]> {
  const requested = uniqueRenewals(inputs);
  if (requested.length === 0) return [];
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresBrokerSession(tx, ctx);
    const now = await databaseNow(tx);
    const rows = await tx<PostgresJobRow[]>`
      SELECT job.*
      FROM bunqueue_jobs AS job
      JOIN unnest(
        ${tx.array(
          requested.map(({ id }) => String(id)),
          'TEXT'
        )},
        ${tx.array(
          requested.map(({ token }) => token),
          'TEXT'
        )}
      ) AS request(id, token)
        ON request.id = job.id AND request.token = job.lease_token
      WHERE job.namespace = ${ctx.config.namespace}
        AND job.state = 'active' AND job.lease_until > ${now}
      ORDER BY job.id
      FOR UPDATE OF job
    `;
    const byKey = new Map(requested.map((input) => [renewalKey(input.id, input.token), input]));
    const prepared = rows.map((row) => prepareRenewal(ctx, row, inputForRow(byKey, row), now));
    if (prepared.length === 0) return [];
    const updated = await tx<{ id: string }[]>`
      UPDATE bunqueue_jobs AS job
      SET payload = decode(batch.payload_base64, 'base64'),
          lease_until = batch.lease_until,
          lease_broker_id = ${ctx.config.brokerId},
          lease_broker_session_id = ${ctx.config.brokerSessionId},
          lease_renewals = job.lease_renewals + 1,
          version = job.version + 1
      FROM unnest(
        ${tx.array(
          prepared.map(({ id }) => String(id)),
          'TEXT'
        )},
        ${tx.array(
          prepared.map(({ token }) => token),
          'TEXT'
        )},
        ${tx.array(
          prepared.map(({ payload }) => postgresByteaBase64(payload)),
          'TEXT'
        )},
        ${tx.array(
          prepared.map(({ leaseUntil }) => leaseUntil),
          'BIGINT'
        )}
      ) AS batch(id, token, payload_base64, lease_until)
      WHERE job.namespace = ${ctx.config.namespace} AND job.id = batch.id
        AND job.state = 'active' AND job.lease_token = batch.token
      RETURNING job.id
    `;
    if (updated.length !== prepared.length) {
      throw new Error('PostgreSQL lease batch changed while its rows were locked');
    }
    return prepared.map(({ row, token }) => ({ row, token }));
  });
}

/** Renew one fenced lease and transfer its broker responsibility atomically. */
export async function renewPostgresLease(
  ctx: PostgresContext,
  id: JobId,
  token: string,
  durationMs: number
): Promise<number | null> {
  const renewed = await renewPostgresLeases(ctx, [{ id, token, durationMs }]);
  return renewed[0]?.row.leaseUntil ?? null;
}
