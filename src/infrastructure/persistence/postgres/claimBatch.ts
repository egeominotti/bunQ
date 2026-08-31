import type { TransactionSQL } from 'bun';
import { generateLockToken, MAX_TIMELINE_ENTRIES, type Job } from '../../../domain/types/job';
import type { GroupPullOptions } from '../../../domain/types/group';
import { recordPostgresJobEvents } from './batchEvents';
import { selectPostgresClaimCandidates } from './claimSelection';
import { decodePostgresJob, encodePostgresValue, postgresByteaBase64 } from './codec';
import type { PostgresContext } from './context';
import type { ClaimedPostgresJob, PostgresJobRow } from './types';
import { commitPostgresGroupClaims } from './groupClaims';

function prepareClaims(
  rows: readonly PostgresJobRow[],
  input: {
    owner: string;
    requestedLeaseMs: number;
    now: number;
    brokerId: string;
  }
): ClaimedPostgresJob[] {
  const { owner, requestedLeaseMs, now, brokerId } = input;
  return rows.map((row) => {
    const job: Job = decodePostgresJob(row).job;
    const token = String(generateLockToken());
    const leaseDuration = Math.max(
      1,
      Math.min(
        requestedLeaseMs,
        job.timeout === null ? requestedLeaseMs : job.timeout,
        job.stallTimeout === null ? requestedLeaseMs : job.stallTimeout
      )
    );
    const leaseUntil = now + leaseDuration;
    job.startedAt = now;
    job.lastHeartbeat = now;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'active', timestamp: now, worker: owner });
    }
    return { job, token, owner, brokerId, leaseUntil };
  });
}

async function persistClaims(
  tx: TransactionSQL,
  ctx: PostgresContext,
  claimed: readonly ClaimedPostgresJob[],
  now: number
): Promise<void> {
  await tx`
    UPDATE bunqueue_jobs AS job
    SET state = 'active',
        payload = decode(batch.payload_base64, 'base64'),
        started_at = ${now},
        lease_owner = ${claimed[0].owner},
        lease_broker_id = ${ctx.config.brokerId},
        lease_broker_session_id = ${ctx.config.brokerSessionId},
        lease_token = batch.token,
        lease_until = batch.lease_until,
        lease_renewals = 0,
        version = job.version + 1
    FROM unnest(
      ${tx.array(
        claimed.map(({ job }) => String(job.id)),
        'TEXT'
      )},
      ${tx.array(
        claimed.map(({ job }) => postgresByteaBase64(encodePostgresValue(job))),
        'TEXT'
      )},
      ${tx.array(
        claimed.map(({ token }) => token),
        'TEXT'
      )},
      ${tx.array(
        claimed.map(({ leaseUntil }) => leaseUntil),
        'BIGINT'
      )}
    ) AS batch(id, payload_base64, token, lease_until)
    WHERE job.namespace = ${ctx.config.namespace} AND job.id = batch.id
  `;
  await recordPostgresJobEvents(
    tx,
    ctx,
    claimed.map(({ job }) => ({ job, type: 'pulled', state: 'active' })),
    now
  );
}

/** Claim one queue batch after its queue-state row has been locked. */
export async function claimPostgresCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: {
    queue: string;
    capacity: number;
    owner: string;
    requestedLeaseMs: number;
    now: number;
    groupOptions?: GroupPullOptions;
    groupSequence: number;
  }
): Promise<ClaimedPostgresJob[]> {
  const { queue, capacity, owner, requestedLeaseMs, now, groupOptions, groupSequence } = input;
  const rows = await selectPostgresClaimCandidates(tx, ctx, {
    queue,
    now,
    capacity,
    options: groupOptions,
  });
  const claimed = prepareClaims(rows, {
    owner,
    requestedLeaseMs,
    now,
    brokerId: ctx.config.brokerId,
  });
  if (claimed.length > 0) {
    await commitPostgresGroupClaims(tx, ctx, {
      queue,
      rows,
      options: groupOptions,
      sequence: groupSequence,
    });
    await persistClaims(tx, ctx, claimed, now);
  }
  return claimed;
}
