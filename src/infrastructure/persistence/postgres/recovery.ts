import type { TransactionSQL } from 'bun';
import { calculateBackoff, MAX_TIMELINE_ENTRIES, type Job } from '../../../domain/types/job';
import {
  createDlqEntry,
  FailureReason,
  getDlqRetryState,
  recordJobFailureAttempt,
} from '../../../domain/types/dlq';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import {
  applyPostgresFlowFailure,
  clearPostgresParentFlowFailures,
  lockPostgresFlowParents,
} from './flowFailures';
import { getPostgresQueuePolicies } from './policies';
import { enforcePostgresDlqLimit } from './dlqLifecycle';
import { discardPostgresProtectedCronLease } from './leaseRelease';
import type { PostgresJobRow } from './types';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';
import { partitionPostgresDestructionCandidates } from './dependencyDestruction';

async function retryExpiredLease(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: { job: Job; now: number; reason: FailureReason; error: string }
): Promise<void> {
  const { job, now, reason, error } = input;
  recordJobFailureAttempt(job, reason, error, now);
  job.runAt = now + calculateBackoff(job);
  job.startedAt = null;
  const state = job.runAt > now ? 'delayed' : job.priority > 0 ? 'prioritized' : 'waiting';
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state, timestamp: now, attempt: job.attempts + 1 });
  }
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(job)}, state = ${state}, attempts = ${job.attempts},
        run_at = ${job.runAt}, started_at = NULL, lease_owner = NULL, lease_token = NULL,
        lease_broker_id = NULL, lease_until = NULL, error = NULL, failure_reason = NULL,
        dlq_entry = NULL,
        dlq_retry_state = ${encodePostgresValue(getDlqRetryState(job))},
        version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(job.id)}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: reason === FailureReason.Stalled ? 'stalled' : 'failed',
    jobId: job.id,
    occurredAt: now,
    job,
    state,
    error,
    dlqEntry: null,
    dlqRetryState: getDlqRetryState(job),
  });
}

async function failExpiredLease(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: { job: Job; now: number; reason: FailureReason; error: string }
): Promise<number | null> {
  const { job, now, reason, error } = input;
  job.completedAt = now;
  const { dlq } = await getPostgresQueuePolicies(tx, ctx, job.queue);
  const entry = createDlqEntry(job, reason, error, dlq);
  const { protectedIds } = job.removeOnFail
    ? await partitionPostgresDestructionCandidates(tx, ctx, [job.id], [job.id])
    : { protectedIds: [] };
  const removed = job.removeOnFail && protectedIds.length === 0;
  if (removed) {
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(job.id)}
    `;
    await tx`
      DELETE FROM bunqueue_dependencies
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(job.id)}
    `;
    await tx`
      DELETE FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(job.id)}
    `;
  } else {
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(job)}, state = 'failed', attempts = ${job.attempts},
          completed_at = ${now}, lease_owner = NULL, lease_token = NULL, lease_until = NULL,
          lease_broker_id = NULL,
          error = ${error}, failure_reason = ${reason},
          dlq_entry = ${encodePostgresValue(entry)}, dlq_retry_state = NULL,
          version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(job.id)}
    `;
  }
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: 'failed',
    jobId: job.id,
    occurredAt: now,
    job,
    state: 'failed',
    error,
    removed,
    dlqEntry: removed ? null : entry,
    dlqRetryState: null,
  });
  return removed ? null : dlq.maxEntries;
}

/** Reclaim expired active leases. Row locks make the sweep safe across many brokers. */
export async function recoverExpiredPostgresLeases(
  ctx: PostgresContext,
  limit = 100
): Promise<number> {
  const result = await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    const initialCandidates = await tx<Array<{ id: string }>>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND state = 'active'
        AND lease_until <= ${now}
      ORDER BY lease_until, id
      LIMIT ${Math.max(1, limit)}
    `;
    if (initialCandidates.length === 0) {
      return { count: 0, dlqLimits: new Map<string, number>() };
    }
    await lockPostgresDependencyCompletions(
      tx,
      ctx,
      initialCandidates.map(({ id }) => id as Job['id'])
    );
    const candidates = await tx<Array<{ id: string; parent_id: string | null }>>`
      SELECT id, parent_id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND id = ANY(${tx.array(
          initialCandidates.map(({ id }) => id),
          'TEXT'
        )})
        AND state = 'active'
        AND lease_until <= ${now}
      ORDER BY lease_until, id
    `;
    const parentIds = [
      ...new Set(candidates.flatMap((row) => (row.parent_id ? [row.parent_id] : []))),
    ].sort();
    await lockPostgresFlowParents(
      tx,
      ctx,
      parentIds.map((id) => id as Job['id'])
    );
    if (candidates.length === 0) {
      return { count: 0, dlqLimits: new Map<string, number>() };
    }
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND id = ANY(${tx.array(
          candidates.map((row) => row.id),
          'TEXT'
        )})
        AND state = 'active'
        AND lease_until <= ${now}
      ORDER BY lease_until, id
      LIMIT ${Math.max(1, limit)}
      FOR UPDATE SKIP LOCKED
    `;
    const dlqLimits = new Map<string, number>();
    for (const row of rows) {
      if (await discardPostgresProtectedCronLease(tx, ctx, row, now)) continue;
      const job = decodePostgresJob(row).job;
      const timedOut =
        job.timeout !== null && job.startedAt !== null && job.startedAt + job.timeout <= now;
      const { stall } = await getPostgresQueuePolicies(tx, ctx, job.queue);
      job.attempts++;
      if (!timedOut) job.stallCount++;
      const attemptsExhausted = job.attempts >= job.maxAttempts;
      const reason = timedOut
        ? FailureReason.Timeout
        : attemptsExhausted
          ? FailureReason.MaxAttemptsExceeded
          : FailureReason.Stalled;
      const error = timedOut ? 'Job processing timed out' : 'PostgreSQL lease expired';
      if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
        job.timeline.push({
          state: 'failed',
          timestamp: now,
          error,
          attempt: job.attempts,
        });
      }
      const failure = { job, now, reason, error };
      const stallsExhausted = !timedOut && stall.maxStalls > 0 && job.stallCount >= stall.maxStalls;
      if (!attemptsExhausted && !stallsExhausted) await retryExpiredLease(tx, ctx, failure);
      else {
        const maxEntries = await failExpiredLease(tx, ctx, failure);
        await clearPostgresParentFlowFailures(tx, ctx, job.id);
        const request = await applyPostgresFlowFailure(tx, ctx, job, error, now);
        if (maxEntries !== null) dlqLimits.set(job.queue, maxEntries);
        if (request) dlqLimits.set(request.queue, request.maxEntries);
      }
    }
    return { count: rows.length, dlqLimits };
  });
  for (const [queue, maxEntries] of [...result.dlqLimits].sort(([left], [right]) =>
    left.localeCompare(right)
  )) {
    await enforcePostgresDlqLimit(ctx, queue, maxEntries);
  }
  return result.count;
}

export async function heartbeatPostgresBroker(ctx: PostgresContext): Promise<void> {
  const now = await databaseNow(ctx.sql);
  await ctx.sql`
    INSERT INTO bunqueue_brokers (namespace, broker_id, started_at, heartbeat_at)
    VALUES (${ctx.config.namespace}, ${ctx.config.brokerId}, ${now}, ${now})
    ON CONFLICT (namespace, broker_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at
  `;
}

export async function unregisterPostgresBroker(ctx: PostgresContext): Promise<void> {
  await ctx.sql`
    DELETE FROM bunqueue_brokers
    WHERE namespace = ${ctx.config.namespace} AND broker_id = ${ctx.config.brokerId}
  `;
}
