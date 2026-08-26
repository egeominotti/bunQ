import type { TransactionSQL } from 'bun';
import {
  calculateBackoff,
  canRetry,
  MAX_TIMELINE_ENTRIES,
  normalizeStacktrace,
  type Job,
  type JobId,
} from '../../../domain/types/job';
import {
  createDlqEntry,
  FailureReason,
  getDlqRetryState,
  recordJobFailureAttempt,
  type DlqEntry,
} from '../../../domain/types/dlq';
import { decodePostgresJob, encodePostgresValue } from './codec';
import {
  databaseNow,
  recordPostgresEvent,
  runPostgresPostCommitMaintenance,
  type PostgresContext,
} from './context';
import {
  applyPostgresFlowFailure,
  clearPostgresParentFlowFailures,
  lockPostgresJobFlowParent,
} from './flowFailures';
import { enforcePostgresDlqLimit } from './dlqLifecycle';
import { getPostgresQueuePolicies } from './policies';
import type { PostgresFailureInput, PostgresJobRow, PostgresTransitionResult } from './types';
import { partitionPostgresDestructionCandidates } from './dependencyDestruction';

export { completePostgresJob } from './completionOutcome';

async function lockActiveJob(
  tx: TransactionSQL,
  ctx: PostgresContext,
  id: JobId
): Promise<PostgresJobRow | null> {
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    FOR UPDATE
  `;
  return rows[0] ?? null;
}

function notApplied(row: PostgresJobRow | null): PostgresTransitionResult {
  return {
    applied: false,
    alreadyFinalized: row?.state === 'completed' || row?.state === 'failed',
    job: row ? decodePostgresJob(row).job : null,
    state: row?.state ?? null,
  };
}

function assertToken(row: PostgresJobRow, token: string, now: number): boolean {
  return row.state === 'active' && row.lease_token === token && Number(row.lease_until ?? 0) > now;
}

async function deleteLiveJob(tx: TransactionSQL, ctx: PostgresContext, id: JobId): Promise<void> {
  await tx`
    DELETE FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
  `;
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
  `;
  await tx`
    DELETE FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
  `;
}

function failureReason(value: string | undefined, terminal: boolean): FailureReason {
  switch (value) {
    case FailureReason.ExplicitFail:
    case FailureReason.MaxAttemptsExceeded:
    case FailureReason.Timeout:
    case FailureReason.Stalled:
    case FailureReason.TtlExpired:
    case FailureReason.WorkerLost:
    case FailureReason.Unknown:
      return value;
  }
  return terminal ? FailureReason.MaxAttemptsExceeded : FailureReason.ExplicitFail;
}

async function persistRetry(
  tx: TransactionSQL,
  ctx: PostgresContext,
  job: Job,
  now: number
): Promise<'waiting' | 'prioritized' | 'delayed'> {
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
    type: 'retried',
    jobId: job.id,
    occurredAt: now,
    job,
    state,
    dlqEntry: null,
    dlqRetryState: getDlqRetryState(job),
  });
  return state;
}

async function persistTerminalFailure(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: {
    job: Job;
    reason: FailureReason;
    error: string | undefined;
    now: number;
    remove: boolean;
  }
): Promise<{ dlqEntry: DlqEntry | null; maxEntries: number | null; removed: boolean }> {
  const { job, reason, error, now, remove } = input;
  const { dlq } = await getPostgresQueuePolicies(tx, ctx, job.queue);
  const entry = createDlqEntry(job, reason, error ?? null, dlq);
  const { protectedIds } = remove
    ? await partitionPostgresDestructionCandidates(tx, ctx, [job.id], [job.id])
    : { protectedIds: [] };
  const removed = remove && protectedIds.length === 0;
  if (removed) {
    await deleteLiveJob(tx, ctx, job.id);
  } else {
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(job)}, state = 'failed', attempts = ${job.attempts},
          completed_at = ${now}, lease_owner = NULL, lease_token = NULL, lease_until = NULL,
          lease_broker_id = NULL,
          error = ${error ?? null}, failure_reason = ${reason},
          dlq_entry = ${encodePostgresValue(entry)}, dlq_retry_state = NULL,
          version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(job.id)}
    `;
  }
  return {
    dlqEntry: removed ? null : entry,
    maxEntries: removed ? null : dlq.maxEntries,
    removed,
  };
}

export async function failPostgresJob(
  ctx: PostgresContext,
  input: PostgresFailureInput
): Promise<PostgresTransitionResult> {
  const result = await ctx.sql.begin(async (tx) => {
    await lockPostgresJobFlowParent(tx, ctx, input.id);
    const row = await lockActiveJob(tx, ctx, input.id);
    const now = await databaseNow(tx);
    if (!row || !assertToken(row, input.token, now)) {
      return { transition: notApplied(row), dlqLimits: new Map<string, number>() };
    }
    const job = decodePostgresJob(row).job;
    job.attempts++;
    job.stacktrace = normalizeStacktrace(input.stack, job.stackTraceLimit);
    const willRetry = !input.unrecoverable && canRetry(job);
    const reason = failureReason(input.failureReason, !willRetry);
    let nextState: PostgresTransitionResult['state'] = 'failed';
    let dlqEntry: DlqEntry | null = null;
    let dlqMaxEntries: number | null = null;
    let removed = false;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({
        state: 'failed',
        timestamp: now,
        error: input.error,
        attempt: job.attempts,
      });
    }
    if (willRetry) {
      recordJobFailureAttempt(job, reason, input.error ?? null, now);
      nextState = await persistRetry(tx, ctx, job, now);
    } else {
      job.completedAt = now;
      const persisted = await persistTerminalFailure(tx, ctx, {
        job,
        reason,
        error: input.error,
        now,
        remove: job.removeOnFail || input.removeOnFail === true,
      });
      dlqEntry = persisted.dlqEntry;
      dlqMaxEntries = persisted.maxEntries;
      removed = persisted.removed;
    }
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'failed',
      jobId: job.id,
      occurredAt: now,
      job,
      state: nextState,
      error: input.error,
      removed,
      dlqEntry,
      dlqRetryState: willRetry ? getDlqRetryState(job) : null,
    });
    const dlqLimits = new Map<string, number>();
    if (!willRetry) {
      await clearPostgresParentFlowFailures(tx, ctx, job.id);
      const request = await applyPostgresFlowFailure(tx, ctx, job, input.error, now);
      if (request) dlqLimits.set(request.queue, request.maxEntries);
    }
    if (dlqMaxEntries !== null) {
      dlqLimits.set(job.queue, dlqMaxEntries);
    }
    return {
      transition: {
        applied: true,
        alreadyFinalized: false,
        job,
        state: nextState,
      } satisfies PostgresTransitionResult,
      dlqLimits,
    };
  });
  for (const [queue, maxEntries] of [...result.dlqLimits].sort(([left], [right]) =>
    left.localeCompare(right)
  )) {
    await runPostgresPostCommitMaintenance(ctx, `dlq-limit:${queue}`, () =>
      enforcePostgresDlqLimit(ctx, queue, maxEntries)
    );
  }
  return result.transition;
}
