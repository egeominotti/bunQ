import type { TransactionSQL } from 'bun';
import { buildRepeatSuccessor } from '../../../application/repeatJobs';
import {
  createJob,
  generateJobId,
  MAX_TIMELINE_ENTRIES,
  type JobId,
} from '../../../domain/types/job';
import { admitPostgresJob } from './admission';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import { planPostgresDependencyCompletion } from './dependencyPromotion';
import { clearPostgresParentFlowFailures } from './flowFailures';
import type { PostgresJobRow, PostgresTransitionResult } from './types';
import { runPostgresTransactionWithRetry } from './transactionRetry';
import { tryLockPostgresRepeatQueues } from './queueLifecycle';

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

export async function completePostgresJob(
  ctx: PostgresContext,
  id: JobId,
  token: string,
  result: unknown,
  removeOnComplete = false
): Promise<PostgresTransitionResult> {
  return await runPostgresTransactionWithRetry(ctx, async (tx) => {
    const dependencyCompletion = await planPostgresDependencyCompletion(tx, ctx, [id]);
    const row = await lockActiveJob(tx, ctx, id);
    const now = await databaseNow(tx);
    if (
      row?.state !== 'active' ||
      row.lease_token !== token ||
      Number(row.lease_until ?? 0) <= now
    ) {
      return notApplied(row);
    }
    const job = decodePostgresJob(row).job;
    job.completedAt = now;
    job.progress = 100;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'completed', timestamp: now });
    }
    const resultPayload = encodePostgresValue(result);
    await clearPostgresParentFlowFailures(tx, ctx, id);
    await tx`
      INSERT INTO bunqueue_completions (namespace, job_id, queue, completed_at, result)
      VALUES (${ctx.config.namespace}, ${String(id)}, ${job.queue}, ${now}, ${resultPayload})
      ON CONFLICT (namespace, job_id) DO UPDATE SET
        queue = excluded.queue, completed_at = excluded.completed_at, result = excluded.result
    `;
    const removed = job.removeOnComplete || removeOnComplete;
    if (removed) {
      await deleteLiveJob(tx, ctx, id);
    } else {
      await tx`
        UPDATE bunqueue_jobs
        SET payload = ${encodePostgresValue(job)}, state = 'completed', completed_at = ${now},
            result = ${resultPayload}, lease_owner = NULL, lease_token = NULL,
            lease_broker_id = NULL, lease_broker_session_id = NULL,
            lease_until = NULL, dlq_retry_state = NULL,
            version = version + 1
        WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
      `;
    }
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'completed',
      jobId: id,
      occurredAt: now,
      job,
      state: 'completed',
      result,
      removed,
      dlqEntry: null,
      dlqRetryState: null,
    });
    await dependencyCompletion.promote(now);
    const shouldRepeat =
      job.repeat !== null &&
      (job.repeat.limit === undefined || job.repeat.count < job.repeat.limit);
    const successorInput = shouldRepeat ? buildRepeatSuccessor(job, now) : null;
    if (successorInput) {
      if (!(await tryLockPostgresRepeatQueues(tx, ctx, [job.queue]))) {
        throw new Error(`Queue ${job.queue} is being obliterated; retry completion`);
      }
      const successor = createJob(generateJobId(), job.queue, successorInput, now);
      const admitted = await admitPostgresJob(tx, ctx, successor, {
        linkParent: false,
        queueLifecycleLocksHeld: true,
      });
      await tx`
        INSERT INTO bunqueue_repeat_links (namespace, original_id, successor_id)
        VALUES (${ctx.config.namespace}, ${String(job.id)}, ${String(admitted.job.id)})
        ON CONFLICT (namespace, original_id) DO UPDATE SET successor_id = excluded.successor_id
      `;
    }
    return { applied: true, alreadyFinalized: false, job, state: 'completed' };
  });
}
