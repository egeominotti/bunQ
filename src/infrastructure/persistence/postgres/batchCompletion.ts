import type { TransactionSQL } from 'bun';
import { createJob, generateJobId, MAX_TIMELINE_ENTRIES } from '../../../domain/types/job';
import { buildRepeatSuccessor } from '../../../application/repeatJobs';
import { decodePostgresJob, encodePostgresValue, postgresByteaBase64 } from './codec';
import { databaseNow, type PostgresContext } from './context';
import { admitPostgresJob } from './admission';
import { recordPostgresCompletionEvents } from './completionEvents';
import { planPostgresDependencyCompletion } from './dependencyPromotion';
import type {
  PostgresBatchCompletionResult,
  PostgresCompletionInput,
  PostgresJobRow,
} from './types';
import { tryLockPostgresRepeatQueues } from './queueLifecycle';

interface AppliedCompletion {
  readonly input: PostgresCompletionInput;
  readonly job: ReturnType<typeof decodePostgresJob>['job'];
  readonly payload: Uint8Array;
  readonly resultPayload: Uint8Array;
  readonly removed: boolean;
}

function invalidToken(id: string): Error {
  return new Error(`Invalid or expired lock token for job ${id}`);
}

function prepareCompletions(
  rows: readonly PostgresJobRow[],
  inputs: readonly PostgresCompletionInput[],
  now: number
): { applied: AppliedCompletion[]; results: PostgresBatchCompletionResult[] } {
  const byId = new Map(rows.map((row) => [row.id, row]));
  const applied: AppliedCompletion[] = [];
  const results = inputs.map((input): PostgresBatchCompletionResult => {
    const row = byId.get(String(input.id));
    if (!row) throw invalidToken(String(input.id));
    if (row.state === 'completed' || row.state === 'failed') {
      return {
        applied: false,
        alreadyFinalized: true,
        job: decodePostgresJob(row).job,
        state: row.state,
        removed: false,
      };
    }
    if (
      row.state !== 'active' ||
      row.lease_token !== input.token ||
      Number(row.lease_until ?? 0) <= now
    ) {
      throw invalidToken(String(input.id));
    }
    const job = decodePostgresJob(row).job;
    job.completedAt = now;
    job.progress = 100;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'completed', timestamp: now });
    }
    const removed = job.removeOnComplete || input.removeOnComplete === true;
    applied.push({
      input,
      job,
      payload: encodePostgresValue(job),
      resultPayload: encodePostgresValue(input.result),
      removed,
    });
    return { applied: true, alreadyFinalized: false, job, state: 'completed', removed };
  });
  return { applied, results };
}

async function lockRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly PostgresCompletionInput[]
): Promise<PostgresJobRow[]> {
  const ids = inputs.map((input) => String(input.id)).sort();
  return await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace}
      AND id = ANY(${tx.array(ids, 'TEXT')})
    ORDER BY id
    FOR UPDATE
  `;
}

async function storeResults(
  tx: TransactionSQL,
  ctx: PostgresContext,
  applied: readonly AppliedCompletion[],
  now: number
): Promise<void> {
  await tx`
    INSERT INTO bunqueue_completions (namespace, job_id, queue, completed_at, result)
    SELECT
      ${ctx.config.namespace}, batch.id, batch.queue, ${now},
      decode(batch.result_base64, 'base64')
    FROM unnest(
      ${tx.array(
        applied.map(({ job }) => String(job.id)),
        'TEXT'
      )},
      ${tx.array(
        applied.map(({ job }) => job.queue),
        'TEXT'
      )},
      ${tx.array(
        applied.map(({ resultPayload }) => postgresByteaBase64(resultPayload)),
        'TEXT'
      )}
    ) AS batch(id, queue, result_base64)
    ON CONFLICT (namespace, job_id) DO UPDATE SET
      queue = excluded.queue,
      completed_at = excluded.completed_at,
      result = excluded.result
  `;
}

async function updateRetainedJobs(
  tx: TransactionSQL,
  ctx: PostgresContext,
  applied: readonly AppliedCompletion[],
  now: number
): Promise<void> {
  const retained = applied.filter((item) => !item.removed);
  if (retained.length === 0) return;
  await tx`
    UPDATE bunqueue_jobs AS job
    SET payload = decode(batch.payload_base64, 'base64'),
        state = 'completed',
        completed_at = ${now},
        result = decode(batch.result_base64, 'base64'),
        lease_owner = NULL,
        lease_token = NULL,
        lease_broker_id = NULL,
        lease_until = NULL,
        dlq_retry_state = NULL,
        version = job.version + 1
    FROM unnest(
      ${tx.array(
        retained.map(({ job }) => String(job.id)),
        'TEXT'
      )},
      ${tx.array(
        retained.map(({ payload }) => postgresByteaBase64(payload)),
        'TEXT'
      )},
      ${tx.array(
        retained.map(({ resultPayload }) => postgresByteaBase64(resultPayload)),
        'TEXT'
      )}
    ) AS batch(id, payload_base64, result_base64)
    WHERE job.namespace = ${ctx.config.namespace} AND job.id = batch.id
  `;
}

async function deleteRemovedJobs(
  tx: TransactionSQL,
  ctx: PostgresContext,
  applied: readonly AppliedCompletion[]
): Promise<void> {
  const ids = applied.filter((item) => item.removed).map(({ job }) => String(job.id));
  if (ids.length === 0) return;
  await tx`
    DELETE FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array(ids, 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array(ids, 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ANY(${tx.array(ids, 'TEXT')})
  `;
}

async function createRepeatSuccessors(
  tx: TransactionSQL,
  ctx: PostgresContext,
  applied: readonly AppliedCompletion[],
  now: number
): Promise<void> {
  for (const { job } of applied) {
    const shouldRepeat =
      job.repeat !== null &&
      (job.repeat.limit === undefined || job.repeat.count < job.repeat.limit);
    const successorInput = shouldRepeat ? buildRepeatSuccessor(job, now) : null;
    if (!successorInput) continue;
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
}

/** Complete a unique ACK batch atomically with set-based persistence. */
export async function completePostgresJobs(
  ctx: PostgresContext,
  inputs: readonly PostgresCompletionInput[]
): Promise<PostgresBatchCompletionResult[]> {
  if (inputs.length === 0) return [];
  if (new Set(inputs.map((input) => String(input.id))).size !== inputs.length) {
    throw new Error('PostgreSQL batch completion requires unique job IDs');
  }
  return await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    const dependencyCompletion = await planPostgresDependencyCompletion(
      tx,
      ctx,
      inputs.map(({ id }) => id)
    );
    const rows = await lockRows(tx, ctx, inputs);
    const { applied, results } = prepareCompletions(rows, inputs, now);
    if (applied.length === 0) return results;
    const repeatQueues = applied.flatMap(({ job }) => {
      const repeats =
        job.repeat !== null &&
        (job.repeat.limit === undefined || job.repeat.count < job.repeat.limit);
      return repeats ? [job.queue] : [];
    });
    if (!(await tryLockPostgresRepeatQueues(tx, ctx, repeatQueues))) {
      throw new Error('A repeat queue is being obliterated; retry batch completion');
    }
    const ids = applied.map(({ job }) => String(job.id));
    await tx`
      DELETE FROM bunqueue_flow_failures
      WHERE namespace = ${ctx.config.namespace} AND parent_id = ANY(${tx.array(ids, 'TEXT')})
    `;
    await storeResults(tx, ctx, applied, now);
    await updateRetainedJobs(tx, ctx, applied, now);
    await deleteRemovedJobs(tx, ctx, applied);
    await recordPostgresCompletionEvents(
      tx,
      ctx,
      applied.map(({ job, input, removed }) => ({ job, result: input.result, removed })),
      now
    );
    await dependencyCompletion.promote(now);
    await createRepeatSuccessors(tx, ctx, applied, now);
    return results;
  });
}
