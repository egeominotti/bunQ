import type { TransactionSQL } from 'bun';
import type { Job } from '../../../domain/types/job';
import {
  decodePostgresJob,
  encodePostgresValue,
  eventPayload,
  postgresByteaBase64,
  postgresStateForJob,
} from './codec';
import { databaseNow, type PostgresContext } from './context';
import type { PostgresAdmissionResult } from './admissionResult';
import type { PostgresJobRow, PostgresJobState } from './types';

interface ReconciliationRow extends PostgresJobRow {
  readonly dependencies_satisfied: boolean;
}

interface PreparedReconciliation {
  readonly id: string;
  readonly job: Job;
  readonly state: PostgresJobState;
  readonly payload: string;
  readonly eventId: number;
  readonly eventPayload: string;
  readonly insertionIndex: number;
}

function latestInsertions(admissions: readonly PostgresAdmissionResult[]): Map<string, number> {
  const latest = new Map<string, number>();
  for (let index = 0; index < admissions.length; index++) {
    if (admissions[index].inserted) latest.set(String(admissions[index].job.id), index);
  }
  return latest;
}

function correctedJob(
  row: ReconciliationRow,
  admission: PostgresAdmissionResult,
  now: number
): { job: Job; state: PostgresJobState } {
  const stored = decodePostgresJob(row);
  const state = postgresStateForJob(stored.job, row.dependencies_satisfied, now);
  if (admission.timelineIndex === undefined) return { job: stored.job, state };
  const timeline = [...stored.job.timeline];
  const entry = timeline[admission.timelineIndex];
  if (entry) timeline[admission.timelineIndex] = { ...entry, state };
  return { job: { ...stored.job, timeline }, state };
}

async function loadFinalRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly string[]
): Promise<ReconciliationRow[]> {
  return await tx<ReconciliationRow[]>`
    SELECT job.*, NOT EXISTS (
      SELECT 1
      FROM bunqueue_dependencies AS dependency
      WHERE dependency.namespace = job.namespace AND dependency.job_id = job.id
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_completions AS completion
          WHERE completion.namespace = dependency.namespace
            AND completion.job_id = dependency.dependency_id
        )
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_jobs AS completed
          WHERE completed.namespace = dependency.namespace
            AND completed.id = dependency.dependency_id AND completed.state = 'completed'
        )
    ) AS dependencies_satisfied
    FROM bunqueue_jobs AS job
    WHERE job.namespace = ${ctx.config.namespace}
      AND job.id = ANY(${tx.array([...ids], 'TEXT')})
    ORDER BY job.id
    FOR UPDATE OF job
  `;
}

async function persistFinalRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  prepared: readonly PreparedReconciliation[]
): Promise<void> {
  if (prepared.length === 0) return;
  await tx`
    UPDATE bunqueue_jobs AS job
    SET payload = decode(batch.payload_base64, 'base64'), state = batch.state
    FROM unnest(
      ${tx.array(
        prepared.map(({ id }) => id),
        'TEXT'
      )},
      ${tx.array(
        prepared.map(({ payload }) => payload),
        'TEXT'
      )},
      ${tx.array(
        prepared.map(({ state }) => state),
        'TEXT'
      )}
    ) AS batch(id, payload_base64, state)
    WHERE job.namespace = ${ctx.config.namespace} AND job.id = batch.id
  `;
  await tx`
    UPDATE bunqueue_events AS event
    SET payload = decode(batch.payload_base64, 'base64')
    FROM unnest(
      ${tx.array(
        prepared.map(({ eventId }) => eventId),
        'BIGINT'
      )},
      ${tx.array(
        prepared.map(({ eventPayload: payload }) => payload),
        'TEXT'
      )}
    ) AS batch(id, payload_base64)
    WHERE event.namespace = ${ctx.config.namespace} AND event.id = batch.id
  `;
}

/** Reconcile same-batch dependency generations without reordering admission events. */
export async function reconcilePostgresSerialAdmissions(
  tx: TransactionSQL,
  ctx: PostgresContext,
  admissions: readonly PostgresAdmissionResult[]
): Promise<Job[]> {
  const latest = latestInsertions(admissions);
  if (latest.size === 0) return admissions.map(({ job }) => job);
  const now = await databaseNow(tx);
  const rows = await loadFinalRows(tx, ctx, [...latest.keys()]);
  const prepared = rows.map((row): PreparedReconciliation => {
    const insertionIndex = latest.get(row.id)!;
    const admission = admissions[insertionIndex];
    const { job, state } = correctedJob(row, admission, now);
    return {
      id: row.id,
      job,
      state,
      payload: postgresByteaBase64(encodePostgresValue(job)),
      eventId: admission.pushedEventId!,
      eventPayload: postgresByteaBase64(eventPayload({ job, state })),
      insertionIndex,
    };
  });
  await persistFinalRows(tx, ctx, prepared);
  const survivors = new Map(prepared.map((entry) => [entry.id, entry]));
  return admissions.map((admission, index) => {
    const survivor = survivors.get(String(admission.job.id));
    return survivor && index >= survivor.insertionIndex ? survivor.job : admission.job;
  });
}
