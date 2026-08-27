import type { TransactionSQL } from 'bun';
import { MAX_TIMELINE_ENTRIES, type Job } from '../../../domain/types/job';
import { postgresAdvisoryLockName } from './advisoryLocks';
import { recordPostgresJobEvents } from './batchEvents';
import { encodePostgresValue, postgresStateForJob } from './codec';
import { databaseNow, type PostgresContext } from './context';
import { retirePostgresCompletionGenerations } from './completionLifecycle';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';
import type { PostgresJobState } from './types';
import { lockPostgresAdmissionQueues, registerPostgresAdmissionQueues } from './queueLifecycle';

type AdmissionState = Extract<
  PostgresJobState,
  'waiting' | 'prioritized' | 'delayed' | 'waiting-children'
>;

interface PreparedAdmission {
  readonly job: Job;
  readonly state: AdmissionState;
}

type BatchAdmissionRecord = Record<PropertyKey, unknown> & {
  readonly namespace: string;
  readonly id: string;
  readonly queue: string;
  readonly payload: Uint8Array;
  readonly state: AdmissionState;
  readonly priority: number;
  readonly lifo: boolean;
  readonly run_at: number;
  readonly created_at: number;
  readonly started_at: number | null;
  readonly completed_at: number | null;
  readonly attempts: number;
  readonly max_attempts: number;
  readonly ttl: number | null;
  readonly timeout: number | null;
  readonly unique_key: string | null;
  readonly unique_expires_at: number | null;
  readonly custom_id: string | null;
  readonly group_id: string | null;
};

export class PostgresBatchAdmissionConflict extends Error {
  constructor() {
    super('PostgreSQL batch admission encountered an existing job ID');
    this.name = 'PostgresBatchAdmissionConflict';
  }
}

export function canBatchAdmitPostgresJobs(jobs: readonly Job[]): boolean {
  if (jobs.length < 2) return false;
  if (new Set(jobs.map((job) => String(job.id))).size !== jobs.length) return false;
  return jobs.every((job) => job.dependsOn.length === 0 && job.parentId === null);
}

function admissionKeys(jobs: readonly Job[]): string[] {
  return [
    ...new Set(
      jobs.flatMap((job) => [
        `id:${String(job.id)}`,
        ...(job.uniqueKey ? [`dedup:${job.queue}:${job.uniqueKey}`] : []),
      ])
    ),
  ].sort();
}

/** Serialize every conflicting batch key in one canonically ordered round trip. */
export async function lockPostgresAdmissionKeys(
  tx: TransactionSQL,
  ctx: PostgresContext,
  jobs: readonly Job[]
): Promise<void> {
  const lockNames = admissionKeys(jobs).map((key) =>
    postgresAdvisoryLockName('admission', ctx.config.namespace, key)
  );
  if (lockNames.length === 0) return;
  await tx`
    WITH ordered_keys AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS admission(lock_name)
      ORDER BY lock_key
    ), locked_keys AS MATERIALIZED (
      SELECT lock_key, pg_advisory_xact_lock(lock_key) AS acquired
      FROM ordered_keys
      ORDER BY lock_key
    )
    SELECT COUNT(*) FROM locked_keys
  `;
}

function prepareAdmissions(jobs: readonly Job[], now: number): PreparedAdmission[] {
  return jobs.map((job) => {
    const preparedJob: Job = { ...job, timeline: [...job.timeline] };
    if (preparedJob.runAt === preparedJob.createdAt) preparedJob.runAt = now;
    const state = postgresStateForJob(preparedJob, true, now);
    if (preparedJob.timeline.length < MAX_TIMELINE_ENTRIES) {
      preparedJob.timeline.push({ state, timestamp: now });
    }
    return { job: preparedJob, state };
  });
}

function toRecords(
  prepared: readonly PreparedAdmission[],
  namespace: string,
  now: number
): BatchAdmissionRecord[] {
  return prepared.map(({ job, state }) => ({
    namespace,
    id: String(job.id),
    queue: job.queue,
    payload: encodePostgresValue(job),
    state,
    priority: job.priority,
    lifo: job.lifo,
    run_at: job.runAt,
    created_at: job.createdAt,
    started_at: job.startedAt,
    completed_at: job.completedAt,
    attempts: job.attempts,
    max_attempts: job.maxAttempts,
    ttl: job.ttl,
    timeout: job.timeout,
    unique_key: job.uniqueKey,
    unique_expires_at:
      job.uniqueKey && job.deduplicationTtl !== null ? now + job.deduplicationTtl : null,
    custom_id: job.customId,
    group_id: job.groupId,
  }));
}

async function insertRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  prepared: readonly PreparedAdmission[],
  now: number
): Promise<void> {
  const records = toRecords(prepared, ctx.config.namespace, now);
  let insertedCount = 0;
  for (let offset = 0; offset < records.length; offset += 1000) {
    const batch = records.slice(offset, offset + 1000);
    const inserted = await tx<{ id: string }[]>`
      INSERT INTO bunqueue_jobs ${tx(
        batch,
        'namespace',
        'id',
        'queue',
        'payload',
        'state',
        'priority',
        'lifo',
        'run_at',
        'created_at',
        'started_at',
        'completed_at',
        'attempts',
        'max_attempts',
        'ttl',
        'timeout',
        'unique_key',
        'unique_expires_at',
        'custom_id',
        'group_id'
      )}
      ON CONFLICT DO NOTHING
      RETURNING id
    `;
    insertedCount += inserted.length;
  }
  if (insertedCount !== prepared.length) throw new PostgresBatchAdmissionConflict();
}

/** Admit independent jobs atomically with one insert and one event write. */
export async function admitPostgresJobsBatch(
  tx: TransactionSQL,
  ctx: PostgresContext,
  jobs: readonly Job[]
): Promise<Job[]> {
  await lockPostgresAdmissionQueues(
    tx,
    ctx,
    jobs.map((job) => job.queue)
  );
  await lockPostgresDependencyCompletions(
    tx,
    ctx,
    jobs.map((job) => job.id)
  );
  await lockPostgresAdmissionKeys(tx, ctx, jobs);
  const now = await databaseNow(tx);
  const prepared = prepareAdmissions(jobs, now);
  await insertRows(tx, ctx, prepared, now);
  await registerPostgresAdmissionQueues(
    tx,
    ctx,
    prepared.map(({ job }) => job.queue)
  );
  await retirePostgresCompletionGenerations(
    tx,
    ctx,
    jobs.map((job) => job.id)
  );
  await recordPostgresJobEvents(
    tx,
    ctx,
    prepared.map(({ job, state }) => ({ job, type: 'pushed', state })),
    now
  );
  return prepared.map(({ job }) => job);
}
