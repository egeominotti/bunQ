import { type DlqEntry, type DlqRetryState, setDlqRetryState } from '../../../domain/types/dlq';
import { Buffer } from 'node:buffer';
import type { Job, JobId } from '../../../domain/types/job';
import { jobId } from '../../../domain/types/job';
import { pack, unpack } from '../sqliteSerializer';
import type {
  PostgresJobRow,
  PostgresStoredJob,
  PostgresStoreEvent,
  PostgresJobState,
} from './types';

export function numeric(value: number | string | bigint | null): number | null {
  if (value === null) return null;
  const converted = Number(value);
  if (!Number.isSafeInteger(converted)) {
    throw new Error(`PostgreSQL integer is outside the JavaScript safe range: ${String(value)}`);
  }
  return converted;
}

export function encodePostgresValue(value: unknown): Uint8Array {
  return pack(value);
}

export function postgresByteaBase64(value: Uint8Array): string {
  return Buffer.from(value).toString('base64');
}

export function decodePostgresValue<T>(value: Uint8Array | null, fallback: T, context: string): T {
  return value ? unpack<T>(value, fallback, context) : fallback;
}

export function decodePostgresJob(row: PostgresJobRow): PostgresStoredJob {
  const job = decodePostgresValue<Job | null>(row.payload, null, `postgresJob:${row.id}`);
  if (!job) throw new Error(`Corrupt PostgreSQL payload for job ${row.id}`);
  const dlqRetryState = decodePostgresValue<DlqRetryState | null>(
    row.dlq_retry_state,
    null,
    `postgresDlqRetry:${row.id}`
  );
  const normalizedJob = { ...job, id: jobId(String(job.id)) };
  setDlqRetryState(normalizedJob, dlqRetryState);
  return {
    job: normalizedJob,
    state: row.state,
    result: decodePostgresValue(row.result, null, `postgresResult:${row.id}`),
    error: row.error,
    dlqEntry: decodePostgresValue<DlqEntry | null>(row.dlq_entry, null, `postgresDlq:${row.id}`),
    dlqRetryState,
    token: row.lease_token,
    leaseOwner: row.lease_owner,
    leaseBrokerId: row.lease_broker_id,
    leaseUntil: numeric(row.lease_until),
    leaseRenewals: row.lease_renewals,
    version: numeric(row.version) ?? 0,
  };
}

interface EventPayload {
  job?: Job;
  invalidationQueue?: string;
  state?: PostgresJobState;
  result?: unknown;
  error?: string;
  removed?: boolean;
  dlqEntry?: DlqEntry | null;
  dlqRetryState?: DlqRetryState | null;
}

export function decodePostgresEvent(row: {
  id: number | string | bigint;
  queue: string;
  event_type: string;
  job_id: string;
  occurred_at: number | string | bigint;
  payload: Uint8Array | null;
}): PostgresStoreEvent {
  const payload = decodePostgresValue<EventPayload>(row.payload, {}, `postgresEvent:${row.id}`);
  const dlqEntry = payload.dlqEntry
    ? {
        ...payload.dlqEntry,
        job: { ...payload.dlqEntry.job, id: jobId(String(payload.dlqEntry.job.id)) },
      }
    : payload.dlqEntry;
  return {
    id: numeric(row.id) ?? 0,
    queue: payload.invalidationQueue ?? row.queue,
    type: row.event_type,
    jobId: jobId(row.job_id),
    occurredAt: numeric(row.occurred_at) ?? 0,
    job: payload.job ? { ...payload.job, id: jobId(String(payload.job.id)) } : null,
    state: payload.state ?? null,
    ...(Object.hasOwn(payload, 'result') && { result: payload.result }),
    ...(payload.error !== undefined && { error: payload.error }),
    ...(payload.removed !== undefined && { removed: payload.removed }),
    ...(Object.hasOwn(payload, 'dlqEntry') && { dlqEntry }),
    ...(Object.hasOwn(payload, 'dlqRetryState') && {
      dlqRetryState: payload.dlqRetryState,
    }),
  };
}

export function postgresStateForJob(
  job: Job,
  dependenciesSatisfied: boolean,
  now = Date.now()
): Extract<PostgresJobState, 'waiting' | 'prioritized' | 'delayed' | 'waiting-children'> {
  if (!dependenciesSatisfied) return 'waiting-children';
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

export function eventPayload(input: EventPayload): Uint8Array {
  return encodePostgresValue(input);
}

export function asJobId(value: string): JobId {
  return jobId(value);
}
