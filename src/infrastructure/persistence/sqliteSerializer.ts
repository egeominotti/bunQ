/**
 * SQLite Serialization Utilities
 * MessagePack encoding/decoding and row conversion
 * Uses msgpackr for 2-3x faster serialization
 */

import { type Job, type JobId, type JobTimelineEntry, jobId } from '../../domain/types/job';
import type { DlqEntry } from '../../domain/types/dlq';
import type { DbJob } from './statements';
import { storageLog } from '../../shared/logger';
import {
  decodeMessagePack as msgpackDecode,
  encodeMessagePack as msgpackEncode,
} from '../../shared/msgpack';

/** Encode data to MessagePack buffer */
export function pack(data: unknown): Uint8Array {
  return msgpackEncode(data);
}

/** Decode MessagePack buffer to data */
export function unpack<T>(buffer: Uint8Array | null, fallback: T, context: string): T {
  if (!buffer) return fallback;
  try {
    return msgpackDecode(buffer) as T;
  } catch (err) {
    storageLog.error('MessagePack decode error', { context, error: String(err) });
    return fallback;
  }
}

/**
 * Normalize jobs constructed by pre-stallCount integrations at the persistence
 * boundary. The database keeps its NOT NULL invariant while an omitted legacy
 * field receives the same zero default as a freshly created Job.
 */
export function persistedStallCount(job: Pick<Job, 'stallCount'>): number {
  return job.stallCount ?? 0;
}

/**
 * Symbol marker stamped on a Job whose `depends_on` blob failed to decode.
 *
 * A corrupt dependency list must NOT collapse into `dependsOn: []` (which the
 * recovery path treats as "ready, no deps" -> out-of-order execution). Rather
 * than parking the job behind a magic-string dependency (which could collide
 * with a real user-supplied jobId, and which leaks into waitingDeps forever),
 * we signal corruption with a Symbol-keyed flag that cannot collide with any
 * user data. The recovery path detects this flag and routes the job to the DLQ.
 *
 * A Symbol property is non-enumerable to JSON/msgpack and is never persisted,
 * so it only exists on the in-memory Job for the duration of recovery.
 */
export const CORRUPT_DEPENDS_ON = Symbol('bunqueue.corruptDependsOn');

/** True if a Job was recovered with a corrupt `depends_on` blob. */
export function isCorruptDependsOn(job: Job): boolean {
  return (job as { [CORRUPT_DEPENDS_ON]?: boolean })[CORRUPT_DEPENDS_ON] === true;
}

/**
 * Decode a job's `depends_on` blob, distinguishing a genuine decode FAILURE
 * from a legitimately empty list. On decode failure, returns `corrupt: true`
 * (with empty ids) instead of silently swallowing it into a healthy empty array.
 */
function decodeDependsOn(
  buffer: Uint8Array | null,
  context: string
): { ids: string[]; corrupt: boolean } {
  if (!buffer) return { ids: [], corrupt: false };
  try {
    return { ids: msgpackDecode(buffer) as string[], corrupt: false };
  } catch (err) {
    storageLog.error('Corrupt depends_on blob (routing job to DLQ)', {
      context,
      error: String(err),
    });
    return { ids: [], corrupt: true };
  }
}

/** Convert database row to Job object */
export function rowToJob(row: DbJob): Job {
  const jobContext = `rowToJob:${row.id}`;
  // A corrupt depends_on blob must NOT be silently swallowed into [] (which the
  // recovery path treats as "ready, no deps" -> out-of-order execution).
  // decodeDependsOn() surfaces the corruption via a `corrupt` flag; we then
  // stamp the returned Job with the CORRUPT_DEPENDS_ON symbol so the recovery
  // path can route it to the DLQ instead of enqueuing it as ready.
  const decoded = decodeDependsOn(row.depends_on, `${jobContext}:dependsOn`);
  const dependsOn: string[] = decoded.ids;
  const childrenIds: string[] = row.children_ids
    ? unpack<string[]>(row.children_ids, [], `${jobContext}:childrenIds`)
    : [];
  const tags: string[] = row.tags ? unpack<string[]>(row.tags, [], `${jobContext}:tags`) : [];

  const job: Job = {
    id: jobId(row.id),
    queue: row.queue,
    data: unpack(row.data, {}, `${jobContext}:data`),
    priority: row.priority,
    createdAt: row.created_at,
    runAt: row.run_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    backoff: row.backoff,
    backoffConfig: null, // BullMQ v5: not persisted in DB, use default
    ttl: row.ttl,
    timeout: row.timeout,
    uniqueKey: row.unique_key,
    customId: row.custom_id,
    dependsOn: dependsOn.map((s) => jobId(s)),
    parentId: row.parent_id ? jobId(row.parent_id) : null,
    childrenIds: childrenIds.map((s) => jobId(s)),
    childrenCompleted: 0,
    tags,
    lifo: row.lifo === 1,
    groupId: row.group_id,
    progress: row.progress ?? 0,
    progressMessage: row.progress_msg,
    removeOnComplete: row.remove_on_complete === 1,
    removeOnFail: row.remove_on_fail === 1,
    repeat: null,
    lastHeartbeat: row.last_heartbeat ?? row.created_at,
    stallTimeout: row.stall_timeout,
    stallCount: row.stall_count ?? 0,
    // BullMQ v5 additional fields (not persisted in DB, use defaults)
    stackTraceLimit: 10,
    keepLogs: null,
    sizeLimit: null,
    failParentOnFailure: false,
    removeDependencyOnFailure: false,
    continueParentOnFailure: false,
    ignoreDependencyOnFailure: false,
    deduplicationTtl: null,
    deduplicationExtend: false,
    deduplicationReplace: false,
    debounceId: null,
    debounceTtl: null,
    timeline: row.timeline
      ? unpack<JobTimelineEntry[]>(row.timeline, [], `${jobContext}:timeline`)
      : [],
    stacktrace: row.stacktrace
      ? unpack<string[] | null>(row.stacktrace, null, `${jobContext}:stacktrace`)
      : null,
  };

  // Stamp a collision-proof corruption marker (non-enumerable Symbol, never
  // persisted) so the recovery path routes this job to the DLQ rather than
  // enqueuing it as ready. We keep dependsOn: [] here — the real deps are
  // unrecoverable — but the marker prevents out-of-order execution.
  if (decoded.corrupt) {
    Object.defineProperty(job, CORRUPT_DEPENDS_ON, {
      value: true,
      enumerable: false,
      configurable: true,
    });
  }

  return job;
}

/**
 * Brand a decoded id as a JobId, preserving its runtime type.
 *
 * Production ids are UUIDv7 strings (identity). msgpackr faithfully round-trips
 * the original runtime type (including bigint), so we only stringify when the
 * decoded value is genuinely non-string rather than unconditionally coercing —
 * which would otherwise rewrite a recovered job's id and break id equality on
 * the critical-loss -> DLQ -> restart recovery path.
 */
function brandId(id: unknown): JobId {
  return typeof id === 'string' ? jobId(id) : (id as JobId);
}

/** Reconstruct DlqEntry from MessagePack-decoded data */
export function reconstructDlqEntry(entry: DlqEntry): DlqEntry {
  return {
    ...entry,
    job: {
      ...entry.job,
      id: brandId(entry.job.id),
      dependsOn: entry.job.dependsOn.map((id) => brandId(id)),
      parentId: entry.job.parentId !== null ? brandId(entry.job.parentId) : null,
      childrenIds: entry.job.childrenIds.map((id) => brandId(id)),
      // Pre-#74 blobs have no stacktrace field — restore the domain invariant
      stacktrace: entry.job.stacktrace ?? null,
    },
  };
}
