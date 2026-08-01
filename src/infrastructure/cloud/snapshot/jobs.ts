import type { QueueManager } from '../../../application/queueManager';
import { redactData } from '../redact';
import type { CloudSnapshot } from '../types';
import type { RedactOptions } from '../types/redact';

const ALL_STATES = [
  'waiting',
  'active',
  'delayed',
  'failed',
  'completed',
  'prioritized',
  'paused',
  'waiting-children',
] as const;

type DomainJob = ReturnType<QueueManager['getJobs']>[number];
type SnapshotJob = CloudSnapshot['recentJobs'][number];

function orUndef<T>(value: T): T | undefined {
  return value || undefined;
}

function nullUndef<T>(value: T | null): T | undefined {
  return value ?? undefined;
}

function posOrUndef(value: number): number | undefined {
  return value > 0 ? value : undefined;
}

function arrOrUndef<T>(array: T[]): T[] | undefined {
  return array.length > 0 ? array : undefined;
}

function mapJobCore(
  job: DomainJob,
  state: string,
  redact: RedactOptions
): Pick<
  SnapshotJob,
  | 'id'
  | 'name'
  | 'queue'
  | 'state'
  | 'data'
  | 'priority'
  | 'createdAt'
  | 'startedAt'
  | 'completedAt'
  | 'runAt'
  | 'failedReason'
  | 'attempts'
  | 'maxAttempts'
  | 'backoff'
  | 'timeout'
  | 'ttl'
  | 'duration'
  | 'waitTime'
  | 'totalDuration'
> {
  const data = job.data as Record<string, unknown> | undefined;
  const safeData = redact.includeJobData ? redactData(data, redact.redactFields) : undefined;
  const processTime =
    job.completedAt && job.startedAt ? job.completedAt - job.startedAt : undefined;
  return {
    id: String(job.id),
    name: (data?.name as string | undefined) ?? 'default',
    queue: job.queue,
    state,
    data: safeData,
    priority: job.priority,
    createdAt: job.createdAt,
    startedAt: nullUndef(job.startedAt),
    completedAt: nullUndef(job.completedAt),
    runAt: job.runAt,
    failedReason:
      state === 'active' && job.attempts > 0
        ? `Retry ${job.attempts}/${job.maxAttempts}`
        : undefined,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    backoff: job.backoff,
    timeout: nullUndef(job.timeout),
    ttl: nullUndef(job.ttl),
    duration: processTime,
    waitTime: job.startedAt ? job.startedAt - job.createdAt : undefined,
    totalDuration: job.completedAt ? job.completedAt - job.createdAt : undefined,
  };
}

function mapJobExtended(job: DomainJob): Partial<SnapshotJob> {
  return {
    timeline: job.timeline.length > 0 ? job.timeline : undefined,
    progress: posOrUndef(job.progress),
    progressMessage: nullUndef(job.progressMessage),
    customId: nullUndef(job.customId),
    uniqueKey: nullUndef(job.uniqueKey),
    tags: arrOrUndef(job.tags),
    groupId: nullUndef(job.groupId),
    parentId: job.parentId ? String(job.parentId) : undefined,
    childrenIds: job.childrenIds.length > 0 ? job.childrenIds.map(String) : undefined,
    dependsOn: job.dependsOn.length > 0 ? job.dependsOn.map(String) : undefined,
    childrenCompleted: posOrUndef(job.childrenCompleted),
    lastHeartbeat: posOrUndef(job.lastHeartbeat),
    stallCount: posOrUndef(job.stallCount),
    stallTimeout: nullUndef(job.stallTimeout),
    removeOnComplete: orUndef(job.removeOnComplete),
    removeOnFail: orUndef(job.removeOnFail),
    lifo: orUndef(job.lifo),
    backoffConfig: nullUndef(job.backoffConfig),
    repeat: nullUndef(job.repeat),
    stackTraceLimit: job.stackTraceLimit,
    keepLogs: nullUndef(job.keepLogs),
    sizeLimit: nullUndef(job.sizeLimit),
    failParentOnFailure: orUndef(job.failParentOnFailure),
    removeDependencyOnFailure: orUndef(job.removeDependencyOnFailure),
    continueParentOnFailure: orUndef(job.continueParentOnFailure),
    ignoreDependencyOnFailure: orUndef(job.ignoreDependencyOnFailure),
    deduplicationTtl: nullUndef(job.deduplicationTtl),
    deduplicationExtend: orUndef(job.deduplicationExtend),
    deduplicationReplace: orUndef(job.deduplicationReplace),
    debounceId: nullUndef(job.debounceId),
    debounceTtl: nullUndef(job.debounceTtl),
  };
}

function mapJobToSnapshot(job: DomainJob, state: string, redact: RedactOptions): SnapshotJob {
  return { ...mapJobCore(job, state, redact), ...mapJobExtended(job) } as SnapshotJob;
}

export function collectLiveJobs(
  queueManager: QueueManager,
  queueNames: string[],
  redact: RedactOptions
): CloudSnapshot['recentJobs'] {
  if (queueNames.length === 0) return [];
  const jobs: CloudSnapshot['recentJobs'] = [];
  for (const name of queueNames) {
    for (const state of ALL_STATES) {
      try {
        const queueJobs = queueManager.getJobs(name, { state: [state], start: 0, end: 999 });
        for (const job of queueJobs) jobs.push(mapJobToSnapshot(job, state, redact));
      } catch {
        // Skip queue/state on error.
      }
    }
  }
  return jobs;
}

export function collectDlqEntries(
  queueManager: QueueManager,
  dlqQueueNames: string[],
  redact: RedactOptions
): CloudSnapshot['dlqEntries'] {
  if (dlqQueueNames.length === 0) return [];
  const entries: CloudSnapshot['dlqEntries'] = [];
  for (const name of dlqQueueNames) {
    try {
      for (const entry of queueManager.getDlqEntries(name)) {
        entries.push({
          jobId: String(entry.job.id),
          queue: entry.job.queue,
          reason: entry.reason,
          error: entry.error,
          enteredAt: entry.enteredAt,
          retryCount: entry.retryCount,
          lastRetryAt: entry.lastRetryAt ?? undefined,
          nextRetryAt: entry.nextRetryAt ?? undefined,
          expiresAt: (entry as unknown as { expiresAt?: number }).expiresAt ?? undefined,
          jobAttempts: entry.job.attempts,
          jobMaxAttempts: entry.job.maxAttempts,
          jobData: redact.includeJobData
            ? redactData(entry.job.data, redact.redactFields)
            : undefined,
          jobCreatedAt: entry.job.createdAt,
          jobPriority: entry.job.priority,
          attemptHistory: entry.attempts.map((attempt) => ({
            attempt: attempt.attempt,
            startedAt: attempt.startedAt,
            failedAt: attempt.failedAt,
            reason: attempt.reason,
            error: attempt.error,
            duration: attempt.duration,
          })),
        });
      }
    } catch {
      // Skip queue on error.
    }
  }
  return entries.sort((a, b) => b.enteredAt - a.enteredAt);
}
