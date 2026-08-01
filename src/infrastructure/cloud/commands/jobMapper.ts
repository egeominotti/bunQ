import type { Job } from '../../../domain/types/job';

/** Derive the dashboard state from persisted job timestamps. */
function deriveState(job: {
  completedAt?: number | null;
  startedAt?: number | null;
  runAt: number;
}): string {
  if (job.completedAt) return 'completed';
  if (job.startedAt) return 'active';
  if (job.runAt > Date.now()) return 'delayed';
  return 'waiting';
}

/** Map a domain job to the field names expected by the dashboard. */
export function mapCloudCommandJob(job: Job) {
  const data = job.data as Record<string, unknown> | undefined;
  const state = deriveState(job);
  const processTime =
    job.completedAt && job.startedAt ? job.completedAt - job.startedAt : undefined;
  return {
    id: String(job.id),
    name: (data?.name as string | undefined) ?? 'default',
    queueName: job.queue,
    _queue: job.queue,
    state,
    _status: state,
    data: data !== undefined ? JSON.stringify(data) : undefined,
    priority: job.priority,
    timestamp: job.createdAt,
    processedOn: job.startedAt ?? undefined,
    finishedOn: job.completedAt ?? undefined,
    runAt: job.runAt,
    failedReason:
      state === 'active' && job.attempts > 0
        ? `Retry ${job.attempts}/${job.maxAttempts}`
        : undefined,
    attemptsMade: job.attempts,
    maxAttempts: job.maxAttempts,
    backoff: job.backoff,
    timeout: job.timeout ?? undefined,
    ttl: job.ttl ?? undefined,
    duration: processTime,
    waitTime: job.startedAt ? job.startedAt - job.createdAt : undefined,
    totalDuration: job.completedAt ? job.completedAt - job.createdAt : undefined,
    progress: job.progress || undefined,
    progressMessage: job.progressMessage ?? undefined,
    customId: job.customId ?? undefined,
    uniqueKey: job.uniqueKey ?? undefined,
    tags: job.tags.length > 0 ? job.tags : undefined,
    groupId: job.groupId ?? undefined,
    parentId: job.parentId ? String(job.parentId) : undefined,
    childrenIds: job.childrenIds.length > 0 ? job.childrenIds.map(String) : undefined,
    dependsOn: job.dependsOn.length > 0 ? job.dependsOn.map(String) : undefined,
    childrenCompleted: job.childrenCompleted > 0 ? job.childrenCompleted : undefined,
    lastHeartbeat: job.lastHeartbeat > 0 ? job.lastHeartbeat : undefined,
    stallCount: job.stallCount > 0 ? job.stallCount : undefined,
    stallTimeout: job.stallTimeout ?? undefined,
    removeOnComplete: job.removeOnComplete || undefined,
    removeOnFail: job.removeOnFail || undefined,
    lifo: job.lifo || undefined,
    backoffConfig: job.backoffConfig ?? undefined,
    repeat: job.repeat ?? undefined,
    stackTraceLimit: job.stackTraceLimit,
    keepLogs: job.keepLogs ?? undefined,
    sizeLimit: job.sizeLimit ?? undefined,
    failParentOnFailure: job.failParentOnFailure || undefined,
    removeDependencyOnFailure: job.removeDependencyOnFailure || undefined,
    continueParentOnFailure: job.continueParentOnFailure || undefined,
    ignoreDependencyOnFailure: job.ignoreDependencyOnFailure || undefined,
    deduplicationTtl: job.deduplicationTtl ?? undefined,
    deduplicationExtend: job.deduplicationExtend || undefined,
    deduplicationReplace: job.deduplicationReplace || undefined,
    debounceId: job.debounceId ?? undefined,
    debounceTtl: job.debounceTtl ?? undefined,
    timeline: job.timeline.length > 0 ? job.timeline : undefined,
  };
}
