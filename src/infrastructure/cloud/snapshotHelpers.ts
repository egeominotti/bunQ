/**
 * Snapshot Helper Collectors
 * Heavy and analytics data collectors used by snapshotCollector.
 */

import type { QueueManager } from '../../application/queueManager';
import { jobId as toJobId } from '../../domain/types/job';
import type { CloudSnapshot } from './types';
import { redactData } from './redact';

/** Redaction options threaded from the cloud config into job-data collectors */
export interface RedactOptions {
  /** Fields to redact from job data (mirrors CloudConfig.redactFields) */
  redactFields: readonly string[];
  /** When false, omit job data entirely (mirrors CloudConfig.includeJobData) */
  includeJobData: boolean;
}

/** Per-queue previous totals for delta-based throughput calculation */
const prevQueueTotals = new Map<
  string,
  { completed: number; failed: number; pushed: number; timestamp: number }
>();

/** Per-queue previous waiting count for backlog velocity */
const prevQueueWaiting = new Map<string, { waiting: number; timestamp: number }>();

// ─── Heavy data collectors (called every ~90s) ───

/** All job states (BullMQ v5 compatible).
 * 'paused' yields jobs only when the queue is actually paused (where 'waiting' /
 * 'prioritized' are suppressed), so a paused queue's ready jobs still appear in
 * the snapshot — under `paused` — instead of vanishing (#92). No double-collect:
 * a job is returned by exactly one of waiting/prioritized/paused. */
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

/** Collect jobs — all states, all jobs (bounded by in-memory eviction limits) */
type DomainJob = ReturnType<QueueManager['getJobs']>[number];
type SnapshotJob = CloudSnapshot['recentJobs'][number];

/** Convert falsy/empty values to undefined for compact serialization */
function orUndef<T>(val: T): T | undefined {
  return val || undefined;
}
function nullUndef<T>(val: T | null): T | undefined {
  return val ?? undefined;
}
function posOrUndef(val: number): number | undefined {
  return val > 0 ? val : undefined;
}
function arrOrUndef<T>(arr: T[]): T[] | undefined {
  return arr.length > 0 ? arr : undefined;
}

/** Map core job fields (identity, state, timing) */
function mapJobCore(
  j: DomainJob,
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
  const data = j.data as Record<string, unknown> | undefined;
  const safeData = redact.includeJobData ? redactData(data, redact.redactFields) : undefined;
  const processTime = j.completedAt && j.startedAt ? j.completedAt - j.startedAt : undefined;
  return {
    id: String(j.id),
    name: (data?.name as string | undefined) ?? 'default',
    queue: j.queue,
    state,
    data: safeData,
    priority: j.priority,
    createdAt: j.createdAt,
    startedAt: nullUndef(j.startedAt),
    completedAt: nullUndef(j.completedAt),
    runAt: j.runAt,
    failedReason:
      state === 'active' && j.attempts > 0 ? `Retry ${j.attempts}/${j.maxAttempts}` : undefined,
    attempts: j.attempts,
    maxAttempts: j.maxAttempts,
    backoff: j.backoff,
    timeout: nullUndef(j.timeout),
    ttl: nullUndef(j.ttl),
    duration: processTime,
    waitTime: j.startedAt ? j.startedAt - j.createdAt : undefined,
    totalDuration: j.completedAt ? j.completedAt - j.createdAt : undefined,
  };
}

/** Map extended job fields (metadata, relationships, config) */
function mapJobExtended(j: DomainJob): Partial<SnapshotJob> {
  const timeline = j.timeline.length > 0 ? j.timeline : undefined;
  return {
    timeline,
    progress: posOrUndef(j.progress),
    progressMessage: nullUndef(j.progressMessage),
    customId: nullUndef(j.customId),
    uniqueKey: nullUndef(j.uniqueKey),
    tags: arrOrUndef(j.tags),
    groupId: nullUndef(j.groupId),
    parentId: j.parentId ? String(j.parentId) : undefined,
    childrenIds: j.childrenIds.length > 0 ? j.childrenIds.map(String) : undefined,
    dependsOn: j.dependsOn.length > 0 ? j.dependsOn.map(String) : undefined,
    childrenCompleted: posOrUndef(j.childrenCompleted),
    lastHeartbeat: posOrUndef(j.lastHeartbeat),
    stallCount: posOrUndef(j.stallCount),
    stallTimeout: nullUndef(j.stallTimeout),
    removeOnComplete: orUndef(j.removeOnComplete),
    removeOnFail: orUndef(j.removeOnFail),
    lifo: orUndef(j.lifo),
    backoffConfig: nullUndef(j.backoffConfig),
    repeat: nullUndef(j.repeat),
    stackTraceLimit: j.stackTraceLimit,
    keepLogs: nullUndef(j.keepLogs),
    sizeLimit: nullUndef(j.sizeLimit),
    failParentOnFailure: orUndef(j.failParentOnFailure),
    removeDependencyOnFailure: orUndef(j.removeDependencyOnFailure),
    continueParentOnFailure: orUndef(j.continueParentOnFailure),
    ignoreDependencyOnFailure: orUndef(j.ignoreDependencyOnFailure),
    deduplicationTtl: nullUndef(j.deduplicationTtl),
    deduplicationExtend: orUndef(j.deduplicationExtend),
    deduplicationReplace: orUndef(j.deduplicationReplace),
    debounceId: nullUndef(j.debounceId),
    debounceTtl: nullUndef(j.debounceTtl),
  };
}

/** Map a domain Job to the snapshot format */
function mapJobToSnapshot(j: DomainJob, state: string, redact: RedactOptions): SnapshotJob {
  return { ...mapJobCore(j, state, redact), ...mapJobExtended(j) } as SnapshotJob;
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
        for (const j of queueJobs) {
          jobs.push(mapJobToSnapshot(j, state, redact));
        }
      } catch {
        // Skip queue/state on error
      }
    }
  }

  return jobs;
}

/** Collect ALL DLQ entries — no cap */
export function collectDlqEntries(
  queueManager: QueueManager,
  dlqQueueNames: string[],
  redact: RedactOptions
): CloudSnapshot['dlqEntries'] {
  if (dlqQueueNames.length === 0) return [];

  const entries: CloudSnapshot['dlqEntries'] = [];

  for (const name of dlqQueueNames) {
    try {
      const dlq = queueManager.getDlqEntries(name);
      for (const e of dlq) {
        entries.push({
          jobId: String(e.job.id),
          queue: e.job.queue,
          reason: e.reason,
          error: e.error,
          enteredAt: e.enteredAt,
          retryCount: e.retryCount,
          lastRetryAt: e.lastRetryAt ?? undefined,
          nextRetryAt: e.nextRetryAt ?? undefined,
          expiresAt: (e as unknown as { expiresAt?: number }).expiresAt ?? undefined,
          jobAttempts: e.job.attempts,
          jobMaxAttempts: e.job.maxAttempts,
          jobData: redact.includeJobData ? redactData(e.job.data, redact.redactFields) : undefined,
          jobCreatedAt: e.job.createdAt,
          jobPriority: e.job.priority,
          attemptHistory: e.attempts.map((a) => ({
            attempt: a.attempt,
            startedAt: a.startedAt,
            failedAt: a.failedAt,
            reason: a.reason,
            error: a.error,
            duration: a.duration,
          })),
        });
      }
    } catch {
      // Skip
    }
  }

  return entries.sort((a, b) => b.enteredAt - a.enteredAt);
}

/** Collect per-queue config */
export function collectQueueConfigs(
  queueManager: QueueManager,
  queueNames: Set<string>
): CloudSnapshot['queueConfigs'] {
  const configs: CloudSnapshot['queueConfigs'] = {};
  const perQueue = queueManager.getPerQueueStats();

  for (const name of queueNames) {
    try {
      const stall = queueManager.getStallConfig(name);
      const dlq = queueManager.getDlqConfig(name);
      const pq = perQueue.get(name);
      const limits = queueManager.getQueueLimits(name);
      configs[name] = {
        paused: queueManager.isPaused(name),
        rateLimit: limits.rateLimit,
        concurrencyLimit: limits.concurrencyLimit,
        concurrencyActive: pq?.active ?? 0,
        stallConfig: {
          enabled: stall.enabled,
          stallInterval: stall.stallInterval,
          maxStalls: stall.maxStalls,
          gracePeriod: stall.gracePeriod,
        },
        dlqConfig: {
          autoRetry: dlq.autoRetry,
          autoRetryInterval: dlq.autoRetryInterval,
          maxRetries: dlq.maxAutoRetries,
          maxAge: dlq.maxAge ?? 0,
          maxEntries: dlq.maxEntries,
        },
      };
    } catch {
      // Skip
    }
  }

  return configs;
}

/** Collect webhook delivery stats */
export function collectWebhooks(queueManager: QueueManager): CloudSnapshot['webhooks'] {
  try {
    return queueManager.webhookManager.list().map((w) => ({
      id: w.id,
      url: w.url,
      events: w.events,
      queue: w.queue,
      enabled: w.enabled,
      successCount: w.successCount,
      failureCount: w.failureCount,
      lastTriggered: w.lastTriggered,
    }));
  } catch {
    return [];
  }
}

/** Collect top errors — only from queues with DLQ entries */
export function collectTopErrors(
  queueManager: QueueManager,
  dlqQueueNames: string[]
): CloudSnapshot['topErrors'] {
  if (dlqQueueNames.length === 0) return [];

  const errorMap = new Map<string, { count: number; queue: string; lastSeen: number }>();

  const allEntries = dlqQueueNames.flatMap((name) => {
    try {
      return queueManager.getDlqEntries(name);
    } catch {
      return [];
    }
  });

  for (const e of allEntries) {
    const msg = e.error ?? e.reason;
    const existing = errorMap.get(msg);
    if (existing) {
      existing.count++;
      if (e.enteredAt > existing.lastSeen) {
        existing.lastSeen = e.enteredAt;
        existing.queue = e.job.queue;
      }
    } else {
      errorMap.set(msg, { count: 1, queue: e.job.queue, lastSeen: e.enteredAt });
    }
  }

  return [...errorMap.entries()]
    .map(([message, data]) => ({ message, ...data }))
    .sort((a, b) => b.count - a.count);
}

// ─── Analytics collectors (derived from recent jobs + queue state) ───

/** Compute per-queue throughput from delta of totalCompleted/totalFailed */
export function collectQueueThroughput(
  queues: Array<{ name: string; totalCompleted: number; totalFailed: number }>
): CloudSnapshot['queueThroughput'] {
  const now = Date.now();
  const result: CloudSnapshot['queueThroughput'] = {};

  for (const q of queues) {
    const prev = prevQueueTotals.get(q.name);
    const total = q.totalCompleted + q.totalFailed;

    if (prev) {
      const elapsedSec = (now - prev.timestamp) / 1000;
      if (elapsedSec > 0.5) {
        const completeDelta = q.totalCompleted - prev.completed;
        const failDelta = q.totalFailed - prev.failed;
        result[q.name] = {
          pushPerSec: Math.round(((completeDelta + failDelta) / elapsedSec) * 100) / 100,
          completePerSec: Math.round((completeDelta / elapsedSec) * 100) / 100,
          failPerSec: Math.round((failDelta / elapsedSec) * 100) / 100,
          errorRate: total > 0 ? Math.round((q.totalFailed / total) * 10000) / 10000 : 0,
        };
      }
    }

    prevQueueTotals.set(q.name, {
      completed: q.totalCompleted,
      failed: q.totalFailed,
      pushed: q.totalCompleted + q.totalFailed,
      timestamp: now,
    });
  }

  return result;
}

/** Compute per-worker utilization */
export function collectWorkerUtilization(
  queueManager: QueueManager
): CloudSnapshot['workerUtilization'] {
  return queueManager.workerManager.list().map((w) => ({
    id: w.id,
    name: w.name,
    utilization: w.concurrency > 0 ? Math.round((w.activeJobs / w.concurrency) * 100) / 100 : 0,
  }));
}

/** Compute duration histogram from recent jobs */
export function collectDurationHistogram(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['durationHistogram'] {
  const h = { lt100ms: 0, lt1s: 0, lt10s: 0, lt60s: 0, gt60s: 0 };
  for (const j of recentJobs) {
    if (!j.duration) continue;
    if (j.duration < 100) h.lt100ms++;
    else if (j.duration < 1000) h.lt1s++;
    else if (j.duration < 10000) h.lt10s++;
    else if (j.duration < 60000) h.lt60s++;
    else h.gt60s++;
  }
  return h;
}

/** Compute per-queue wait time (createdAt → startedAt) from recent jobs */
export function collectQueueWaitTime(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['queueWaitTime'] {
  const byQueue = new Map<string, number[]>();
  for (const j of recentJobs) {
    if (!j.startedAt || !j.createdAt) continue;
    const wait = j.startedAt - j.createdAt;
    if (wait < 0) continue;
    let arr = byQueue.get(j.queue);
    if (!arr) {
      arr = [];
      byQueue.set(j.queue, arr);
    }
    arr.push(wait);
  }

  const result: CloudSnapshot['queueWaitTime'] = {};
  for (const [queue, waits] of byQueue) {
    const sum = waits.reduce((a, b) => a + b, 0);
    result[queue] = {
      avgMs: Math.round(sum / waits.length),
      maxMs: Math.max(...waits),
      minMs: Math.min(...waits),
    };
  }
  return result;
}

/** Compute per-queue retry rate from recent jobs */
export function collectQueueRetryRate(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['queueRetryRate'] {
  const byQueue = new Map<string, { retrying: number; total: number }>();
  for (const j of recentJobs) {
    let entry = byQueue.get(j.queue);
    if (!entry) {
      entry = { retrying: 0, total: 0 };
      byQueue.set(j.queue, entry);
    }
    entry.total++;
    if (j.attempts > 0) entry.retrying++;
  }

  const result: CloudSnapshot['queueRetryRate'] = {};
  for (const [queue, data] of byQueue) {
    result[queue] = {
      retryRate: data.total > 0 ? Math.round((data.retrying / data.total) * 10000) / 10000 : 0,
      retrying: data.retrying,
      total: data.total,
    };
  }
  return result;
}

/** Compute queue backlog velocity (delta waiting between snapshots) */
export function collectBacklogVelocity(
  queues: Array<{ name: string; waiting: number }>
): CloudSnapshot['queueBacklogVelocity'] {
  const now = Date.now();
  const result: CloudSnapshot['queueBacklogVelocity'] = {};

  for (const q of queues) {
    const prev = prevQueueWaiting.get(q.name);
    if (prev) {
      const elapsedMin = (now - prev.timestamp) / 60000;
      if (elapsedMin > 0.1) {
        const delta = q.waiting - prev.waiting;
        const deltaPerMin = Math.round((delta / elapsedMin) * 100) / 100;
        result[q.name] = {
          deltaWaiting: delta,
          deltaPerMin,
          trend: deltaPerMin > 5 ? 'growing' : deltaPerMin < -5 ? 'shrinking' : 'stable',
        };
      }
    }
    prevQueueWaiting.set(q.name, { waiting: q.waiting, timestamp: now });
  }

  return result;
}

/** Collect stall details from processing shards */
export async function collectStallDetails(
  queueManager: QueueManager
): Promise<CloudSnapshot['stallDetails']> {
  try {
    const memStats = queueManager.getMemoryStats();
    if (memStats.stalledCandidates === 0) return [];

    const stalledIds = (queueManager as unknown as { stalledCandidates: Set<string> })
      .stalledCandidates;
    if (stalledIds.size === 0) return [];

    const now = Date.now();
    const details: CloudSnapshot['stallDetails'] = [];
    const ids = [...stalledIds].slice(0, 20);
    for (const id of ids) {
      try {
        const job = await queueManager.getJob(toJobId(id));
        if (!job) continue;
        details.push({
          jobId: id,
          queue: job.queue,
          workerId: null,
          stalledAt: job.startedAt ?? now,
          stalledForMs: now - (job.startedAt ?? now),
        });
      } catch {
        // Skip
      }
    }
    return details;
  } catch {
    return [];
  }
}

/** Compute per-queue priority distribution from recent jobs */
export function collectPriorityDistribution(
  recentJobs: CloudSnapshot['recentJobs']
): CloudSnapshot['queuePriorityDistribution'] {
  const result: CloudSnapshot['queuePriorityDistribution'] = {};
  for (const j of recentJobs) {
    result[j.queue] ??= {};
    result[j.queue][j.priority] = (result[j.queue][j.priority] ?? 0) + 1;
  }
  return result;
}
