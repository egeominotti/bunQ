import { arch, cpus, hostname, platform } from 'os';
import type { QueueManager } from '../../../application/queueManager';
import type { CloudSnapshot } from '../types';
import type { CollectSnapshotParams } from '../types/collector';
import type { CloudSnapshotSource } from '../queueAdapter/types';

/** Cached host and runtime metadata. */
export const SNAPSHOT_HOST = hostname();
export const SNAPSHOT_RUNTIME = {
  bunVersion: typeof Bun !== 'undefined' ? Bun.version : 'unknown',
  os: platform(),
  arch: arch(),
  cpus: cpus().length,
};

/** Resolve redaction options with defaults (includeJobData defaults on). */
export function resolveRedaction(params: CollectSnapshotParams): {
  redactFields: readonly string[];
  includeJobData: boolean;
} {
  return {
    redactFields: params.redactFields ?? [],
    includeJobData: params.includeJobData ?? true,
  };
}

/** Map cron definitions to their snapshot shape. */
export function collectCrons(queueManager: QueueManager): CloudSnapshot['crons'] {
  return mapSnapshotCrons(queueManager.listCrons());
}

export function mapSnapshotCrons(crons: CloudSnapshotSource['crons']): CloudSnapshot['crons'] {
  return crons.map((cron) => ({
    name: cron.name,
    queue: cron.queue,
    schedule: cron.schedule ?? null,
    repeatEvery: cron.repeatEvery ?? null,
    nextRun: cron.nextRun,
    executions: cron.executions,
    maxLimit: cron.maxLimit,
    lastRun: cron.executions > 0 && cron.repeatEvery ? cron.nextRun - cron.repeatEvery : null,
    priority: cron.priority,
    timezone: cron.timezone ?? null,
    data: cron.data,
    uniqueKey: cron.uniqueKey ?? null,
    dedup: cron.dedup ?? null,
  }));
}

/** Back-fill failed-job timing from the last DLQ attempt. */
export function enrichFailedJobDurations(
  recentJobs: CloudSnapshot['recentJobs'],
  dlqEntries: CloudSnapshot['dlqEntries']
): void {
  if (dlqEntries.length === 0) return;
  const dlqMap = new Map<string, { duration: number; failedAt: number }>();
  for (const entry of dlqEntries) {
    if (entry.attemptHistory.length > 0) {
      const last = entry.attemptHistory[entry.attemptHistory.length - 1];
      dlqMap.set(entry.jobId, { duration: last.duration, failedAt: last.failedAt });
    }
  }
  for (const job of recentJobs) {
    if (job.state === 'failed' && !job.duration) {
      const dlq = dlqMap.get(job.id);
      if (dlq) {
        job.duration = dlq.duration;
        job.completedAt = dlq.failedAt;
        job.totalDuration = dlq.failedAt - job.createdAt;
      }
    }
  }
}

/** Collect worker registry details and derived health fields. */
export function collectWorkerDetails(
  queueManager: QueueManager,
  now: number
): CloudSnapshot['workerDetails'] {
  const workerTimeoutMs = 30_000;
  return queueManager.workerManager.list().map((worker) => {
    const totalJobs = worker.processedJobs + worker.failedJobs;
    return {
      id: worker.id,
      name: worker.name,
      queues: worker.queues,
      concurrency: worker.concurrency,
      hostname: worker.hostname,
      pid: worker.pid,
      registeredAt: worker.registeredAt,
      lastSeen: worker.lastSeen,
      activeJobs: worker.activeJobs,
      processedJobs: worker.processedJobs,
      failedJobs: worker.failedJobs,
      currentJob: worker.currentJob,
      uptime: now - worker.registeredAt,
      status:
        now - worker.lastSeen > workerTimeoutMs
          ? ('stalled' as const)
          : worker.activeJobs > 0
            ? ('active' as const)
            : ('idle' as const),
      errorRate: totalJobs > 0 ? +(worker.failedJobs / totalJobs).toFixed(4) : 0,
      utilization:
        worker.concurrency > 0 ? +(worker.activeJobs / worker.concurrency).toFixed(4) : 0,
    };
  });
}

export function mapSnapshotWorkers(
  workers: CloudSnapshotSource['workers'],
  now: number
): CloudSnapshot['workerDetails'] {
  const workerTimeoutMs = 30_000;
  return workers.map((worker) => {
    const totalJobs = worker.processedJobs + worker.failedJobs;
    return {
      id: worker.id,
      name: worker.name,
      queues: worker.queues,
      concurrency: worker.concurrency,
      hostname: worker.hostname,
      pid: worker.pid,
      registeredAt: worker.registeredAt,
      lastSeen: worker.lastSeen,
      activeJobs: worker.activeJobs,
      processedJobs: worker.processedJobs,
      failedJobs: worker.failedJobs,
      currentJob: worker.currentJob,
      uptime: now - worker.registeredAt,
      status:
        now - worker.lastSeen > workerTimeoutMs
          ? ('stalled' as const)
          : worker.activeJobs > 0
            ? ('active' as const)
            : ('idle' as const),
      errorRate: totalJobs > 0 ? +(worker.failedJobs / totalJobs).toFixed(4) : 0,
      utilization:
        worker.concurrency > 0 ? +(worker.activeJobs / worker.concurrency).toFixed(4) : 0,
    };
  });
}

/** Collect results, logs, and active locks for the snapshot. */
export function collectJobArtifacts(
  queueManager: QueueManager
): Pick<CloudSnapshot, 'jobResults' | 'jobLogEntries' | 'activeLocks'> {
  const jobResults: Record<string, unknown> = {};
  for (const [id, value] of queueManager.getAllJobResults()) jobResults[id] = value;

  const jobLogEntries: CloudSnapshot['jobLogEntries'] = {};
  for (const [id, logs] of queueManager.getAllJobLogs()) jobLogEntries[id] = logs;

  const activeLocks = [...queueManager.getAllJobLocks().values()].map((lock) => ({
    jobId: String(lock.jobId),
    owner: lock.owner,
    token: lock.token,
    createdAt: lock.createdAt,
    expiresAt: lock.expiresAt,
    lastRenewalAt: lock.lastRenewalAt,
    renewalCount: lock.renewalCount,
    ttl: lock.ttl,
  }));

  return { jobResults, jobLogEntries, activeLocks };
}

export function mapSnapshotJobArtifacts(
  source: CloudSnapshotSource
): Pick<CloudSnapshot, 'jobResults' | 'jobLogEntries' | 'activeLocks'> {
  return {
    jobResults: Object.fromEntries(source.jobResults),
    jobLogEntries: Object.fromEntries([...source.jobLogs].map(([id, logs]) => [id, [...logs]])),
    activeLocks: [...source.activeLocks],
  };
}
