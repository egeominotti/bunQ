/**
 * Job Logs Manager
 * Per-job logging operations
 */

import type { JobId } from '../domain/types/job';
import { type JobLogEntry, createLogEntry } from '../domain/types/worker';
import type { JobLocation } from '../domain/types/queue';
import type { MapLike } from '../shared/lru';

/** Context for job logs operations */
export interface JobLogsContext {
  jobIndex: Map<JobId, JobLocation>;
  jobLogs: MapLike<JobId, JobLogEntry[]>;
  jobLogQueues: Map<JobId, string>;
  maxLogsPerJob: number;
}

/** Add log entry to a job */
export function addJobLog(
  jobId: JobId,
  message: string,
  ctx: JobLogsContext,
  level: 'info' | 'warn' | 'error' = 'info'
): boolean {
  const location = ctx.jobIndex.get(jobId);
  if (!location) return false;

  const logs = ctx.jobLogs.get(jobId) ?? [];
  logs.push(createLogEntry(message, level));

  // Keep bounded
  if (logs.length > ctx.maxLogsPerJob) {
    logs.splice(0, logs.length - ctx.maxLogsPerJob);
  }

  ctx.jobLogs.set(jobId, logs);
  ctx.jobLogQueues.set(jobId, location.queueName);
  return true;
}

/** Get logs for a job */
export function getJobLogs(jobId: JobId, ctx: JobLogsContext): JobLogEntry[] {
  return ctx.jobLogs.get(jobId) ?? [];
}

/** Clear logs for a job, optionally keeping the most recent N entries */
export function clearJobLogs(jobId: JobId, ctx: JobLogsContext, keepLogs?: number): void {
  if (keepLogs === undefined || keepLogs <= 0) {
    ctx.jobLogs.delete(jobId);
    ctx.jobLogQueues.delete(jobId);
  } else {
    const logs = ctx.jobLogs.get(jobId);
    if (logs && logs.length > keepLogs) {
      // Keep only the most recent keepLogs entries
      const trimmed = logs.slice(-keepLogs);
      ctx.jobLogs.set(jobId, trimmed);
    }
  }
}
