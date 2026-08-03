import type { CronScheduler } from '../../infrastructure/scheduler/cronScheduler';
import type { JobId } from '../../domain/types/job';
import type { JobTimeoutScheduler } from '../background/timeouts';

/** A processing generation whose timeout transition won before its worker outcome. */
export interface RetiredTimeoutGeneration {
  readonly jobId: JobId;
  readonly startedAt: number;
  readonly token?: string;
}

export interface BackgroundTaskHandles {
  cleanupInterval: ReturnType<typeof setInterval>;
  timeoutScheduler: JobTimeoutScheduler;
  depCheckInterval: ReturnType<typeof setInterval>;
  stallCheckInterval: ReturnType<typeof setInterval>;
  dlqMaintenanceInterval: ReturnType<typeof setInterval>;
  lockCheckInterval: ReturnType<typeof setInterval>;
  cronScheduler: CronScheduler;
}
