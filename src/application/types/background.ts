import type { CronScheduler } from '../../infrastructure/scheduler/cronScheduler';

export interface BackgroundTaskHandles {
  cleanupInterval: ReturnType<typeof setInterval>;
  timeoutInterval: ReturnType<typeof setInterval>;
  depCheckInterval: ReturnType<typeof setInterval>;
  stallCheckInterval: ReturnType<typeof setInterval>;
  dlqMaintenanceInterval: ReturnType<typeof setInterval>;
  lockCheckInterval: ReturnType<typeof setInterval>;
  cronScheduler: CronScheduler;
}
