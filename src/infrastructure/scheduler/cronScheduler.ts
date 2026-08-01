import { CronExecution } from './cron/execution';

export type {
  CronSchedulerConfig,
  PersistCronCallback,
  PushJobCallback,
} from './types/cronScheduler';

/** Event-driven cron scheduler façade. */
export class CronScheduler extends CronExecution {}
