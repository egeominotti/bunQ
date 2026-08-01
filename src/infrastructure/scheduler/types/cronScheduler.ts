import type { CronJob } from '../../../domain/types/cron';
import type { JobInput } from '../../../domain/types/job';

export interface CronSchedulerConfig {
  /** @deprecated The scheduler now uses a precise event-driven timer. */
  checkIntervalMs?: number;
}

export type PushJobCallback = (queue: string, input: JobInput) => Promise<void>;

export type PersistCronCallback = (name: string, executions: number, nextRun: number) => void;

export interface CronHeapEntry {
  cron: CronJob;
  generation: number;
}

export interface CronRegistryEntry {
  cron: CronJob;
  generation: number;
}
