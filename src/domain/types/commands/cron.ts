import type { CronJobOptions } from '../cron';
import type { BaseCommand } from './base';

export interface CronCommand extends BaseCommand {
  readonly cmd: 'Cron';
  readonly name: string;
  readonly jobName?: string;
  readonly queue: string;
  readonly data: unknown;
  readonly schedule?: string;
  readonly repeatEvery?: number;
  readonly priority?: number;
  readonly maxLimit?: number;
  readonly timezone?: string;
  readonly uniqueKey?: string;
  readonly dedup?: { ttl?: number; extend?: boolean; replace?: boolean };
  readonly skipMissedOnRestart?: boolean;
  readonly immediately?: boolean;
  readonly skipIfNoWorker?: boolean;
  readonly preventOverlap?: boolean;
  readonly jobOptions?: CronJobOptions;
}
export interface CronDeleteCommand extends BaseCommand {
  readonly cmd: 'CronDelete';
  readonly name: string;
}
export interface CronListCommand extends BaseCommand {
  readonly cmd: 'CronList';
}
export interface CronGetCommand extends BaseCommand {
  readonly cmd: 'CronGet';
  readonly name: string;
}
