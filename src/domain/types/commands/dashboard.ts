import type { BaseCommand } from './base';

export interface DashboardOverviewCommand extends BaseCommand {
  readonly cmd: 'DashboardOverview';
}
export interface DashboardQueuesCommand extends BaseCommand {
  readonly cmd: 'DashboardQueues';
}
export interface DashboardQueueCommand extends BaseCommand {
  readonly cmd: 'DashboardQueue';
  readonly queue: string;
  readonly includeJobs?: boolean;
  readonly jobsLimit?: number;
}
