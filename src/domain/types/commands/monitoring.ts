import type { BaseCommand } from './base';

export interface AddLogCommand extends BaseCommand {
  readonly cmd: 'AddLog';
  readonly id: string;
  readonly message: string;
  readonly level?: 'info' | 'warn' | 'error';
}
export interface GetLogsCommand extends BaseCommand {
  readonly cmd: 'GetLogs';
  readonly id: string;
  readonly start?: number;
  readonly end?: number;
}
export interface AddWebhookCommand extends BaseCommand {
  readonly cmd: 'AddWebhook';
  readonly url: string;
  readonly events: string[];
  readonly queue?: string;
  readonly secret?: string;
}
export interface RemoveWebhookCommand extends BaseCommand {
  readonly cmd: 'RemoveWebhook';
  readonly webhookId: string;
}
export interface ListWebhooksCommand extends BaseCommand {
  readonly cmd: 'ListWebhooks';
}
export interface StatsCommand extends BaseCommand {
  readonly cmd: 'Stats';
}
export interface MetricsCommand extends BaseCommand {
  readonly cmd: 'Metrics';
}
export interface PrometheusCommand extends BaseCommand {
  readonly cmd: 'Prometheus';
}
export interface StorageStatusCommand extends BaseCommand {
  readonly cmd: 'StorageStatus';
}
