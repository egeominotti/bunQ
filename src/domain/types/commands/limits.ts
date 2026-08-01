import type { BaseCommand } from './base';

export interface GetQueueLimitsCommand extends BaseCommand {
  readonly cmd: 'GetQueueLimits';
  readonly queue: string;
  readonly maxJobs?: number;
}
export interface GetDeduplicationJobIdCommand extends BaseCommand {
  readonly cmd: 'GetDeduplicationJobId';
  readonly queue: string;
  readonly deduplicationId: string;
}
export interface RemoveDeduplicationKeyCommand extends BaseCommand {
  readonly cmd: 'RemoveDeduplicationKey';
  readonly queue: string;
  readonly deduplicationId: string;
}
export interface RemoveJobDeduplicationKeyCommand extends BaseCommand {
  readonly cmd: 'RemoveJobDeduplicationKey';
  readonly id: string;
}
export interface MoveToWaitingChildrenCommand extends BaseCommand {
  readonly cmd: 'MoveToWaitingChildren';
  readonly id: string;
}
export interface RateLimitCommand extends BaseCommand {
  readonly cmd: 'RateLimit';
  readonly queue: string;
  readonly limit: number;
  readonly duration?: number;
  readonly ttl?: number;
}
export interface SetConcurrencyCommand extends BaseCommand {
  readonly cmd: 'SetConcurrency';
  readonly queue: string;
  readonly limit: number;
}
export interface RateLimitClearCommand extends BaseCommand {
  readonly cmd: 'RateLimitClear';
  readonly queue: string;
}
export interface ClearConcurrencyCommand extends BaseCommand {
  readonly cmd: 'ClearConcurrency';
  readonly queue: string;
}
export interface SetStallConfigCommand extends BaseCommand {
  readonly cmd: 'SetStallConfig';
  readonly queue: string;
  readonly config: Record<string, unknown>;
}
export interface GetStallConfigCommand extends BaseCommand {
  readonly cmd: 'GetStallConfig';
  readonly queue: string;
}
