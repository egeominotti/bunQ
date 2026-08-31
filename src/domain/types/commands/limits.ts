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
  readonly token?: string;
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

interface GroupCommand extends BaseCommand {
  readonly queue: string;
  readonly groupId: string;
}

export interface GetGroupJobsCountCommand extends GroupCommand {
  readonly cmd: 'GetGroupJobsCount';
}
export interface GetGroupsJobsCountCommand extends BaseCommand {
  readonly cmd: 'GetGroupsJobsCount';
  readonly queue: string;
  readonly maxCount?: number;
}
export interface GetGroupActiveCountCommand extends GroupCommand {
  readonly cmd: 'GetGroupActiveCount';
}
export interface SetGroupRateLimitCommand extends GroupCommand {
  readonly cmd: 'SetGroupRateLimit';
  readonly max: number;
  readonly duration: number;
}
export interface GetGroupRateLimitCommand extends GroupCommand {
  readonly cmd: 'GetGroupRateLimit';
}
export interface RemoveGroupRateLimitCommand extends GroupCommand {
  readonly cmd: 'RemoveGroupRateLimit';
}
export interface GetGroupRateLimitTtlCommand extends GroupCommand {
  readonly cmd: 'GetGroupRateLimitTtl';
  readonly maxJobs?: number;
}
export interface SetGroupConcurrencyCommand extends GroupCommand {
  readonly cmd: 'SetGroupConcurrency';
  readonly concurrency: number;
}
export interface GetGroupConcurrencyCommand extends GroupCommand {
  readonly cmd: 'GetGroupConcurrency';
}
export interface RemoveGroupConcurrencyCommand extends GroupCommand {
  readonly cmd: 'RemoveGroupConcurrency';
}
export interface PauseGroupCommand extends GroupCommand {
  readonly cmd: 'PauseGroup';
}
export interface ResumeGroupCommand extends GroupCommand {
  readonly cmd: 'ResumeGroup';
}
export interface IsGroupPausedCommand extends GroupCommand {
  readonly cmd: 'IsGroupPaused';
}
export interface RateLimitGroupCommand extends GroupCommand {
  readonly cmd: 'RateLimitGroup';
  readonly duration: number;
}
