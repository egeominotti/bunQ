import type { BaseCommand } from './base';

export interface ClearLogsCommand extends BaseCommand {
  readonly cmd: 'ClearLogs';
  readonly id: string;
  readonly keepLogs?: number;
}
export interface ExtendLockCommand extends BaseCommand {
  readonly cmd: 'ExtendLock';
  readonly id: string;
  readonly token?: string;
  readonly duration: number;
}
export interface ExtendLocksCommand extends BaseCommand {
  readonly cmd: 'ExtendLocks';
  readonly ids: string[];
  readonly tokens: string[];
  readonly durations: number[];
}
export interface ChangeDelayCommand extends BaseCommand {
  readonly cmd: 'ChangeDelay';
  readonly id: string;
  readonly delay: number;
  readonly token?: string;
}
export interface SetWebhookEnabledCommand extends BaseCommand {
  readonly cmd: 'SetWebhookEnabled';
  readonly id: string;
  readonly enabled: boolean;
}
export interface CompactMemoryCommand extends BaseCommand {
  readonly cmd: 'CompactMemory';
}
export interface UpdateParentCommand extends BaseCommand {
  readonly cmd: 'UpdateParent';
  readonly childId: string;
  readonly parentId: string;
}
export interface GetFailedChildrenValuesCommand extends BaseCommand {
  readonly cmd: 'GetFailedChildrenValues';
  readonly id: string;
}
export interface GetIgnoredChildrenFailuresCommand extends BaseCommand {
  readonly cmd: 'GetIgnoredChildrenFailures';
  readonly id: string;
}
export interface RemoveChildDependencyCommand extends BaseCommand {
  readonly cmd: 'RemoveChildDependency';
  readonly id: string;
}
export interface RemoveUnprocessedChildrenCommand extends BaseCommand {
  readonly cmd: 'RemoveUnprocessedChildren';
  readonly id: string;
}
export interface MoveToWaitCommand extends BaseCommand {
  readonly cmd: 'MoveToWait';
  readonly id: string;
  readonly token?: string;
}
export interface PromoteJobsCommand extends BaseCommand {
  readonly cmd: 'PromoteJobs';
  readonly queue: string;
  readonly count?: number;
}
