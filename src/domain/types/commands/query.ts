import type { JobState } from '../job';
import type { BaseCommand } from './base';

export interface GetJobCommand extends BaseCommand {
  readonly cmd: 'GetJob';
  readonly id: string;
}
export interface GetStateCommand extends BaseCommand {
  readonly cmd: 'GetState';
  readonly id: string;
}
export interface GetResultCommand extends BaseCommand {
  readonly cmd: 'GetResult';
  readonly id: string;
}
export interface GetJobsCommand extends BaseCommand {
  readonly cmd: 'GetJobs';
  readonly queue: string;
  readonly state?: JobState | JobState[];
  readonly limit?: number;
  readonly offset?: number;
  readonly asc?: boolean;
}
export interface GetJobCountsCommand extends BaseCommand {
  readonly cmd: 'GetJobCounts';
  readonly queue: string;
}
export interface GetCountsPerPriorityCommand extends BaseCommand {
  readonly cmd: 'GetCountsPerPriority';
  readonly queue: string;
}
export interface GetJobByCustomIdCommand extends BaseCommand {
  readonly cmd: 'GetJobByCustomId';
  readonly customId: string;
}
export interface CountCommand extends BaseCommand {
  readonly cmd: 'Count';
  readonly queue: string;
}
export interface GetProgressCommand extends BaseCommand {
  readonly cmd: 'GetProgress';
  readonly id: string;
}
export interface GetChildrenValuesCommand extends BaseCommand {
  readonly cmd: 'GetChildrenValues';
  readonly id: string;
}
