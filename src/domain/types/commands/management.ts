import type { JobState } from '../job';
import type { BaseCommand } from './base';

export interface CancelCommand extends BaseCommand {
  readonly cmd: 'Cancel';
  readonly id: string;
}
export interface ProgressCommand extends BaseCommand {
  readonly cmd: 'Progress';
  readonly id: string;
  readonly progress: number;
  readonly message?: string;
}
export interface UpdateCommand extends BaseCommand {
  readonly cmd: 'Update';
  readonly id: string;
  readonly data: unknown;
}
export interface ChangePriorityCommand extends BaseCommand {
  readonly cmd: 'ChangePriority';
  readonly id: string;
  readonly priority: number;
  readonly lifo?: boolean;
}
export interface PromoteCommand extends BaseCommand {
  readonly cmd: 'Promote';
  readonly id: string;
}
export interface WaitJobCommand extends BaseCommand {
  readonly cmd: 'WaitJob';
  readonly id: string;
  readonly timeout?: number;
}
export interface MoveToDelayedCommand extends BaseCommand {
  readonly cmd: 'MoveToDelayed';
  readonly id: string;
  readonly delay: number;
  readonly token?: string;
}
export interface DiscardCommand extends BaseCommand {
  readonly cmd: 'Discard';
  readonly id: string;
  readonly token?: string;
}
export interface PauseCommand extends BaseCommand {
  readonly cmd: 'Pause';
  readonly queue: string;
}
export interface ResumeCommand extends BaseCommand {
  readonly cmd: 'Resume';
  readonly queue: string;
}
export interface IsPausedCommand extends BaseCommand {
  readonly cmd: 'IsPaused';
  readonly queue: string;
}
export interface DrainCommand extends BaseCommand {
  readonly cmd: 'Drain';
  readonly queue: string;
}
export interface ObliterateCommand extends BaseCommand {
  readonly cmd: 'Obliterate';
  readonly queue: string;
}
export interface ListQueuesCommand extends BaseCommand {
  readonly cmd: 'ListQueues';
}
export interface CleanCommand extends BaseCommand {
  readonly cmd: 'Clean';
  readonly queue: string;
  readonly grace: number;
  readonly state?: JobState;
  readonly limit?: number;
}
