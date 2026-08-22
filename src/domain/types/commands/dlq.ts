import type { DlqFilter } from '../dlq';
import type { BaseCommand } from './base';

export interface DlqCommand extends BaseCommand {
  readonly cmd: 'Dlq';
  readonly queue: string;
  readonly count?: number;
  readonly filter?: DlqFilter;
}
export interface GetDlqStatsCommand extends BaseCommand {
  readonly cmd: 'GetDlqStats';
  readonly queue: string;
}
export interface RetryDlqCommand extends BaseCommand {
  readonly cmd: 'RetryDlq';
  readonly queue: string;
  readonly jobId?: string;
  readonly count?: number;
  readonly filter?: DlqFilter;
}
export interface PurgeDlqCommand extends BaseCommand {
  readonly cmd: 'PurgeDlq';
  readonly queue: string;
}
export interface RemoveDlqJobCommand extends BaseCommand {
  readonly cmd: 'RemoveDlqJob';
  readonly queue: string;
  readonly jobId: string;
}
export interface RetryCompletedCommand extends BaseCommand {
  readonly cmd: 'RetryCompleted';
  readonly queue: string;
  readonly id?: string;
  readonly count?: number;
  readonly timestamp?: number;
}
export interface SetDlqConfigCommand extends BaseCommand {
  readonly cmd: 'SetDlqConfig';
  readonly queue: string;
  readonly config: Record<string, unknown>;
}
export interface GetDlqConfigCommand extends BaseCommand {
  readonly cmd: 'GetDlqConfig';
  readonly queue: string;
}
