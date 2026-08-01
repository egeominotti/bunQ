import type { BaseCommand } from './base';

export interface HeartbeatCommand extends BaseCommand {
  readonly cmd: 'Heartbeat';
  readonly id: string;
  readonly activeJobs?: number;
  readonly processed?: number;
  readonly failed?: number;
}
export interface JobHeartbeatCommand extends BaseCommand {
  readonly cmd: 'JobHeartbeat';
  readonly id: string;
  readonly token?: string;
  readonly duration?: number;
}
export interface JobHeartbeatBatchCommand extends BaseCommand {
  readonly cmd: 'JobHeartbeatB';
  readonly ids: string[];
  readonly tokens?: string[];
}
export interface PingCommand extends BaseCommand {
  readonly cmd: 'Ping';
}
export interface RegisterWorkerCommand extends BaseCommand {
  readonly cmd: 'RegisterWorker';
  readonly name: string;
  readonly queues: string[];
  readonly concurrency?: number;
  readonly workerId?: string;
  readonly hostname?: string;
  readonly pid?: number;
  readonly startedAt?: number;
}
export interface UnregisterWorkerCommand extends BaseCommand {
  readonly cmd: 'UnregisterWorker';
  readonly workerId: string;
}
export interface ListWorkersCommand extends BaseCommand {
  readonly cmd: 'ListWorkers';
}
