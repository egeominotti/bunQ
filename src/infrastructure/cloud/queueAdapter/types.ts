import type { QueueManager } from '../../../application/queueManager';
import type { CronJob, CronJobInput } from '../../../domain/types/cron';
import type { DlqConfig, DlqEntry } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import type { StallConfig } from '../../../domain/types/stall';
import type { JobLogEntry, Worker } from '../../../domain/types/worker';

export interface CloudQueueCounts {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  'waiting-children': number;
  totalCompleted: number;
  totalFailed: number;
}

export interface CloudQueueSource {
  readonly name: string;
  readonly counts: CloudQueueCounts;
  readonly paused: boolean;
  readonly rateLimit: number | null;
  readonly concurrencyLimit: number | null;
  readonly stallConfig: StallConfig;
  readonly dlqConfig: DlqConfig;
}

export interface CloudSourceJob {
  readonly job: Job;
  readonly state: string;
}

export interface CloudSourceLock {
  readonly jobId: string;
  readonly owner: string;
  readonly token: string;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly lastRenewalAt: number;
  readonly renewalCount: number;
  readonly ttl: number;
}

export interface CloudSnapshotSource {
  readonly stats: ReturnType<QueueManager['getStats']>;
  readonly queues: readonly CloudQueueSource[];
  readonly jobs: readonly CloudSourceJob[];
  readonly dlqEntries: readonly DlqEntry[];
  readonly workers: readonly Worker[];
  readonly workerStats: ReturnType<QueueManager['workerManager']['getStats']>;
  readonly crons: readonly CronJob[];
  readonly jobResults: ReadonlyMap<string, unknown>;
  readonly jobLogs: ReadonlyMap<string, readonly JobLogEntry[]>;
  readonly activeLocks: readonly CloudSourceLock[];
}

export interface CloudQueueAdapter {
  readonly manager: QueueManager;
  pause(queue: string, paused: boolean): Promise<void>;
  drain(queue: string): Promise<number>;
  clean(queue: string, graceMs: number, state?: string, limit?: number): Promise<JobId[]>;
  obliterate(queue: string): Promise<void>;
  promoteAll(queue: string, limit: number): Promise<number>;
  retryCompleted(queue: string): Promise<number>;
  setRateLimit(queue: string, limit: number | null): Promise<void>;
  setConcurrency(queue: string, limit: number | null): Promise<void>;
  setStallConfig(queue: string, patch: Record<string, unknown>): Promise<void>;
  setDlqConfig(queue: string, patch: Record<string, unknown>): Promise<void>;
  listJobs(
    queue: string,
    options?: { state?: string | string[]; start?: number; end?: number; asc?: boolean }
  ): Promise<Job[]>;
  getJob(id: JobId): Promise<Job | null>;
  getLogs(id: JobId): Promise<JobLogEntry[]>;
  getResult(id: JobId): Promise<unknown>;
  clearLogs(id: JobId, keepLogs?: number): Promise<void>;
  retryDlq(queue: string, id?: JobId): Promise<number>;
  purgeDlq(queue: string): Promise<number>;
  push(queue: string, input: Parameters<QueueManager['push']>[1]): Promise<Job>;
  cancel(id: JobId): Promise<boolean>;
  promote(id: JobId): Promise<boolean>;
  changePriority(id: JobId, priority: number): Promise<boolean>;
  discard(id: JobId): Promise<boolean>;
  changeDelay(id: JobId, delay: number): Promise<boolean>;
  updateData(id: JobId, data: unknown): Promise<boolean>;
  addCron(input: CronJobInput): Promise<CronJob>;
  removeCron(name: string): Promise<boolean>;
  readSnapshotSource(requestedQueues?: readonly string[]): Promise<CloudSnapshotSource>;
}
