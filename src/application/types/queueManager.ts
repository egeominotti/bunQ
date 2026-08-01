import type { Shard } from '../../domain/queue/shard';
import type { DlqEntry } from '../../domain/types/dlq';
import type { FailureReason } from '../../domain/types/dlq';
import type { Job, JobId, JobInput, JobLock } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { JobLogEntry } from '../../domain/types/worker';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { CronScheduler } from '../../infrastructure/scheduler/cronScheduler';
import type { RWLock } from '../../shared/lock';
import type { BoundedMap, BoundedSet, LRUMap } from '../../shared/lru';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import type { EventsManager } from '../eventsManager';
import type { OperationalMetrics } from '../metricsExporter';
import type { MonitoringState } from '../monitoringChecks';
import type { WebhookManager } from '../webhookManager';
import type { WorkerManager } from '../workerManager';
import type { DEFAULT_CONFIG } from './config';

export interface QueueManagerStateView {
  config: typeof DEFAULT_CONFIG & { dataPath?: string };
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  customIdLock: RWLock;
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  completedJobs: BoundedSet<JobId>;
  completedJobsData: BoundedMap<JobId, Job>;
  depCompletions: DependencyCompletionTracker;
  timedOutJobs: BoundedSet<JobId>;
  jobResults: LRUMap<JobId, unknown>;
  dependencyResults: DependencyResultTracker;
  customIdMap: LRUMap<string, JobId>;
  jobLogs: LRUMap<JobId, JobLogEntry[]>;
  pendingDepChecks: Set<JobId>;
  stalledCandidates: Set<JobId>;
  jobLocks: Map<JobId, JobLock>;
  clientJobs: Map<string, Set<JobId>>;
  repeatChain: Map<JobId, JobId>;
  queueNamesCache: Set<string>;
  eventsManager: EventsManager;
  webhookManager: WebhookManager;
  workerManager: WorkerManager;
  monitoringState: MonitoringState;
  metrics: {
    totalPushed: { value: bigint };
    totalPulled: { value: bigint };
    totalCompleted: { value: bigint };
    totalFailed: { value: bigint };
  };
  startTime: number;
  maxLogsPerJob: number;
  perQueueMetrics: LRUMap<string, { totalCompleted: bigint; totalFailed: bigint }>;
  failedChildrenValues: Map<JobId, Record<string, string>>;
  ignoredChildrenFailures: Map<JobId, Record<string, string>>;
  cronScheduler: CronScheduler;
  operationalMetricsProvider: (() => OperationalMetrics) | null;
}

export interface QueueManagerRuntimeApi {
  push(queue: string, input: JobInput): Promise<Job>;
  fail(jobId: JobId, error?: string): Promise<void>;
  failWithReason(jobId: JobId, error: string | undefined, reason: FailureReason): Promise<void>;
  registerQueueName(queue: string): void;
  unregisterQueueName(queue: string): void;
  onJobCompleted(jobId: JobId): void;
  onJobFailed(jobId: JobId): void;
  onJobsCompleted(jobIds: JobId[]): void;
  hasPendingDeps(): boolean;
  emitDashboardEvent(event: string, data: Record<string, unknown>): void;
  failParentOnChildFailure(job: Job, error: string | undefined): Promise<void>;
  onChildDependencyOption(job: Job, error: string | undefined): Promise<void>;
  onDlqEvicted(entry: DlqEntry): void;
}

export type QueueManagerRuntime = QueueManagerStateView & QueueManagerRuntimeApi;
