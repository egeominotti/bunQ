import type { Shard } from '../../domain/queue/shard';
import type { FailureReason } from '../../domain/types/dlq';
import type { Job, JobId, JobLock } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { JobLogEntry } from '../../domain/types/worker';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import type { BoundedMap, BoundedSet, LRUMap, MapLike, SetLike } from '../../shared/lru';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import type { EventsManager } from '../eventsManager';
import type { MonitoringState } from '../monitoringChecks';
import type { WebhookManager } from '../webhookManager';
import type { WorkerManager } from '../workerManager';
import type { DEFAULT_CONFIG } from './config';
import type { JobTimeoutScheduler } from '../background/timeouts';
import type { RetiredTimeoutGeneration } from './background';

export interface QueueManagerState {
  readonly config: typeof DEFAULT_CONFIG & { dataPath?: string };
  readonly storage: SqliteStorage | null;
  readonly shards: Shard[];
  readonly shardLocks: RWLock[];
  readonly processingShards: Map<JobId, Job>[];
  readonly processingLocks: RWLock[];
  readonly jobIndex: Map<JobId, JobLocation>;
  readonly completedJobs: BoundedSet<JobId>;
  readonly jobResults: LRUMap<JobId, unknown>;
  readonly dependencyResults: DependencyResultTracker;
  readonly customIdMap: LRUMap<string, JobId>;
  readonly jobLogs: LRUMap<JobId, JobLogEntry[]>;
  readonly jobLocks: Map<JobId, JobLock>;
  readonly timedOutJobs: MapLike<JobId, RetiredTimeoutGeneration>;
  readonly retiredTimeoutLeaseTokens: MapLike<string, RetiredTimeoutGeneration>;
  readonly retiredCronLeaseTokens: MapLike<JobId, string>;
  readonly timeoutScheduler: JobTimeoutScheduler;
  readonly clientJobs: Map<string, Set<JobId>>;
  readonly stalledCandidates: Set<JobId>;
  readonly pendingDepChecks: Set<JobId>;
  readonly queueNamesCache: Set<string>;
  readonly eventsManager: EventsManager;
  readonly webhookManager: WebhookManager;
  readonly metrics: QueueManagerMetrics;
  readonly startTime: number;
  readonly perQueueMetrics: MapLike<string, { totalCompleted: bigint; totalFailed: bigint }>;
}

export interface QueueManagerMetrics {
  totalPushed: { value: bigint };
  totalPulled: { value: bigint };
  totalCompleted: { value: bigint };
  totalFailed: { value: bigint };
}

export interface LockContext {
  jobIndex: Map<JobId, JobLocation>;
  jobLocks: Map<JobId, JobLock>;
  retiredCronLeaseTokens: MapLike<JobId, string>;
  clientJobs: Map<string, Set<JobId>>;
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  shards: Shard[];
  shardLocks: RWLock[];
  eventsManager: EventsManager;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
  storage: SqliteStorage | null;
  timeoutScheduler: JobTimeoutScheduler;
}

export interface BackgroundContext extends QueueManagerState {
  fail: (jobId: JobId, error?: string, failureReason?: FailureReason) => Promise<void>;
  registerQueueName: (queue: string) => void;
  unregisterQueueName: (queue: string) => void;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
  workerManager: WorkerManager;
  monitoringState: MonitoringState;
  completedJobsData: BoundedMap<JobId, Job>;
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
  timedOutJobs: BoundedMap<JobId, RetiredTimeoutGeneration>;
  retiredTimeoutLeaseTokens: BoundedMap<string, RetiredTimeoutGeneration>;
}

export interface StatsContext {
  shards: Shard[];
  processingShards: Map<JobId, Job>[];
  completedJobs: SetLike<JobId>;
  jobIndex: Map<JobId, JobLocation>;
  jobResults: LRUMap<JobId, unknown>;
  jobLogs: LRUMap<JobId, JobLogEntry[]>;
  customIdMap: LRUMap<string, JobId>;
  jobLocks: Map<JobId, JobLock>;
  clientJobs: Map<string, Set<JobId>>;
  pendingDepChecks: Set<JobId>;
  stalledCandidates: Set<JobId>;
  metrics: QueueManagerMetrics;
  startTime: number;
  perQueueMetrics?: MapLike<string, { totalCompleted: bigint; totalFailed: bigint }>;
}
