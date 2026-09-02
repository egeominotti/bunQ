import type { Shard } from '../../domain/queue/shard';
import type { FailureReason } from '../../domain/types/dlq';
import type { Job, JobId, JobLock } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { JobLogEntry } from '../../domain/types/worker';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { BoundedMap, BoundedSet, LRUMap, MapLike } from '../../shared/lru';
import type { RWLock } from '../../shared/lock';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import type { EventsManager } from '../eventsManager';
import type { MonitoringState } from '../monitoringChecks';
import type { WebhookManager } from '../webhookManager';
import type { WorkerManager } from '../workerManager';
import type { DEFAULT_CONFIG } from './config';
import type { JobTimeoutScheduler } from '../background/timeouts';
import type { RetiredTimeoutGeneration } from './background';

export interface ContextDependencies {
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
  depCompletions?: DependencyCompletionTracker;
  timedOutJobs: BoundedMap<JobId, RetiredTimeoutGeneration>;
  retiredTimeoutLeaseTokens: BoundedMap<string, RetiredTimeoutGeneration>;
  retiredCronLeaseTokens: BoundedMap<JobId, string>;
  timeoutScheduler: JobTimeoutScheduler;
  jobResults: LRUMap<JobId, unknown>;
  jobResultQueues: Map<JobId, string>;
  dependencyResults: DependencyResultTracker;
  customIdMap: LRUMap<string, JobId>;
  jobLogs: LRUMap<JobId, JobLogEntry[]>;
  jobLogQueues: Map<JobId, string>;
  jobLocks: Map<JobId, JobLock>;
  clientJobs: Map<string, Set<JobId>>;
  stalledCandidates: Set<JobId>;
  pendingDepChecks: Set<JobId>;
  pendingQueueAdmissions: Map<string, number>;
  queueNamesCache: Set<string>;
  repeatChain: Map<JobId, JobId>;
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
  perQueueMetrics: MapLike<string, { totalCompleted: bigint; totalFailed: bigint }>;
}

export interface ContextCallbacks {
  fail: (jobId: JobId, error?: string, failureReason?: FailureReason) => Promise<void>;
  registerQueueName: (queue: string) => void;
  unregisterQueueName: (queue: string) => void;
  onQueueAdmissionsDrained: (queue: string) => void;
  onJobCompleted: (completedId: JobId) => void;
  onJobFailed?: (failedId: JobId) => void;
  onJobsCompleted: (completedIds: JobId[]) => void;
  hasPendingDeps: () => boolean;
  onRepeat: (job: Job) => void;
  emitDashboardEvent?: (event: string, data: Record<string, unknown>) => void;
  onChildTerminalFailure?: (childJob: Job, error: string | undefined) => Promise<void>;
  onChildDependencyOption?: (childJob: Job, error: string | undefined) => Promise<void>;
}
