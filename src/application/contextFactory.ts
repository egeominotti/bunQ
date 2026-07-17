/**
 * ContextFactory - Builds context objects for delegated operations
 */

import type { Job, JobId, JobLock } from '../domain/types/job';
import type { JobLocation } from '../domain/types/queue';
import type { JobLogEntry } from '../domain/types/worker';
import type { Shard } from '../domain/queue/shard';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';
import type { RWLock } from '../shared/lock';
import type { LRUMap, BoundedSet, BoundedMap, MapLike } from '../shared/lru';
import type { WebhookManager } from './webhookManager';
import type { WorkerManager } from './workerManager';
import type { EventsManager } from './eventsManager';
import type { MonitoringState } from './monitoringChecks';
import type { LockContext, BackgroundContext, StatsContext } from './types';
import type { PushContext } from './operations/push';
import type { PullContext } from './operations/pull';
import type { AckContext } from './operations/ack';
import type { JobManagementContext } from './operations/jobManagement';
import type { QueryContext } from './operations/queryOperations';
import type { DlqContext, RetryCompletedContext } from './dlqManager';

import type { DEFAULT_CONFIG } from './types';

/** Dependencies needed to build all contexts */
export interface ContextDependencies {
  config: typeof DEFAULT_CONFIG & { dataPath?: string };
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  completedJobs: BoundedSet<JobId>;
  completedJobsData: BoundedMap<JobId, Job>;
  depCompletions?: BoundedSet<JobId>;
  timedOutJobs?: BoundedSet<JobId>;
  jobResults: LRUMap<JobId, unknown>;
  customIdMap: LRUMap<string, JobId>;
  jobLogs: LRUMap<JobId, JobLogEntry[]>;
  jobLocks: Map<JobId, JobLock>;
  clientJobs: Map<string, Set<JobId>>;
  stalledCandidates: Set<JobId>;
  pendingDepChecks: Set<JobId>;
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

/** Callbacks needed for some contexts */
export interface ContextCallbacks {
  fail: (jobId: JobId, error?: string, token?: string) => Promise<void>;
  registerQueueName: (queue: string) => void;
  unregisterQueueName: (queue: string) => void;
  onJobCompleted: (completedId: JobId) => void;
  onJobsCompleted: (completedIds: JobId[]) => void;
  hasPendingDeps: () => boolean;
  onRepeat: (job: Job) => void;
  emitDashboardEvent?: (event: string, data: Record<string, unknown>) => void;
  onChildTerminalFailure?: (childJob: Job, error: string | undefined) => void;
  onChildDependencyOption?: (childJob: Job, error: string | undefined) => void;
}

/**
 * Factory for building context objects
 */
export class ContextFactory {
  constructor(
    private readonly deps: ContextDependencies,
    private readonly callbacks: ContextCallbacks
  ) {}

  getLockContext(): LockContext {
    return {
      jobIndex: this.deps.jobIndex,
      jobLocks: this.deps.jobLocks,
      clientJobs: this.deps.clientJobs,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      eventsManager: this.deps.eventsManager,
      storage: this.deps.storage,
    };
  }

  getBackgroundContext(): BackgroundContext {
    return {
      config: this.deps.config,
      storage: this.deps.storage,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      jobIndex: this.deps.jobIndex,
      completedJobs: this.deps.completedJobs,
      depCompletions: this.deps.depCompletions,
      timedOutJobs: this.deps.timedOutJobs,
      jobResults: this.deps.jobResults,
      customIdMap: this.deps.customIdMap,
      jobLogs: this.deps.jobLogs,
      jobLocks: this.deps.jobLocks,
      clientJobs: this.deps.clientJobs,
      stalledCandidates: this.deps.stalledCandidates,
      pendingDepChecks: this.deps.pendingDepChecks,
      queueNamesCache: this.deps.queueNamesCache,
      eventsManager: this.deps.eventsManager,
      webhookManager: this.deps.webhookManager,
      metrics: this.deps.metrics,
      startTime: this.deps.startTime,
      perQueueMetrics: this.deps.perQueueMetrics,
      fail: this.callbacks.fail,
      registerQueueName: this.callbacks.registerQueueName,
      unregisterQueueName: this.callbacks.unregisterQueueName,
      dashboardEmit: this.callbacks.emitDashboardEvent,
      workerManager: this.deps.workerManager,
      monitoringState: this.deps.monitoringState,
      completedJobsData: this.deps.completedJobsData,
    };
  }

  getStatsContext(): StatsContext {
    return {
      shards: this.deps.shards,
      processingShards: this.deps.processingShards,
      completedJobs: this.deps.completedJobs,
      jobIndex: this.deps.jobIndex,
      jobResults: this.deps.jobResults,
      jobLogs: this.deps.jobLogs,
      customIdMap: this.deps.customIdMap,
      jobLocks: this.deps.jobLocks,
      clientJobs: this.deps.clientJobs,
      pendingDepChecks: this.deps.pendingDepChecks,
      stalledCandidates: this.deps.stalledCandidates,
      metrics: this.deps.metrics,
      startTime: this.deps.startTime,
      perQueueMetrics: this.deps.perQueueMetrics,
    };
  }

  getPushContext(): PushContext {
    return {
      storage: this.deps.storage,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      completedJobs: this.deps.completedJobs,
      completedJobsData: this.deps.completedJobsData,
      depCompletions: this.deps.depCompletions,
      timedOutJobs: this.deps.timedOutJobs,
      jobResults: this.deps.jobResults,
      customIdMap: this.deps.customIdMap,
      jobIndex: this.deps.jobIndex,
      totalPushed: this.deps.metrics.totalPushed,
      broadcast: this.deps.eventsManager.broadcast.bind(this.deps.eventsManager),
      dashboardEmit: this.callbacks.emitDashboardEvent,
    };
  }

  getPullContext(): PullContext {
    return {
      storage: this.deps.storage,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      jobIndex: this.deps.jobIndex,
      totalPulled: this.deps.metrics.totalPulled,
      broadcast: this.deps.eventsManager.broadcast.bind(this.deps.eventsManager),
      dashboardEmit: this.callbacks.emitDashboardEvent,
    };
  }

  getAckContext(): AckContext {
    return {
      storage: this.deps.storage,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      completedJobs: this.deps.completedJobs,
      completedJobsData: this.deps.completedJobsData,
      depCompletions: this.deps.depCompletions,
      jobResults: this.deps.jobResults,
      jobIndex: this.deps.jobIndex,
      customIdMap: this.deps.customIdMap,
      totalCompleted: this.deps.metrics.totalCompleted,
      totalFailed: this.deps.metrics.totalFailed,
      perQueueMetrics: this.deps.perQueueMetrics,
      broadcast: this.deps.eventsManager.broadcast.bind(this.deps.eventsManager),
      onJobCompleted: this.callbacks.onJobCompleted,
      onJobsCompleted: this.callbacks.onJobsCompleted,
      needsBroadcast: this.deps.eventsManager.needsBroadcast.bind(this.deps.eventsManager),
      hasPendingDeps: this.callbacks.hasPendingDeps,
      onRepeat: this.callbacks.onRepeat,
      emitDashboardEvent: this.callbacks.emitDashboardEvent,
      onChildTerminalFailure: this.callbacks.onChildTerminalFailure,
      onChildDependencyOption: this.callbacks.onChildDependencyOption,
    };
  }

  getJobMgmtContext(): JobManagementContext {
    return {
      storage: this.deps.storage,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      jobIndex: this.deps.jobIndex,
      jobLocks: this.deps.jobLocks,
      clientJobs: this.deps.clientJobs,
      webhookManager: this.deps.webhookManager,
      eventsManager: this.deps.eventsManager,
      repeatChain: this.deps.repeatChain,
    };
  }

  getQueryContext(): QueryContext {
    return {
      storage: this.deps.storage,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      jobIndex: this.deps.jobIndex,
      completedJobs: this.deps.completedJobs,
      completedJobsData: this.deps.completedJobsData,
      jobResults: this.deps.jobResults,
      customIdMap: this.deps.customIdMap,
    };
  }

  getDlqContext(): DlqContext {
    return {
      shards: this.deps.shards,
      jobIndex: this.deps.jobIndex,
      jobResults: this.deps.jobResults,
      jobLogs: this.deps.jobLogs,
      storage: this.deps.storage,
    };
  }

  getRetryCompletedContext(): RetryCompletedContext {
    return {
      shards: this.deps.shards,
      jobIndex: this.deps.jobIndex,
      jobLogs: this.deps.jobLogs,
      storage: this.deps.storage,
      completedJobs: this.deps.completedJobs,
      jobResults: this.deps.jobResults,
    };
  }

  getLogsContext() {
    return {
      jobIndex: this.deps.jobIndex,
      jobLogs: this.deps.jobLogs,
      maxLogsPerJob: this.deps.maxLogsPerJob,
    };
  }

  getQueueControlContext() {
    return {
      shards: this.deps.shards,
      jobIndex: this.deps.jobIndex,
      processingShards: this.deps.processingShards,
      completedJobs: this.deps.completedJobs,
      completedJobsData: this.deps.completedJobsData,
      jobResults: this.deps.jobResults,
      jobLogs: this.deps.jobLogs,
      storage: this.deps.storage,
    };
  }
}
