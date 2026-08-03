/**
 * ContextFactory - Builds context objects for delegated operations
 */

import type { LockContext, BackgroundContext, StatsContext } from './types';
import type { PushContext } from './operations/push';
import type { PullContext } from './operations/pull';
import type { AckContext } from './operations/ack';
import type { JobManagementContext } from './operations/jobManagement';
import type { QueryContext } from './operations/queryOperations';
import type { DlqContext, RetryCompletedContext } from './dlqManager';

import type { ContextCallbacks, ContextDependencies } from './types/contextFactory';

export type { ContextCallbacks, ContextDependencies } from './types/contextFactory';

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
      retiredCronLeaseTokens: this.deps.retiredCronLeaseTokens,
      clientJobs: this.deps.clientJobs,
      processingShards: this.deps.processingShards,
      processingLocks: this.deps.processingLocks,
      shards: this.deps.shards,
      shardLocks: this.deps.shardLocks,
      eventsManager: this.deps.eventsManager,
      storage: this.deps.storage,
      timeoutScheduler: this.deps.timeoutScheduler,
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
      maxDependencyCompletions: this.deps.config.maxCompletedJobs,
      timedOutJobs: this.deps.timedOutJobs,
      retiredTimeoutLeaseTokens: this.deps.retiredTimeoutLeaseTokens,
      timeoutScheduler: this.deps.timeoutScheduler,
      jobResults: this.deps.jobResults,
      dependencyResults: this.deps.dependencyResults,
      customIdMap: this.deps.customIdMap,
      jobLogs: this.deps.jobLogs,
      jobLocks: this.deps.jobLocks,
      retiredCronLeaseTokens: this.deps.retiredCronLeaseTokens,
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
      customIdLock: this.deps.customIdLock,
      completedJobs: this.deps.completedJobs,
      completedJobsData: this.deps.completedJobsData,
      depCompletions: this.deps.depCompletions,
      maxDependencyCompletions: this.deps.config.maxCompletedJobs,
      timedOutJobs: this.deps.timedOutJobs,
      retiredTimeoutLeaseTokens: this.deps.retiredTimeoutLeaseTokens,
      retiredCronLeaseTokens: this.deps.retiredCronLeaseTokens,
      jobResults: this.deps.jobResults,
      dependencyResults: this.deps.dependencyResults,
      customIdMap: this.deps.customIdMap,
      jobIndex: this.deps.jobIndex,
      totalPushed: this.deps.metrics.totalPushed,
      broadcast: this.deps.eventsManager.broadcast.bind(this.deps.eventsManager),
      dashboardEmit: this.callbacks.emitDashboardEvent,
      registerQueueName: this.callbacks.registerQueueName,
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
      retiredCronLeaseTokens: this.deps.retiredCronLeaseTokens,
      depCompletions: this.deps.depCompletions,
      maxDependencyCompletions: this.deps.config.maxCompletedJobs,
      jobResults: this.deps.jobResults,
      dependencyResults: this.deps.dependencyResults,
      jobIndex: this.deps.jobIndex,
      customIdMap: this.deps.customIdMap,
      totalCompleted: this.deps.metrics.totalCompleted,
      totalFailed: this.deps.metrics.totalFailed,
      perQueueMetrics: this.deps.perQueueMetrics,
      broadcast: this.deps.eventsManager.broadcast.bind(this.deps.eventsManager),
      onJobCompleted: this.callbacks.onJobCompleted,
      onJobFailed: this.callbacks.onJobFailed,
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
      dependencyResults: this.deps.dependencyResults,
      depCompletions: this.deps.depCompletions,
      maxDependencyCompletions: this.deps.config.maxCompletedJobs,
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
      dependencyResults: this.deps.dependencyResults,
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
      completedJobsData: this.deps.completedJobsData,
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
      dependencyResults: this.deps.dependencyResults,
      jobLogs: this.deps.jobLogs,
      storage: this.deps.storage,
    };
  }
}
