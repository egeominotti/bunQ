import type { ContextCallbacks, ContextDependencies } from '../contextFactory';
import type { QueueManagerRuntime, QueueManagerStateView } from '../types';
import { handleRepeat } from './repeat';

export function managerRuntime(value: unknown): QueueManagerRuntime {
  return value as QueueManagerRuntime;
}

export function createContextDependencies(state: QueueManagerStateView): ContextDependencies {
  return {
    config: state.config,
    storage: state.storage,
    shards: state.shards,
    shardLocks: state.shardLocks,
    customIdLock: state.customIdLock,
    processingShards: state.processingShards,
    processingLocks: state.processingLocks,
    jobIndex: state.jobIndex,
    completedJobs: state.completedJobs,
    completedJobsData: state.completedJobsData,
    depCompletions: state.depCompletions,
    timedOutJobs: state.timedOutJobs,
    retiredTimeoutLeaseTokens: state.retiredTimeoutLeaseTokens,
    retiredCronLeaseTokens: state.retiredCronLeaseTokens,
    timeoutScheduler: state.timeoutScheduler,
    jobResults: state.jobResults,
    jobResultQueues: state.jobResultQueues,
    dependencyResults: state.dependencyResults,
    customIdMap: state.customIdMap,
    jobLogs: state.jobLogs,
    jobLogQueues: state.jobLogQueues,
    jobLocks: state.jobLocks,
    clientJobs: state.clientJobs,
    stalledCandidates: state.stalledCandidates,
    pendingDepChecks: state.pendingDepChecks,
    pendingQueueAdmissions: state.pendingQueueAdmissions,
    queueNamesCache: state.queueNamesCache,
    repeatChain: state.repeatChain,
    eventsManager: state.eventsManager,
    webhookManager: state.webhookManager,
    workerManager: state.workerManager,
    monitoringState: state.monitoringState,
    metrics: state.metrics,
    startTime: state.startTime,
    maxLogsPerJob: state.maxLogsPerJob,
    perQueueMetrics: state.perQueueMetrics,
  };
}

export function createContextCallbacks(runtime: QueueManagerRuntime): ContextCallbacks {
  return {
    fail: async (id, error, failureReason) => {
      if (failureReason) await runtime.failWithReason(id, error, failureReason);
      else await runtime.fail(id, error);
    },
    registerQueueName: (queue) => runtime.registerQueueName(queue),
    unregisterQueueName: (queue) => runtime.unregisterQueueName(queue),
    onQueueAdmissionsDrained: (queue) => runtime.onQueueAdmissionsDrained(queue),
    onJobCompleted: (id) => runtime.onJobCompleted(id),
    onJobFailed: (id) => runtime.onJobFailed(id),
    onJobsCompleted: (ids) => runtime.onJobsCompleted(ids),
    hasPendingDeps: () => runtime.hasPendingDeps(),
    onRepeat: (job) => handleRepeat(runtime, job),
    emitDashboardEvent: (event, data) => runtime.emitDashboardEvent(event, data),
    onChildTerminalFailure: (job, error) => runtime.failParentOnChildFailure(job, error),
    onChildDependencyOption: (job, error) => runtime.onChildDependencyOption(job, error),
  };
}
