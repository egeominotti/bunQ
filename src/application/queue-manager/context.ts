import type { Job } from '../../domain/types/job';
import type { ContextCallbacks, ContextDependencies } from '../contextFactory';
import type { QueueManagerRuntime, QueueManagerStateView } from '../types';

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
    jobResults: state.jobResults,
    dependencyResults: state.dependencyResults,
    customIdMap: state.customIdMap,
    jobLogs: state.jobLogs,
    jobLocks: state.jobLocks,
    clientJobs: state.clientJobs,
    stalledCandidates: state.stalledCandidates,
    pendingDepChecks: state.pendingDepChecks,
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
    fail: (id, error, failureReason) =>
      failureReason ? runtime.failWithReason(id, error, failureReason) : runtime.fail(id, error),
    registerQueueName: (queue) => runtime.registerQueueName(queue),
    unregisterQueueName: (queue) => runtime.unregisterQueueName(queue),
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

function handleRepeat(runtime: QueueManagerRuntime, job: Job): void {
  if (!job.repeat) return;
  const oldId = job.id;
  void runtime
    .push(job.queue, {
      data: job.data,
      priority: job.priority,
      delay: job.repeat.every ?? 0,
      maxAttempts: job.maxAttempts,
      backoff: job.backoff,
      ttl: job.ttl ?? undefined,
      timeout: job.timeout ?? undefined,
      tags: job.tags,
      groupId: job.groupId ?? undefined,
      lifo: job.lifo,
      removeOnComplete: job.removeOnComplete,
      removeOnFail: job.removeOnFail,
      repeat: {
        every: job.repeat.every,
        limit: job.repeat.limit,
        pattern: job.repeat.pattern,
        count: job.repeat.count + 1,
      },
    })
    .then((newJob) => {
      runtime.repeatChain.set(oldId, newJob.id);
      if (runtime.repeatChain.size > 10_000) {
        const first = runtime.repeatChain.keys().next().value;
        if (first !== undefined) runtime.repeatChain.delete(first);
      }
    });
}
