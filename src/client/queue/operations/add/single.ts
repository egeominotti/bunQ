import { getSharedManager } from '../../../manager';
import { removeJobDeduplicationKey } from '../../../jobDeduplication';
import type { TcpConnectionPool } from '../../../tcpPool';
import type { Job, JobOptions } from '../../../types';
import { toPublicJob } from '../../../types';
import { jobId } from '../../../../domain/types/job';
import { createJobProxy } from '../../jobProxy';
import type { AddContext, ExtendedJobOptions } from '../../types/add';
import { buildJobData, buildPushPayload, buildRepeatOptions } from './payload';

export async function add<T>(
  context: AddContext,
  jobName: string,
  data: T,
  options: JobOptions = {}
): Promise<Job<T>> {
  const merged = { ...context.opts.defaultJobOptions, ...options } as ExtendedJobOptions;
  const jobData = buildJobData(data, merged);

  if (context.embedded) {
    const manager = getSharedManager();
    const removeOnComplete =
      typeof merged.removeOnComplete === 'boolean' ? merged.removeOnComplete : false;
    const removeOnFail = typeof merged.removeOnFail === 'boolean' ? merged.removeOnFail : false;
    const repeat = merged.repeat ? buildRepeatOptions(merged.repeat) : undefined;

    const job = await manager.push(context.name, {
      name: jobName,
      data: jobData,
      priority: merged.priority,
      delay: merged.delay,
      maxAttempts: merged.attempts,
      backoff: merged.backoff,
      ttl: merged.ttl,
      timeout: merged.timeout,
      uniqueKey: merged.deduplication?.id,
      customId: merged.jobId,
      dependsOn: merged.dependsOn?.map((id: string) => jobId(id)),
      tags: merged.tags,
      groupId: merged.groupId,
      dedup: merged.deduplication
        ? {
            ttl: merged.deduplication.ttl,
            extend: merged.deduplication.extend,
            replace: merged.deduplication.replace,
          }
        : undefined,
      lifo: merged.lifo,
      removeOnComplete,
      removeOnFail,
      stallTimeout: merged.stallTimeout,
      durable: merged.durable,
      repeat,
      parentId: merged.parent ? jobId(merged.parent.id) : undefined,
      stackTraceLimit: merged.stackTraceLimit,
      keepLogs: merged.keepLogs,
      sizeLimit: merged.sizeLimit,
      failParentOnFailure: merged.failParentOnFailure,
      removeDependencyOnFailure: merged.removeDependencyOnFailure,
      continueParentOnFailure: merged.continueParentOnFailure,
      ignoreDependencyOnFailure: merged.ignoreDependencyOnFailure,
      timestamp: merged.timestamp,
      debounceId: merged.debounce?.id,
      debounceTtl: merged.debounce?.ttl,
    });

    return toPublicJob<T>({
      job,
      name: jobName,
      updateProgress: async (id, progress, message) => {
        await manager.updateProgress(jobId(id), progress, message);
      },
      // oxlint-disable-next-line typescript/require-await -- public callback contract is asynchronous
      log: async (id, message) => {
        manager.addLog(jobId(id), message);
      },
      getState: (id) => context.getJobState(id),
      remove: (id) => context.removeAsync(id),
      retry: (id) => context.retryJob(id),
      getChildrenValues: (id) => context.getChildrenValues(id),
      updateData: (id, value) => context.updateJobData(id, value),
      promote: (id) => context.promoteJob(id),
      changeDelay: (id, delay) => context.changeJobDelay(id, delay),
      changePriority: (id, value) => context.changeJobPriority(id, value),
      extendLock: (id, token, duration) => context.extendJobLock(id, token, duration),
      clearLogs: (id, keepLogs) => context.clearJobLogs(id, keepLogs),
      getDependencies: (id, opts) => context.getJobDependencies(id, opts),
      getDependenciesCount: (id, opts) => context.getJobDependenciesCount(id, opts),
      moveToCompleted: (id, result, token) => context.moveJobToCompleted(id, result, token),
      moveToFailed: (id, error, token) => context.moveJobToFailed(id, error, token),
      moveToWait: (id, token) => context.moveJobToWait(id, token),
      moveToDelayed: (id, timestamp, token) => context.moveJobToDelayed(id, timestamp, token),
      moveToWaitingChildren: (id, token, opts) => context.moveJobToWaitingChildren(id, token, opts),
      waitUntilFinished: (id, queueEvents, ttl) =>
        context.waitJobUntilFinished(id, queueEvents, ttl),
      removeDeduplicationKey: (id) => removeJobDeduplicationKey(id, true, null),
      discard: (id) => {
        void manager.discard(jobId(id));
      },
      getFailedChildrenValues: (id) => manager.getFailedChildrenValues(jobId(id)),
      getIgnoredChildrenFailures: (id) => manager.getIgnoredChildrenFailures(jobId(id)),
      removeChildDependency: (id) => manager.removeChildDependency(jobId(id)),
      removeUnprocessedChildren: async (id) => {
        await manager.removeUnprocessedChildren(jobId(id));
      },
    });
  }

  const tcp = context.tcp as TcpConnectionPool;
  const response = await tcp.send(buildPushPayload(context.name, jobName, jobData, merged));
  if (!response.ok) {
    throw new Error((response.error as string | undefined) ?? 'Failed to add job');
  }

  return createJobProxy(
    response.id as string,
    jobName,
    data,
    {
      queueName: context.name,
      tcp,
      getJobState: context.getJobState,
      removeAsync: context.removeAsync,
      retryJob: context.retryJob,
      getChildrenValues: context.getChildrenValues,
    },
    { priority: merged.priority, delay: merged.delay, opts: merged }
  );
}
