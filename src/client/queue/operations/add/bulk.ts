import { getSharedManager } from '../../../manager';
import type { TcpConnectionPool } from '../../../tcpPool';
import type { Job, JobOptions } from '../../../types';
import { jobId } from '../../../../domain/types/job';
import { createJobProxy, createSimpleJob } from '../../jobProxy';
import type { AddContext, ExtendedJobOptions } from '../../types/add';
import { buildBulkData, buildRepeatOptions, compact, reflectionMeta } from './payload';

export async function addBulk<T>(
  context: AddContext,
  jobs: Array<{ name: string; data: T; opts?: JobOptions }>
): Promise<Job<T>[]> {
  if (jobs.length === 0) return [];
  const now = Date.now();
  const merged: ExtendedJobOptions[] = jobs.map(({ opts }) => ({
    ...context.opts.defaultJobOptions,
    ...opts,
  }));

  if (context.embedded) {
    const manager = getSharedManager();
    const inputs = jobs.map(({ name, data }, index) => {
      const options = merged[index];
      const removeOnComplete =
        typeof options.removeOnComplete === 'boolean' ? options.removeOnComplete : false;
      const removeOnFail = typeof options.removeOnFail === 'boolean' ? options.removeOnFail : false;
      return {
        data: buildBulkData(name, data, options),
        priority: options.priority,
        delay: options.delay,
        maxAttempts: options.attempts,
        backoff: options.backoff,
        timeout: options.timeout,
        ttl: options.ttl,
        customId: options.jobId,
        uniqueKey: options.deduplication?.id,
        dependsOn: options.dependsOn?.map((id: string) => jobId(id)),
        parentId: options.parent ? jobId(options.parent.id) : undefined,
        tags: options.tags,
        groupId: options.groupId,
        stallTimeout: options.stallTimeout,
        timestamp: options.timestamp,
        removeOnComplete,
        removeOnFail,
        repeat: options.repeat ? buildRepeatOptions(options.repeat) : undefined,
        durable: options.durable,
        lifo: options.lifo,
        stackTraceLimit: options.stackTraceLimit,
        keepLogs: options.keepLogs,
        sizeLimit: options.sizeLimit,
        failParentOnFailure: options.failParentOnFailure,
        removeDependencyOnFailure: options.removeDependencyOnFailure,
        continueParentOnFailure: options.continueParentOnFailure,
        ignoreDependencyOnFailure: options.ignoreDependencyOnFailure,
        dedup: options.deduplication
          ? {
              ttl: options.deduplication.ttl,
              extend: options.deduplication.extend,
              replace: options.deduplication.replace,
            }
          : undefined,
        debounceId: options.debounce?.id,
        debounceTtl: options.debounce?.ttl,
      };
    });

    const ids = await manager.pushBatch(context.name, inputs);
    return ids.map((id, index) =>
      createSimpleJob(String(id), jobs[index].name, jobs[index].data, now, {
        queueName: context.name,
        embedded: context.embedded,
        tcp: context.tcp,
        getJobState: context.getJobState,
        removeAsync: context.removeAsync,
        retryJob: context.retryJob,
        getChildrenValues: context.getChildrenValues,
        meta: reflectionMeta(merged[index]),
      })
    );
  }

  const tcp = context.tcp as TcpConnectionPool;
  const batchJobs = jobs.map(({ name, data }, index) => {
    const options = merged[index];
    const removeOnComplete =
      typeof options.removeOnComplete === 'boolean' ? options.removeOnComplete : false;
    const removeOnFail = typeof options.removeOnFail === 'boolean' ? options.removeOnFail : false;
    return compact({
      data: buildBulkData(name, data, options),
      priority: options.priority,
      delay: options.delay,
      maxAttempts: options.attempts,
      backoff: options.backoff,
      timeout: options.timeout,
      ttl: options.ttl,
      customId: options.jobId,
      tags: options.tags,
      groupId: options.groupId,
      dependsOn: options.dependsOn?.map((id: string) => jobId(id)),
      parentId: options.parent ? jobId(options.parent.id) : undefined,
      uniqueKey: options.deduplication?.id,
      dedup: options.deduplication
        ? {
            ttl: options.deduplication.ttl,
            extend: options.deduplication.extend,
            replace: options.deduplication.replace,
          }
        : undefined,
      lifo: options.lifo,
      stallTimeout: options.stallTimeout,
      timestamp: options.timestamp,
      removeOnComplete,
      removeOnFail,
      repeat: options.repeat,
      durable: options.durable,
      stackTraceLimit: options.stackTraceLimit,
      keepLogs: options.keepLogs,
      sizeLimit: options.sizeLimit,
      failParentOnFailure: options.failParentOnFailure,
      removeDependencyOnFailure: options.removeDependencyOnFailure,
      continueParentOnFailure: options.continueParentOnFailure,
      ignoreDependencyOnFailure: options.ignoreDependencyOnFailure,
      debounceId: options.debounce?.id,
      debounceTtl: options.debounce?.ttl,
    });
  });

  const response = await tcp.send({ cmd: 'PUSHB', queue: context.name, jobs: batchJobs });
  if (!response.ok) {
    throw new Error((response.error as string | undefined) ?? 'Failed to add jobs');
  }

  const ids = (response.ids ?? []) as string[];
  return ids.map((id, index) =>
    createJobProxy(
      id,
      jobs[index].name,
      jobs[index].data,
      {
        queueName: context.name,
        tcp,
        getJobState: context.getJobState,
        removeAsync: context.removeAsync,
        retryJob: context.retryJob,
        getChildrenValues: context.getChildrenValues,
      },
      reflectionMeta(merged[index])
    )
  );
}
