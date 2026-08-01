import type { Job } from '../../types';
import { removeJobDeduplicationKey } from '../../jobDeduplication';
import { buildFailCommand } from '../failWire';
import type { JobProxyContext, JobReflectionMeta } from '../types/job';
import { computeDependencies } from './dependencies';
import { reflectFields } from './reflection';

/** Create a full Job proxy whose methods use the TCP transport. */
export function createJobProxy<T>(
  id: string,
  name: string,
  data: T,
  ctx: JobProxyContext,
  meta?: JobReflectionMeta
): Job<T> {
  const { tcp, queueName } = ctx;
  const timestamp = meta?.timestamp ?? Date.now();
  const reflected = reflectFields(id, queueName, meta);

  return {
    id,
    name,
    data,
    queueName,
    attemptsMade: 0,
    timestamp,
    progress: 0,
    delay: reflected.delay,
    processedOn: undefined,
    finishedOn: undefined,
    stacktrace: reflected.stacktrace,
    failedReason: reflected.failedReason,
    stalledCounter: 0,
    priority: reflected.priority,
    parent: reflected.parent,
    parentKey: reflected.parentKey,
    opts: reflected.opts,
    token: undefined,
    processedBy: undefined,
    deduplicationId: reflected.deduplicationId,
    repeatJobKey: reflected.repeatJobKey,
    attemptsStarted: 0,
    updateProgress: async (progress: number, message?: string) => {
      await tcp.send({ cmd: 'Progress', id, progress, message });
    },
    log: async (message: string) => {
      await tcp.send({ cmd: 'AddLog', id, message });
    },
    getState: () => ctx.getJobState(id),
    remove: () => ctx.removeAsync(id),
    retry: () => ctx.retryJob(id),
    getChildrenValues: <R = unknown>() => ctx.getChildrenValues(id) as Promise<Record<string, R>>,
    isWaiting: async () => (await ctx.getJobState(id)) === 'waiting',
    isActive: async () => (await ctx.getJobState(id)) === 'active',
    isDelayed: async () => (await ctx.getJobState(id)) === 'delayed',
    isCompleted: async () => (await ctx.getJobState(id)) === 'completed',
    isFailed: async () => (await ctx.getJobState(id)) === 'failed',
    isWaitingChildren: async () => (await ctx.getJobState(id)) === 'waiting-children',
    updateData: async (newData) => {
      await tcp.send({ cmd: 'Update', id, data: newData });
    },
    promote: async () => {
      await tcp.send({ cmd: 'Promote', id });
    },
    changeDelay: async (delay) => {
      await tcp.send({ cmd: 'ChangeDelay', id, delay });
    },
    changePriority: async (opts) => {
      await tcp.send({ cmd: 'ChangePriority', id, priority: opts.priority, lifo: opts.lifo });
    },
    extendLock: async (token, duration) => {
      const response = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
      return response.ok === true ? duration : 0;
    },
    clearLogs: async () => {
      await tcp.send({ cmd: 'ClearLogs', id });
    },
    getDependencies: () => computeDependencies(id, queueName, tcp),
    getDependenciesCount: async () => {
      const dependencies = await computeDependencies(id, queueName, tcp);
      return {
        processed: Object.keys(dependencies.processed).length,
        unprocessed: dependencies.unprocessed.length,
      };
    },
    toJSON: () => ({
      id,
      name,
      data,
      opts: reflected.opts,
      progress: 0,
      delay: reflected.delay,
      timestamp,
      attemptsMade: 0,
      stacktrace: reflected.stacktrace,
      failedReason: reflected.failedReason,
      queueQualifiedName: `bull:${queueName}`,
      parentKey: reflected.parentKey,
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(data),
      opts: JSON.stringify(reflected.opts),
      progress: '0',
      delay: String(reflected.delay),
      timestamp: String(timestamp),
      attemptsMade: '0',
      stacktrace: reflected.stacktrace ? JSON.stringify(reflected.stacktrace) : null,
      failedReason: reflected.failedReason,
      parentKey: reflected.parentKey,
    }),
    moveToCompleted: async (returnValue) => {
      await tcp.send({ cmd: 'ACK', id, result: returnValue });
      return null;
    },
    moveToFailed: async (error) => {
      await tcp.send(buildFailCommand(id, error));
    },
    moveToWait: async () => {
      const response = await tcp.send({ cmd: 'MoveToWait', id });
      return response.ok === true;
    },
    moveToDelayed: async (targetTimestamp) => {
      const delay = Math.max(0, targetTimestamp - Date.now());
      await tcp.send({ cmd: 'MoveToDelayed', id, delay });
    },
    moveToWaitingChildren: async () => {
      const response = await tcp.send({ cmd: 'MoveToWaitingChildren', id });
      return response.ok === true;
    },
    waitUntilFinished: async (_queueEvents, ttl) => {
      const timeout = ttl ?? 30000;
      const response = await tcp.send({ cmd: 'WaitJob', id, timeout });
      const typed = response as { completed?: boolean; result?: unknown };
      if (!typed.completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
      return typed.result;
    },
    discard: () => {
      void tcp.send({ cmd: 'Discard', id });
    },
    getFailedChildrenValues: async () => {
      const response = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async () => {
      const response = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async () => {
      const response = await tcp.send({ cmd: 'RemoveChildDependency', id });
      return (response.removed as boolean | undefined) ?? false;
    },
    removeDeduplicationKey: () => removeJobDeduplicationKey(id, false, tcp),
    removeUnprocessedChildren: async () => {
      await tcp.send({ cmd: 'RemoveUnprocessedChildren', id });
    },
  };
}
