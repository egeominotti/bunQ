import type { ChangePriorityOpts, Job } from '../../types';
import { removeJobDeduplicationKey } from '../../jobDeduplication';
import { getSharedManager } from '../../manager';
import { jobId } from '../../../domain/types/job';
import { buildFailCommand, failEmbeddedArgs } from '../failWire';
import type { SimpleJobContext } from '../types/job';
import { computeSimpleDependencies } from './dependencies';
import { reflectFields } from './reflection';

/** Create a Job wired to either the embedded manager or a TCP connection. */
export function createSimpleJob<T>(
  id: string,
  name: string,
  data: T,
  timestamp: number,
  ctx: SimpleJobContext
): Job<T> {
  const { queueName, embedded, tcp, meta } = ctx;
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
    updateProgress: async (progress, message) => {
      if (embedded) {
        await getSharedManager().updateProgress(jobId(id), progress, message);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Progress', id, progress, message });
    },
    log: async (message) => {
      if (embedded) {
        getSharedManager().addLog(jobId(id), message);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'AddLog', id, message });
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
      if (embedded) {
        await getSharedManager().updateJobData(jobId(id), newData);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Update', id, data: newData });
    },
    promote: async () => {
      if (embedded) {
        await getSharedManager().promote(jobId(id));
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Promote', id });
    },
    changeDelay: async (delay) => {
      if (embedded) {
        await getSharedManager().changeDelay(jobId(id), delay);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'ChangeDelay', id, delay });
    },
    changePriority: async (opts: ChangePriorityOpts) => {
      if (embedded) {
        await getSharedManager().changePriority(jobId(id), opts.priority, opts.lifo);
        return;
      }
      if (tcp) {
        await tcp.send({ cmd: 'ChangePriority', id, priority: opts.priority, lifo: opts.lifo });
      }
    },
    extendLock: async (token, duration) => {
      if (embedded) {
        const ok = await getSharedManager().extendLock(jobId(id), token, duration);
        return ok ? duration : 0;
      }
      if (!tcp) return 0;
      const response = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
      return response.ok === true ? duration : 0;
    },
    clearLogs: async (keepLogs) => {
      if (embedded) {
        getSharedManager().clearLogs(jobId(id), keepLogs);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'ClearLogs', id, keepLogs });
    },
    getDependencies: () => computeSimpleDependencies(id, queueName, embedded, tcp),
    getDependenciesCount: async () => {
      const dependencies = await computeSimpleDependencies(id, queueName, embedded, tcp);
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
      if (embedded) {
        await getSharedManager().ack(jobId(id), returnValue);
        return null;
      }
      if (tcp) await tcp.send({ cmd: 'ACK', id, result: returnValue });
      return null;
    },
    moveToFailed: async (error) => {
      if (embedded) {
        await getSharedManager().fail(jobId(id), ...failEmbeddedArgs(error));
        return;
      }
      if (tcp) await tcp.send(buildFailCommand(id, error));
    },
    moveToWait: async () => {
      if (embedded) {
        const manager = getSharedManager();
        const state = await manager.getJobState(jobId(id));
        if (state === 'active') return await manager.moveActiveToWait(jobId(id));
        if (state === 'delayed') return await manager.promote(jobId(id));
        if (state === 'failed') {
          const job = await manager.getJob(jobId(id));
          if (!job) return false;
          return manager.retryDlq(job.queue, jobId(id)) > 0;
        }
        if (state === 'waiting' || state === 'prioritized') return true;
        return false;
      }
      if (!tcp) return false;
      const response = await tcp.send({ cmd: 'MoveToWait', id });
      return response.ok === true;
    },
    moveToDelayed: async (targetTimestamp) => {
      const delay = Math.max(0, targetTimestamp - Date.now());
      if (embedded) {
        await getSharedManager().moveToDelayed(jobId(id), delay);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'MoveToDelayed', id, delay });
    },
    moveToWaitingChildren: async () => {
      if (embedded) {
        return await getSharedManager().moveToWaitingChildren(jobId(id));
      }
      if (!tcp) return false;
      const response = await tcp.send({ cmd: 'MoveToWaitingChildren', id });
      return response.ok === true;
    },
    waitUntilFinished: async (_queueEvents, ttl) => {
      const timeout = ttl ?? 30000;
      if (embedded) {
        const manager = getSharedManager();
        const job = await manager.getJob(jobId(id));
        if (!job) throw new Error(`Job ${id} not found`);
        if (job.completedAt) return manager.getResult(jobId(id));
        const ok = await manager.waitForJobCompletion(jobId(id), timeout);
        if (!ok) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
        return manager.getResult(jobId(id));
      }
      if (!tcp) throw new Error('waitUntilFinished: no connection');
      const response = await tcp.send({ cmd: 'WaitJob', id, timeout });
      const typed = response as { completed?: boolean; result?: unknown };
      if (!typed.completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
      return typed.result;
    },
    discard: () => {
      if (embedded) {
        void getSharedManager().discard(jobId(id));
        return;
      }
      if (tcp) void tcp.send({ cmd: 'Discard', id });
    },
    getFailedChildrenValues: async () => {
      if (embedded) return await getSharedManager().getFailedChildrenValues(jobId(id));
      if (!tcp) return {};
      const response = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async () => {
      if (embedded) return await getSharedManager().getIgnoredChildrenFailures(jobId(id));
      if (!tcp) return {};
      const response = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async () => {
      if (embedded) return await getSharedManager().removeChildDependency(jobId(id));
      if (!tcp) return false;
      const response = await tcp.send({ cmd: 'RemoveChildDependency', id });
      return (response.removed as boolean | undefined) ?? false;
    },
    removeDeduplicationKey: () => removeJobDeduplicationKey(id, embedded ?? false, tcp),
    removeUnprocessedChildren: async () => {
      if (embedded) {
        await getSharedManager().removeUnprocessedChildren(jobId(id));
        return;
      }
      if (tcp) await tcp.send({ cmd: 'RemoveUnprocessedChildren', id });
    },
  };
}
