import { jobId } from '../../domain/types/job';
import { getFlowDependencies } from '../flowJobDependencies';
import type { PublicJobMethodContext } from '../jobConversionTypes';
import { getSharedManager } from '../manager';
import { removeJobDeduplicationKey } from '../jobDeduplication';
import type { TcpConnectionPool } from '../tcpPool';
import { buildFailCommand, failEmbeddedArgs } from './failWire';
import type { SimpleJobContext } from './jobProxy';

export interface DlqJobContext extends Pick<
  SimpleJobContext,
  'getJobState' | 'removeAsync' | 'retryJob' | 'getChildrenValues'
> {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

function assertCommand(response: Record<string, unknown>, operation: string): void {
  if (response.ok === true) return;
  throw new Error(typeof response.error === 'string' ? response.error : `${operation} failed`);
}

export function createDlqJobMethods(ctx: DlqJobContext): PublicJobMethodContext {
  const manager = () => getSharedManager();
  const send = async (command: Record<string, unknown>, operation: string) => {
    if (!ctx.tcp) throw new Error(`${operation}: no connection`);
    const response = await ctx.tcp.send(command);
    assertCommand(response, operation);
    return response;
  };
  const dependencies = (id: string) => getFlowDependencies(id, ctx.name, ctx.embedded, ctx.tcp);

  return {
    updateProgress: async (id, progress, message) => {
      if (ctx.embedded) {
        if (!(await manager().updateProgress(jobId(id), progress, message))) {
          throw new Error(`Job ${id} is not active`);
        }
      } else await send({ cmd: 'Progress', id, progress, message }, 'Progress');
    },
    log: async (id, message) => {
      if (ctx.embedded) manager().addLog(jobId(id), message);
      else await send({ cmd: 'AddLog', id, message }, 'AddLog');
    },
    getState: ctx.getJobState,
    remove: ctx.removeAsync,
    retry: ctx.retryJob,
    getChildrenValues: ctx.getChildrenValues,
    updateData: async (id, data) => {
      if (ctx.embedded) await manager().updateJobData(jobId(id), data);
      else await send({ cmd: 'Update', id, data }, 'Update');
    },
    promote: async (id) => {
      if (ctx.embedded) await manager().promote(jobId(id));
      else await send({ cmd: 'Promote', id }, 'Promote');
    },
    changeDelay: async (id, delay) => {
      if (ctx.embedded) await manager().changeDelay(jobId(id), delay);
      else await send({ cmd: 'ChangeDelay', id, delay }, 'ChangeDelay');
    },
    changePriority: async (id, options) => {
      if (ctx.embedded) await manager().changePriority(jobId(id), options.priority, options.lifo);
      else {
        await send(
          { cmd: 'ChangePriority', id, priority: options.priority, lifo: options.lifo },
          'ChangePriority'
        );
      }
    },
    extendLock: async (id, token, duration) => {
      if (ctx.embedded)
        return (await manager().extendLock(jobId(id), token, duration)) ? duration : 0;
      const response = await send({ cmd: 'ExtendLock', id, token, duration }, 'ExtendLock');
      return response.ok === true ? duration : 0;
    },
    clearLogs: async (id, keepLogs) => {
      if (ctx.embedded) manager().clearLogs(jobId(id), keepLogs);
      else await send({ cmd: 'ClearLogs', id, keepLogs }, 'ClearLogs');
    },
    getDependencies: async (id) => dependencies(id),
    getDependenciesCount: async (id) => {
      const value = await dependencies(id);
      return {
        processed: Object.keys(value.processed).length,
        unprocessed: value.unprocessed.length,
      };
    },
    moveToCompleted: async (id, result, token) => {
      if (ctx.embedded) await manager().ack(jobId(id), result, token);
      else await send({ cmd: 'ACK', id, result, token }, 'ACK');
      return null;
    },
    moveToFailed: async (id, error, token) => {
      if (ctx.embedded) await manager().fail(jobId(id), ...failEmbeddedArgs(error, token));
      else await send(buildFailCommand(id, error, token), 'FAIL');
    },
    moveToWait: async (id, token) => {
      if (ctx.embedded) {
        const state = await manager().getJobState(jobId(id));
        if (state === 'failed') return manager().retryDlq(ctx.name, jobId(id)) > 0;
        if (state === 'active') return manager().moveActiveToWait(jobId(id), token);
        if (state === 'delayed') return manager().promote(jobId(id));
        return state === 'waiting' || state === 'prioritized';
      }
      const response = await send(
        { cmd: 'MoveToWait', id, ...(token === undefined ? {} : { token }) },
        'MoveToWait'
      );
      return response.ok === true;
    },
    moveToDelayed: async (id, timestamp, token) => {
      const delay = Math.max(0, timestamp - Date.now());
      if (ctx.embedded) await manager().moveToDelayed(jobId(id), delay, token);
      else {
        await send(
          { cmd: 'MoveToDelayed', id, delay, ...(token === undefined ? {} : { token }) },
          'MoveToDelayed'
        );
      }
    },
    moveToWaitingChildren: async (id, token) => {
      if (ctx.embedded) return manager().moveToWaitingChildren(jobId(id), token);
      await send(
        { cmd: 'MoveToWaitingChildren', id, ...(token === undefined ? {} : { token }) },
        'MoveToWaitingChildren'
      );
      return true;
    },
    waitUntilFinished: async (id, _events, ttl) => {
      const timeout = ttl ?? 30_000;
      if (ctx.embedded) {
        const current = await manager().getJob(jobId(id));
        if (!current) throw new Error(`Job ${id} not found`);
        if (current.completedAt) return manager().getResult(jobId(id));
        if (!(await manager().waitForJobCompletion(jobId(id), timeout))) {
          throw new Error(`Job ${id} timed out after ${timeout}ms`);
        }
        return manager().getResult(jobId(id));
      }
      const response = await send({ cmd: 'WaitJob', id, timeout }, 'WaitJob');
      if (!response.completed) throw new Error(`Job ${id} timed out after ${timeout}ms`);
      return response.result;
    },
    discard: (id) => {
      if (ctx.embedded) void manager().discard(jobId(id));
      else if (ctx.tcp) void ctx.tcp.send({ cmd: 'Discard', id });
    },
    getFailedChildrenValues: async (id) => {
      if (ctx.embedded) return manager().getFailedChildrenValues(jobId(id));
      const response = await send(
        { cmd: 'GetFailedChildrenValues', id },
        'GetFailedChildrenValues'
      );
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async (id) => {
      if (ctx.embedded) return manager().getIgnoredChildrenFailures(jobId(id));
      const response = await send(
        { cmd: 'GetIgnoredChildrenFailures', id },
        'GetIgnoredChildrenFailures'
      );
      return (response.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async (id) => {
      if (ctx.embedded) return manager().removeChildDependency(jobId(id));
      const response = await send({ cmd: 'RemoveChildDependency', id }, 'RemoveChildDependency');
      return (response.removed as boolean | undefined) ?? false;
    },
    removeDeduplicationKey: (id) => removeJobDeduplicationKey(id, ctx.embedded, ctx.tcp),
    removeUnprocessedChildren: async (id) => {
      if (ctx.embedded) await manager().removeUnprocessedChildren(jobId(id));
      else await send({ cmd: 'RemoveUnprocessedChildren', id }, 'RemoveUnprocessedChildren');
    },
  };
}
