import { jobId } from '../domain/types/job';
import type { ChangePriorityOpts } from './types';
import { getSharedManager } from './manager';
import { getFlowDependencies } from './flowJobDependencies';
import { assertFlowTcpOk, type FlowJobRuntime } from './flowJobTypes';

/** Core state, mutation, and dependency methods exposed by a FlowProducer Job. */
export function buildFlowJobCoreMethods(runtime: FlowJobRuntime) {
  const { id, queueName, callbacks, embedded, tcp, getState } = runtime;
  return {
    updateProgress: async (progress: number | object) => {
      if (callbacks?.updateProgress) return callbacks.updateProgress(id, progress);
      if (embedded) {
        const value = typeof progress === 'number' ? progress : 0;
        const message = typeof progress === 'object' ? JSON.stringify(progress) : undefined;
        return void (await getSharedManager().updateProgress(jobId(id), value, message));
      }
      if (!tcp) return;
      const value = typeof progress === 'number' ? progress : 0;
      const message = typeof progress === 'object' ? JSON.stringify(progress) : undefined;
      assertFlowTcpOk(
        await tcp.send({ cmd: 'Progress', id, progress: value, message }),
        'Progress'
      );
    },
    log: async (message: string) => {
      if (callbacks?.log) return callbacks.log(id, message);
      if (embedded) return void getSharedManager().addLog(jobId(id), message);
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'AddLog', id, message }), 'AddLog');
    },
    getState,
    remove: async () => {
      if (callbacks?.remove) return callbacks.remove(id);
      if (embedded) return void (await getSharedManager().cancel(jobId(id)));
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'Cancel', id }), 'Cancel');
    },
    retry: async () => {
      if (callbacks?.retry) return callbacks.retry(id);
      if (embedded) {
        const manager = getSharedManager();
        const state = await manager.getJobState(jobId(id));
        if (state === 'failed') {
          if (manager.retryDlq(queueName, jobId(id)) === 0) {
            throw new Error(`Job ${id} is failed but not present in DLQ`);
          }
          return;
        }
        if (state === 'active') {
          if (!(await manager.moveActiveToWait(jobId(id)))) {
            throw new Error(`Failed to retry active job ${id}`);
          }
          return;
        }
        if (state === 'waiting' || state === 'prioritized' || state === 'delayed') return;
        throw new Error(`Cannot retry job ${id} from state '${state}'`);
      }
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'MoveToWait', id }), 'MoveToWait');
    },
    getChildrenValues: async <R = unknown>() => {
      if (embedded) {
        return (await getSharedManager().getChildrenValues(jobId(id))) as Record<string, R>;
      }
      if (!tcp) return {};
      const response = await tcp.send({ cmd: 'GetChildrenValues', id });
      assertFlowTcpOk(response, 'GetChildrenValues');
      const values =
        (response as { data?: { values?: Record<string, unknown> } }).data?.values ?? {};
      return values as Record<string, R>;
    },
    isWaiting: async () => (await getState()) === 'waiting',
    isActive: async () => (await getState()) === 'active',
    isDelayed: async () => (await getState()) === 'delayed',
    isCompleted: async () => (await getState()) === 'completed',
    isFailed: async () => (await getState()) === 'failed',
    isWaitingChildren: async () => (await getState()) === 'waiting-children',
    updateData: async (data: unknown) => {
      if (callbacks?.updateData) return callbacks.updateData(id, data);
      if (embedded) return void (await getSharedManager().updateJobData(jobId(id), data));
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'Update', id, data }), 'Update');
    },
    promote: async () => {
      if (callbacks?.promote) return callbacks.promote(id);
      if (embedded) return void (await getSharedManager().promote(jobId(id)));
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'Promote', id }), 'Promote');
    },
    changeDelay: async (delay: number) => {
      if (callbacks?.changeDelay) return callbacks.changeDelay(id, delay);
      if (embedded) return void (await getSharedManager().changeDelay(jobId(id), delay));
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'ChangeDelay', id, delay }), 'ChangeDelay');
    },
    changePriority: async (opts: ChangePriorityOpts) => {
      if (callbacks?.changePriority) {
        return callbacks.changePriority(id, { priority: opts.priority, lifo: opts.lifo });
      }
      if (embedded) {
        return void (await getSharedManager().changePriority(jobId(id), opts.priority, opts.lifo));
      }
      if (!tcp) return;
      assertFlowTcpOk(
        await tcp.send({ cmd: 'ChangePriority', id, priority: opts.priority, lifo: opts.lifo }),
        'ChangePriority'
      );
    },
    extendLock: async (token: string, duration: number) => {
      if (embedded) {
        return (await getSharedManager().extendLock(jobId(id), token, duration)) ? duration : 0;
      }
      if (!tcp) return 0;
      const response = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
      assertFlowTcpOk(response, 'ExtendLock');
      return duration;
    },
    clearLogs: async (keepLogs?: number) => {
      if (callbacks?.clearLogs) return callbacks.clearLogs(id, keepLogs);
      if (embedded) return void getSharedManager().clearLogs(jobId(id), keepLogs);
      if (!tcp) return;
      assertFlowTcpOk(await tcp.send({ cmd: 'ClearLogs', id, keepLogs }), 'ClearLogs');
    },
    getDependencies: () => getFlowDependencies(id, queueName, embedded, tcp),
    getDependenciesCount: async () => {
      const dependencies = await getFlowDependencies(id, queueName, embedded, tcp);
      return {
        processed: Object.keys(dependencies.processed).length,
        unprocessed: dependencies.unprocessed.length,
      };
    },
  };
}
