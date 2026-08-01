import { jobId, type Job as InternalJob } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import type { ChangePriorityOpts } from '../../types';
import type { TcpConnection } from '../types';

export function createRemoveHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<void> {
  return async (id: string) => {
    if (embedded) {
      await getSharedManager().cancel(jobId(id));
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'Cancel', id });
  };
}

export function createRetryHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob
): (id: string) => Promise<void> {
  return async (id: string) => {
    if (embedded) {
      const manager = getSharedManager();
      const state = await manager.getJobState(jobId(id));
      if (state === 'failed') {
        const count = manager.retryDlq(internalJob.queue, jobId(id));
        if (count === 0) throw new Error(`Job ${id} is failed but not present in DLQ`);
        return;
      }
      if (state === 'active') {
        const ok = await manager.moveActiveToWait(jobId(id));
        if (!ok) throw new Error(`Failed to retry active job ${id}`);
        return;
      }
      if (state === 'waiting' || state === 'prioritized' || state === 'delayed') return;
      throw new Error(`Cannot retry job ${id} from state '${state}'`);
    }
    if (!tcp) return;
    const response = await tcp.send({ cmd: 'MoveToWait', id });
    if (response.ok !== true) {
      const error = typeof response.error === 'string' ? response.error : 'retry failed';
      throw new Error(error);
    }
  };
}

export function createUpdateDataHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, data: unknown) => Promise<void> {
  return async (id: string, data: unknown) => {
    if (embedded) {
      await getSharedManager().updateJobData(jobId(id), data);
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'Update', id, data });
  };
}

export function createPromoteHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<void> {
  return async (id: string) => {
    if (embedded) {
      await getSharedManager().promote(jobId(id));
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'Promote', id });
  };
}

export function createChangeDelayHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, delay: number) => Promise<void> {
  return async (id: string, delay: number) => {
    if (embedded) {
      await getSharedManager().changeDelay(jobId(id), delay);
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'ChangeDelay', id, delay });
  };
}

export function createChangePriorityHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, options: ChangePriorityOpts) => Promise<void> {
  return async (id: string, options: ChangePriorityOpts) => {
    if (embedded) {
      await getSharedManager().changePriority(jobId(id), options.priority, options.lifo);
      return;
    }
    if (!tcp) return;
    await tcp.send({
      cmd: 'ChangePriority',
      id,
      priority: options.priority,
      lifo: options.lifo,
    });
  };
}

export function createExtendLockHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, token: string, duration: number) => Promise<number> {
  return async (id: string, token: string, duration: number) => {
    if (embedded) {
      const ok = await getSharedManager().extendLock(jobId(id), token, duration);
      return ok ? duration : 0;
    }
    if (!tcp) return 0;
    const response = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
    return response.ok === true ? duration : 0;
  };
}

export function createClearLogsHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, keepLogs?: number) => Promise<void> {
  return async (id: string, keepLogs?: number) => {
    if (embedded) {
      getSharedManager().clearLogs(jobId(id), keepLogs);
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'ClearLogs', id, keepLogs });
  };
}

export function createMoveToWaitHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, _token?: string) => Promise<boolean> {
  return async (id: string, _token?: string) => {
    if (embedded) return await getSharedManager().moveActiveToWait(jobId(id));
    if (!tcp) return false;
    const response = await tcp.send({ cmd: 'MoveToWait', id });
    return response.ok === true;
  };
}

export function createMoveToDelayedHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, timestamp: number, _token?: string) => Promise<void> {
  return async (id: string, timestamp: number, _token?: string) => {
    const delay = Math.max(0, timestamp - Date.now());
    if (embedded) {
      await getSharedManager().moveToDelayed(jobId(id), delay);
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'MoveToDelayed', id, delay });
  };
}

export function createMoveToWaitingChildrenHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (
  id: string,
  _token?: string,
  _opts?: { child?: { id: string; queue: string } }
) => Promise<boolean> {
  return async (id: string) => {
    if (embedded) return await getSharedManager().moveToWaitingChildren(jobId(id));
    if (!tcp) return false;
    const response = await tcp.send({ cmd: 'MoveToWaitingChildren', id });
    return response.ok === true;
  };
}
