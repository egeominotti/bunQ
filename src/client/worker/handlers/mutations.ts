import { jobId, type Job as InternalJob } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import type { ChangePriorityOpts } from '../../types';
import type { TcpConnection } from '../types';
import { assertFlowTcpOk } from '../../flowJobTypes';

function tokenError(response: Record<string, unknown>): Error | null {
  if (response.ok === false && /token/i.test(String(response.error))) {
    return new Error(typeof response.error === 'string' ? response.error : 'Invalid lock token');
  }
  return null;
}

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
  options: { internalJob: InternalJob; token?: string; onTransitionApplied?: () => void }
): (id: string) => Promise<void> {
  const { internalJob, token, onTransitionApplied } = options;
  return async (id: string) => {
    if (embedded) {
      const manager = getSharedManager();
      const state = await manager.getJobState(jobId(id));
      if (state === 'failed') {
        const count = manager.retryDlq(internalJob.queue, jobId(id));
        if (count === 0) throw new Error(`Job ${id} is failed but not present in DLQ`);
        onTransitionApplied?.();
        return;
      }
      if (state === 'active') {
        const ok = await manager.moveActiveToWait(jobId(id), token);
        if (!ok) throw new Error(`Failed to retry active job ${id}`);
        onTransitionApplied?.();
        return;
      }
      if (state === 'waiting' || state === 'prioritized' || state === 'delayed') return;
      throw new Error(`Cannot retry job ${id} from state '${state}'`);
    }
    if (!tcp) return;
    const response = await tcp.send({
      cmd: 'MoveToWait',
      id,
      ...(token === undefined ? {} : { token }),
    });
    if (response.ok !== true) {
      const error = typeof response.error === 'string' ? response.error : 'retry failed';
      throw new Error(error);
    }
    onTransitionApplied?.();
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
    assertFlowTcpOk(await tcp.send({ cmd: 'Update', id, data }), 'Update');
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
  tcp: TcpConnection | null,
  token?: string,
  onTransitionApplied?: () => void
): (id: string, delay: number) => Promise<void> {
  return async (id: string, delay: number) => {
    if (embedded) {
      const changed = await getSharedManager().changeDelay(jobId(id), delay, token);
      if (!changed) throw new Error(`Failed to change delay for job ${id}`);
      onTransitionApplied?.();
      return;
    }
    if (!tcp) return;
    assertFlowTcpOk(
      await tcp.send({
        cmd: 'ChangeDelay',
        id,
        delay,
        ...(token === undefined ? {} : { token }),
      }),
      'ChangeDelay'
    );
    onTransitionApplied?.();
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
  tcp: TcpConnection | null,
  onTransitionApplied?: () => void
): (id: string, token?: string) => Promise<boolean> {
  return async (id: string, token?: string) => {
    if (embedded) {
      const moved = await getSharedManager().moveActiveToWait(jobId(id), token);
      if (moved) onTransitionApplied?.();
      return moved;
    }
    if (!tcp) return false;
    const response = await tcp.send({
      cmd: 'MoveToWait',
      id,
      ...(token === undefined ? {} : { token }),
    });
    const error = tokenError(response);
    if (error) throw error;
    const moved = response.ok === true;
    if (moved) onTransitionApplied?.();
    return moved;
  };
}

export function createMoveToDelayedHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  onTransitionApplied?: () => void
): (id: string, timestamp: number, token?: string) => Promise<void> {
  return async (id: string, timestamp: number, token?: string) => {
    const delay = Math.max(0, timestamp - Date.now());
    if (embedded) {
      const moved = await getSharedManager().moveToDelayed(jobId(id), delay, token);
      if (!moved) throw new Error(`Failed to move job ${id} to delayed`);
      onTransitionApplied?.();
      return;
    }
    if (!tcp) return;
    const response = await tcp.send({
      cmd: 'MoveToDelayed',
      id,
      delay,
      ...(token === undefined ? {} : { token }),
    });
    if (response.ok !== true) {
      throw new Error(
        typeof response.error === 'string' ? response.error : 'Failed to move job to delayed'
      );
    }
    onTransitionApplied?.();
  };
}

export function createMoveToWaitingChildrenHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  onTransitionApplied?: () => void
): (
  id: string,
  token?: string,
  _opts?: { child?: { id: string; queue: string } }
) => Promise<boolean> {
  return async (id: string, token?: string) => {
    if (embedded) {
      const moved = await getSharedManager().moveToWaitingChildren(jobId(id), token);
      if (moved) onTransitionApplied?.();
      return moved;
    }
    if (!tcp) return false;
    const response = await tcp.send({
      cmd: 'MoveToWaitingChildren',
      id,
      ...(token === undefined ? {} : { token }),
    });
    const error = tokenError(response);
    if (error) throw error;
    const moved = response.ok === true;
    if (moved) onTransitionApplied?.();
    return moved;
  };
}
