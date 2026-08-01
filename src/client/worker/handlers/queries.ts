import type { EventEmitter } from 'events';
import { jobId } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import type { FlowJobData, Job, JobStateType } from '../../types';
import type { TcpConnection } from '../types';

export function createProgressHandler<T extends FlowJobData>(
  embedded: boolean,
  tcp: TcpConnection | null,
  emitter: EventEmitter,
  jobHolder: { current: Job<T> | null }
) {
  return async (id: string, progress: number, message?: string) => {
    if (embedded) {
      const manager = getSharedManager();
      await manager.updateProgress(jobId(id), progress, message);
    } else if (tcp) {
      await tcp.send({ cmd: 'Progress', id, progress, message });
    }
    emitter.emit('progress', jobHolder.current, progress);
  };
}

export function createLogHandler<T extends FlowJobData>(
  embedded: boolean,
  tcp: TcpConnection | null,
  emitter: EventEmitter,
  jobHolder: { current: Job<T> | null }
) {
  return async (id: string, message: string) => {
    if (embedded) {
      const manager = getSharedManager();
      manager.addLog(jobId(id), message);
    } else if (tcp) {
      await tcp.send({ cmd: 'AddLog', id, message });
    }
    emitter.emit('log', jobHolder.current, message);
  };
}

export function createGetStateHandler(embedded: boolean, tcp: TcpConnection | null) {
  return async (id: string): Promise<JobStateType> => {
    if (embedded) {
      const manager = getSharedManager();
      return (await manager.getJobState(jobId(id))) as JobStateType;
    } else if (tcp) {
      const response = await tcp.send({ cmd: 'GetState', id });
      return ((response as { state?: string }).state ?? 'unknown') as JobStateType;
    }
    return 'unknown';
  };
}

export function createGetChildrenValuesHandler(embedded: boolean, tcp: TcpConnection | null) {
  return async (id: string): Promise<Record<string, unknown>> => {
    if (embedded) {
      const manager = getSharedManager();
      return manager.getChildrenValues(jobId(id));
    } else if (tcp) {
      const response = await tcp.send({ cmd: 'GetChildrenValues', id });
      const data = (response as { data?: { values?: Record<string, unknown> } }).data;
      return data?.values ?? {};
    }
    return {};
  };
}

export function createGetFailedChildrenValuesHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<Record<string, string>> {
  return async (id: string) => {
    if (embedded) {
      const manager = getSharedManager();
      return manager.getFailedChildrenValues(jobId(id));
    }
    if (!tcp) return {};
    const response = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
    return (response.values as Record<string, string> | undefined) ?? {};
  };
}

export function createGetIgnoredChildrenFailuresHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<Record<string, string>> {
  return async (id: string) => {
    if (embedded) {
      const manager = getSharedManager();
      return manager.getIgnoredChildrenFailures(jobId(id));
    }
    if (!tcp) return {};
    const response = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
    return (response.values as Record<string, string> | undefined) ?? {};
  };
}

export function createRemoveChildDependencyHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<boolean> {
  return async (id: string) => {
    if (embedded) {
      const manager = getSharedManager();
      return manager.removeChildDependency(jobId(id));
    }
    if (!tcp) return false;
    const response = await tcp.send({ cmd: 'RemoveChildDependency', id });
    return response.ok === true;
  };
}

export function createRemoveUnprocessedChildrenHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => Promise<void> {
  return async (id: string) => {
    if (embedded) {
      const manager = getSharedManager();
      return manager.removeUnprocessedChildren(jobId(id));
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'RemoveUnprocessedChildren', id });
  };
}
