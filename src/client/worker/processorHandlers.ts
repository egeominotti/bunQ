/**
 * Processor Handler Factories
 * Creates callback handlers for job operations during processing
 */

import type { EventEmitter } from 'events';
import type { TcpConnection } from './types';
import type {
  Job,
  FlowJobData,
  JobStateType,
  ChangePriorityOpts,
  JobDependencies,
  JobDependenciesCount,
} from '../types';
import type { Job as InternalJob } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import { getSharedManager } from '../manager';
import type { AckBatcher } from './ackBatcher';

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
    const res = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
    return (res.values as Record<string, string> | undefined) ?? {};
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
    const res = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
    return (res.values as Record<string, string> | undefined) ?? {};
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
    const res = await tcp.send({ cmd: 'RemoveChildDependency', id });
    return res.ok === true;
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

/**
 * Bug #74: split an Error's stack into trimmed, non-empty lines.
 * `wireStack` is the bandwidth-capped copy sent to the server (50 lines); the
 * authoritative cap (job.stackTraceLimit) is applied server-side in failJob.
 * Shared by the natural-throw path (handleJobFailure) and the explicit
 * moveToFailed() path so both persist the stack identically.
 */
export function computeStackLines(err: Error): {
  stackLines: string[];
  wireStack: string[] | undefined;
} {
  const stackLines = err.stack
    ? err.stack
        .split('\n')
        .map((l) => l.trim())
        .filter(Boolean)
    : [];
  const wireStack = stackLines.length > 0 ? stackLines.slice(0, 50) : undefined;
  return { stackLines, wireStack };
}

/** Issue #82: Create moveToFailed handler for use inside processor */
export function createMoveToFailedHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob,
  token: string | null | undefined,
  onCalled: (error: Error) => void
): (id: string, error: Error, _lockToken?: string) => Promise<void> {
  return async (_id: string, error: Error, _lockToken?: string) => {
    // Bug #74 follow-up: carry the stack on explicit moveToFailed() too. The
    // natural-throw path already does; @arthurvanl's repro showed the manual
    // path lost it. Compute before the send so the server can persist it.
    const { wireStack } = computeStackLines(error);
    if (embedded) {
      const manager = getSharedManager();
      await manager.fail(internalJob.id, error.message, token ?? undefined, undefined, wireStack);
    } else if (tcp) {
      await tcp.send({
        cmd: 'FAIL',
        id: internalJob.id,
        error: error.message,
        ...(wireStack ? { stack: wireStack } : {}),
        ...(token ? { token } : {}),
      });
    }
    onCalled(error);
  };
}

/** Issue #82: Create moveToCompleted handler for use inside processor */
export function createMoveToCompletedHandler(
  embedded: boolean,
  ackBatcher: AckBatcher,
  internalJob: InternalJob,
  token: string | null | undefined,
  onCalled: (value: unknown) => void
): (id: string, returnValue: unknown, _lockToken?: string) => Promise<unknown> {
  return async (_id: string, returnValue: unknown, _lockToken?: string) => {
    if (embedded) {
      const manager = getSharedManager();
      await manager.ack(internalJob.id, returnValue, token ?? undefined);
    } else {
      await ackBatcher.queue(String(internalJob.id), returnValue, token ?? undefined);
    }
    onCalled(returnValue);
    return null;
  };
}

// ============================================================
// Job mutation handlers (Issue #82 follow-up: wire remaining no-op methods)
// ============================================================

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
    // BullMQ contract: Job.retry() moves a failed job back to waiting.
    // Dispatch by state so it also works on an active job that wants to requeue.
    if (embedded) {
      const mgr = getSharedManager();
      const state = await mgr.getJobState(jobId(id));
      if (state === 'failed') {
        const count = mgr.retryDlq(internalJob.queue, jobId(id));
        if (count === 0) throw new Error(`Job ${id} is failed but not present in DLQ`);
        return;
      }
      if (state === 'active') {
        const ok = await mgr.moveActiveToWait(jobId(id));
        if (!ok) throw new Error(`Failed to retry active job ${id}`);
        return;
      }
      if (state === 'waiting' || state === 'prioritized' || state === 'delayed') return;
      throw new Error(`Cannot retry job ${id} from state '${state}'`);
    }
    if (!tcp) return;
    const res = await tcp.send({ cmd: 'MoveToWait', id });
    if (res.ok !== true) {
      const err = typeof res.error === 'string' ? res.error : 'retry failed';
      throw new Error(err);
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
): (id: string, opts: ChangePriorityOpts) => Promise<void> {
  return async (id: string, opts: ChangePriorityOpts) => {
    if (embedded) {
      await getSharedManager().changePriority(jobId(id), opts.priority, opts.lifo);
      return;
    }
    if (!tcp) return;
    await tcp.send({ cmd: 'ChangePriority', id, priority: opts.priority, lifo: opts.lifo });
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
    const res = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
    return res.ok === true ? duration : 0;
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
    if (embedded) {
      return await getSharedManager().moveActiveToWait(jobId(id));
    }
    if (!tcp) return false;
    const res = await tcp.send({ cmd: 'MoveToWait', id });
    return res.ok === true;
  };
}

export function createMoveToDelayedHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, timestamp: number, _token?: string) => Promise<void> {
  return async (id: string, timestamp: number, _token?: string) => {
    // BullMQ API passes absolute timestamp; server expects relative delay (ms)
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
    if (embedded) {
      return await getSharedManager().moveToWaitingChildren(jobId(id));
    }
    if (!tcp) return false;
    throw new Error(
      'moveToWaitingChildren is not supported in TCP mode — no server command available'
    );
  };
}

export function createWaitUntilFinishedHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string, _queueEvents: unknown, ttl?: number) => Promise<unknown> {
  return async (id: string, _queueEvents: unknown, ttl?: number) => {
    const timeout = ttl ?? 30000;
    if (embedded) {
      const manager = getSharedManager();
      const job = await manager.getJob(jobId(id));
      if (!job) throw new Error(`Job ${id} not found`);
      if (job.completedAt) return manager.getResult(jobId(id));
      const completed = await manager.waitForJobCompletion(jobId(id), timeout);
      if (!completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
      return manager.getResult(jobId(id));
    }
    if (!tcp) throw new Error('waitUntilFinished: no connection');
    const res = await tcp.send({ cmd: 'WaitJob', id, timeout });
    const typed = res as { completed?: boolean; result?: unknown };
    if (!typed.completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
    return typed.result;
  };
}

export function createDiscardHandler(
  embedded: boolean,
  tcp: TcpConnection | null
): (id: string) => void {
  return (id: string) => {
    if (embedded) {
      void getSharedManager().discard(jobId(id));
      return;
    }
    if (!tcp) return;
    void tcp.send({ cmd: 'Discard', id });
  };
}

/**
 * getDependencies — compute from internalJob.childrenIds by querying each child's state.
 * No dedicated server API; derive from existing job state queries.
 */
export function createGetDependenciesHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob
): (_id: string) => Promise<JobDependencies> {
  return async () => {
    const childIds = internalJob.childrenIds;
    const processed: Record<string, unknown> = {};
    const unprocessed: string[] = [];
    for (const cid of childIds) {
      let state: string = 'unknown';
      let result: unknown;
      if (embedded) {
        const mgr = getSharedManager();
        state = await mgr.getJobState(cid);
        if (state === 'completed') result = mgr.getResult(cid);
      } else if (tcp) {
        const r = await tcp.send({ cmd: 'GetState', id: String(cid) });
        state = (r as { state?: string }).state ?? 'unknown';
        if (state === 'completed') {
          const rr = await tcp.send({ cmd: 'GetResult', id: String(cid) });
          result = (rr as { result?: unknown }).result;
        }
      }
      const key = `${internalJob.queue}:${String(cid)}`;
      if (state === 'completed' || state === 'failed') {
        processed[key] = result ?? null;
      } else {
        unprocessed.push(key);
      }
    }
    return { processed, unprocessed };
  };
}

export function createGetDependenciesCountHandler(
  embedded: boolean,
  tcp: TcpConnection | null,
  internalJob: InternalJob
): (_id: string) => Promise<JobDependenciesCount> {
  const getDeps = createGetDependenciesHandler(embedded, tcp, internalJob);
  return async (id: string) => {
    const deps = await getDeps(id);
    return {
      processed: Object.keys(deps.processed).length,
      unprocessed: deps.unprocessed.length,
    };
  };
}

/**
 * removeDeduplicationKey — no server primitive; throw explicit error so callers
 * learn this isn't supported rather than silently getting `false`.
 */
export function createRemoveDeduplicationKeyHandler(): (_id: string) => Promise<boolean> {
  return (): Promise<boolean> =>
    Promise.reject(
      new Error('removeDeduplicationKey is not implemented — no server primitive available')
    );
}
