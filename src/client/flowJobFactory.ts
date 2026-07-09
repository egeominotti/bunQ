/**
 * Flow Job Factory
 * Creates simple Job objects for FlowProducer results
 */

import type { Job, ChangePriorityOpts, JobStateType } from './types';
import type { TcpConnectionPool } from './tcpPool';
import { getSharedManager } from './manager';
import { buildFailCommand, failEmbeddedArgs } from './queue/failWire';
import { jobId } from '../domain/types/job';

/** Callbacks for flow job mutation operations */
export interface FlowJobCallbacks {
  /** Execution mode — enables full method wiring when provided */
  embedded?: boolean;
  tcp?: TcpConnectionPool | null;
  updateData?: (id: string, data: unknown) => Promise<void>;
  updateProgress?: (id: string, progress: number | object) => Promise<void>;
  log?: (id: string, message: string) => Promise<void>;
  promote?: (id: string) => Promise<void>;
  remove?: (id: string) => Promise<void>;
  changePriority?: (id: string, opts: { priority: number; lifo?: boolean }) => Promise<void>;
  changeDelay?: (id: string, delay: number) => Promise<void>;
  clearLogs?: (id: string) => Promise<void>;
  retry?: (id: string) => Promise<void>;
  getState?: (id: string) => Promise<string>;
}

/** Extract user data (remove internal fields like __parentId, __childrenIds, name) */
export function extractUserDataFromInternal(data: Record<string, unknown>): unknown {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data)) {
    if (!key.startsWith('__') && key !== 'name') {
      result[key] = value;
    }
  }
  return result;
}

/** Create a simple Job object for flow results */
export function createFlowJobObject<T>(
  id: string,
  name: string,
  data: T,
  queueName: string,
  callbacks?: FlowJobCallbacks
): Job<T> {
  const ts = Date.now();
  const embedded = !!callbacks?.embedded;
  const tcp = callbacks?.tcp ?? null;

  const stateAsType = (s: string): JobStateType => {
    const valid: JobStateType[] = [
      'waiting',
      'active',
      'completed',
      'failed',
      'delayed',
      'prioritized',
      'waiting-children',
      'unknown',
    ];
    return (valid.includes(s as JobStateType) ? s : 'unknown') as JobStateType;
  };

  const getStateInternal = (): Promise<JobStateType> => {
    if (callbacks?.getState) return callbacks.getState(id).then(stateAsType);
    if (embedded) {
      return getSharedManager()
        .getJobState(jobId(id))
        .then((s) => stateAsType(s));
    }
    if (tcp) {
      return tcp.send({ cmd: 'GetState', id }).then((r) => {
        const s = (r as { state?: string }).state ?? 'unknown';
        return stateAsType(s);
      });
    }
    return Promise.resolve('unknown' as JobStateType);
  };

  return {
    id,
    name,
    data,
    queueName,
    attemptsMade: 0,
    timestamp: ts,
    progress: 0,
    delay: 0,
    processedOn: undefined,
    finishedOn: undefined,
    stacktrace: null,
    stalledCounter: 0,
    priority: 0,
    parentKey: undefined,
    opts: {},
    token: undefined,
    processedBy: undefined,
    deduplicationId: undefined,
    repeatJobKey: undefined,
    attemptsStarted: 0,

    updateProgress: async (progress) => {
      if (callbacks?.updateProgress) {
        return callbacks.updateProgress(id, progress);
      }
      if (embedded) {
        await getSharedManager().updateProgress(jobId(id), progress);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Progress', id, progress: progress });
    },
    log: async (message) => {
      if (callbacks?.log) return callbacks.log(id, message);
      if (embedded) {
        getSharedManager().addLog(jobId(id), message);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'AddLog', id, message });
    },
    getState: getStateInternal,
    remove: async () => {
      if (callbacks?.remove) return callbacks.remove(id);
      if (embedded) {
        await getSharedManager().cancel(jobId(id));
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Cancel', id });
    },
    retry: async () => {
      if (callbacks?.retry) return callbacks.retry(id);
      if (embedded) {
        const mgr = getSharedManager();
        const state = await mgr.getJobState(jobId(id));
        if (state === 'failed') {
          const count = mgr.retryDlq(queueName, jobId(id));
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
    },
    getChildrenValues: async <R = unknown>() => {
      if (embedded) {
        return (await getSharedManager().getChildrenValues(jobId(id))) as Record<string, R>;
      }
      if (!tcp) return {};
      const res = await tcp.send({ cmd: 'GetChildrenValues', id });
      const vals = (res as { data?: { values?: Record<string, unknown> } }).data?.values ?? {};
      return vals as Record<string, R>;
    },

    isWaiting: async () => (await getStateInternal()) === 'waiting',
    isActive: async () => (await getStateInternal()) === 'active',
    isDelayed: async () => (await getStateInternal()) === 'delayed',
    isCompleted: async () => (await getStateInternal()) === 'completed',
    isFailed: async () => (await getStateInternal()) === 'failed',
    isWaitingChildren: async () => (await getStateInternal()) === 'waiting-children',

    updateData: async (newData) => {
      if (callbacks?.updateData) return callbacks.updateData(id, newData);
      if (embedded) {
        await getSharedManager().updateJobData(jobId(id), newData);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Update', id, data: newData });
    },
    promote: async () => {
      if (callbacks?.promote) return callbacks.promote(id);
      if (embedded) {
        await getSharedManager().promote(jobId(id));
        return;
      }
      if (tcp) await tcp.send({ cmd: 'Promote', id });
    },
    changeDelay: async (delay) => {
      if (callbacks?.changeDelay) return callbacks.changeDelay(id, delay);
      if (embedded) {
        await getSharedManager().changeDelay(jobId(id), delay);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'ChangeDelay', id, delay });
    },
    changePriority: async (opts: ChangePriorityOpts) => {
      if (callbacks?.changePriority) {
        return callbacks.changePriority(id, { priority: opts.priority, lifo: opts.lifo });
      }
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
      const res = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
      return res.ok === true ? duration : 0;
    },
    clearLogs: async (keepLogs) => {
      if (callbacks?.clearLogs) return callbacks.clearLogs(id);
      if (embedded) {
        getSharedManager().clearLogs(jobId(id), keepLogs);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'ClearLogs', id, keepLogs });
    },

    getDependencies: async () => {
      const childIds = await fetchChildIds(id, embedded, tcp);
      return computeDepsFlow(queueName, childIds, embedded, tcp);
    },
    getDependenciesCount: async () => {
      const childIds = await fetchChildIds(id, embedded, tcp);
      const deps = await computeDepsFlow(queueName, childIds, embedded, tcp);
      return {
        processed: Object.keys(deps.processed).length,
        unprocessed: deps.unprocessed.length,
      };
    },

    toJSON: () => ({
      id,
      name,
      data,
      opts: {},
      progress: 0,
      delay: 0,
      timestamp: ts,
      attemptsMade: 0,
      stacktrace: null,
      queueQualifiedName: `bull:${queueName}`,
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(data),
      opts: '{}',
      progress: '0',
      delay: '0',
      timestamp: String(ts),
      attemptsMade: '0',
      stacktrace: null,
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
      // Forward stacktrace + UnrecoverableError (was dropped; #74/#111-class).
      if (embedded) {
        await getSharedManager().fail(jobId(id), ...failEmbeddedArgs(error));
        return;
      }
      if (tcp) await tcp.send(buildFailCommand(id, error));
    },
    moveToWait: async () => {
      if (embedded) return await getSharedManager().moveActiveToWait(jobId(id));
      if (!tcp) return false;
      const res = await tcp.send({ cmd: 'MoveToWait', id });
      return res.ok === true;
    },
    moveToDelayed: async (timestamp) => {
      const delay = Math.max(0, timestamp - Date.now());
      if (embedded) {
        await getSharedManager().moveToDelayed(jobId(id), delay);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'MoveToDelayed', id, delay });
    },
    moveToWaitingChildren: async () => {
      if (embedded) return await getSharedManager().moveToWaitingChildren(jobId(id));
      throw new Error(
        'moveToWaitingChildren is not supported in TCP mode — no server command available'
      );
    },
    waitUntilFinished: async (_qe, ttl) => {
      const timeout = ttl ?? 30000;
      if (embedded) {
        const mgr = getSharedManager();
        const job = await mgr.getJob(jobId(id));
        if (!job) throw new Error(`Job ${id} not found`);
        if (job.completedAt) return mgr.getResult(jobId(id));
        const ok = await mgr.waitForJobCompletion(jobId(id), timeout);
        if (!ok) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
        return mgr.getResult(jobId(id));
      }
      if (!tcp) throw new Error('waitUntilFinished: no connection');
      const res = await tcp.send({ cmd: 'WaitJob', id, timeout });
      const typed = res as { completed?: boolean; result?: unknown };
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
      const res = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
      return (res.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async () => {
      if (embedded) return await getSharedManager().getIgnoredChildrenFailures(jobId(id));
      if (!tcp) return {};
      const res = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
      return (res.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async () => {
      if (embedded) return await getSharedManager().removeChildDependency(jobId(id));
      if (!tcp) return false;
      const res = await tcp.send({ cmd: 'RemoveChildDependency', id });
      return (res.removed as boolean | undefined) ?? false;
    },
    removeDeduplicationKey: (): Promise<boolean> =>
      Promise.reject(
        new Error('removeDeduplicationKey is not implemented — no server primitive available')
      ),
    removeUnprocessedChildren: async () => {
      if (embedded) {
        await getSharedManager().removeUnprocessedChildren(jobId(id));
        return;
      }
      if (tcp) await tcp.send({ cmd: 'RemoveUnprocessedChildren', id });
    },
  };
}

async function fetchChildIds(
  id: string,
  embedded: boolean,
  tcp: TcpConnectionPool | null
): Promise<string[]> {
  if (embedded) {
    const job = await getSharedManager().getJob(jobId(id));
    return (job?.childrenIds ?? []).map(String);
  }
  if (!tcp) return [];
  const jobRes = await tcp.send({ cmd: 'GetJob', id });
  const parent = (jobRes as { job?: { childrenIds?: string[] } }).job;
  return (parent?.childrenIds ?? []).map(String);
}

async function computeDepsFlow(
  queueName: string,
  childIds: string[],
  embedded: boolean,
  tcp: TcpConnectionPool | null
): Promise<{ processed: Record<string, unknown>; unprocessed: string[] }> {
  const processed: Record<string, unknown> = {};
  const unprocessed: string[] = [];
  for (const cid of childIds) {
    let state = 'unknown';
    let result: unknown;
    if (embedded) {
      const mgr = getSharedManager();
      state = await mgr.getJobState(jobId(cid));
      if (state === 'completed') result = mgr.getResult(jobId(cid));
    } else if (tcp) {
      const r = await tcp.send({ cmd: 'GetState', id: cid });
      state = (r as { state?: string }).state ?? 'unknown';
      if (state === 'completed') {
        const rr = await tcp.send({ cmd: 'GetResult', id: cid });
        result = (rr as { result?: unknown }).result;
      }
    }
    const key = `${queueName}:${cid}`;
    if (state === 'completed' || state === 'failed') {
      processed[key] = result ?? null;
    } else {
      unprocessed.push(key);
    }
  }
  return { processed, unprocessed };
}
