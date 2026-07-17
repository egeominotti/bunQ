/**
 * Job Proxy
 * Creates Job objects with methods for TCP and embedded modes
 */

import type { Job, JobOptions } from '../types';
import type { TcpConnectionPool } from '../tcpPool';

import type { JobStateType, ChangePriorityOpts } from '../types';
import { getSharedManager } from '../manager';
import { jobId } from '../../domain/types/job';
import { buildFailCommand, failEmbeddedArgs } from './failWire';

interface JobProxyContext {
  queueName: string;
  tcp: TcpConnectionPool;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
}

/**
 * Options actually requested for the job, so the returned Job object reflects
 * them (priority, delay, opts) instead of hardcoded defaults. See issue #88.
 */
export interface JobReflectionMeta {
  priority?: number;
  delay?: number;
  opts?: JobOptions;
  timestamp?: number;
  /** Persisted stacktrace of the last failure, from the server/manager job (#74) */
  stacktrace?: string[] | null;
  /** Last failure's error message, derived from the job timeline (#74) */
  failedReason?: string;
}

/** Fields derived from the requested options. Mirrors the embedded path
 * (jobConversionHelpers.buildJobProperties / jobHelpers) so a job returned
 * over TCP reflects deduplicationId/parentKey/parent/repeatJobKey/opts/delay
 * identically to embedded mode. See issues #88 and #90. */
interface ReflectedFields {
  delay: number;
  priority: number;
  opts: JobOptions;
  deduplicationId: string | undefined;
  parentKey: string | undefined;
  parent: { id: string; queueQualifiedName: string } | undefined;
  repeatJobKey: string | undefined;
  stacktrace: string[] | null;
  failedReason: string | undefined;
}

function reflectFields(id: string, queueName: string, meta?: JobReflectionMeta): ReflectedFields {
  const opts = meta?.opts ?? {};
  const p = opts.parent;
  const repeat = opts.repeat;
  const pattern = repeat?.pattern ?? (repeat?.every ? `every:${repeat.every}` : '');
  return {
    delay: meta?.delay ?? 0,
    priority: meta?.priority ?? 0,
    opts,
    deduplicationId: opts.jobId ?? opts.deduplication?.id,
    parentKey: p ? `${p.queue}:${p.id}` : undefined,
    parent: p ? { id: p.id, queueQualifiedName: p.queue } : undefined,
    repeatJobKey: repeat ? `${queueName}:${id}:${pattern}` : undefined,
    stacktrace: meta?.stacktrace ?? null,
    failedReason: meta?.failedReason,
  };
}

/** Create a full Job proxy with TCP methods */
export function createJobProxy<T>(
  id: string,
  name: string,
  data: T,
  ctx: JobProxyContext,
  meta?: JobReflectionMeta
): Job<T> {
  const { tcp, queueName } = ctx;
  const ts = meta?.timestamp ?? Date.now();
  const r = reflectFields(id, queueName, meta);

  return {
    id,
    name,
    data,
    queueName,
    attemptsMade: 0,
    timestamp: ts,
    progress: 0,
    delay: r.delay,
    processedOn: undefined,
    finishedOn: undefined,
    stacktrace: r.stacktrace,
    failedReason: r.failedReason,
    stalledCounter: 0,
    priority: r.priority,
    parent: r.parent,
    parentKey: r.parentKey,
    opts: r.opts,
    token: undefined,
    processedBy: undefined,
    deduplicationId: r.deduplicationId,
    repeatJobKey: r.repeatJobKey,
    attemptsStarted: 0,

    // Methods
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

    // State check methods
    isWaiting: async () => (await ctx.getJobState(id)) === 'waiting',
    isActive: async () => (await ctx.getJobState(id)) === 'active',
    isDelayed: async () => (await ctx.getJobState(id)) === 'delayed',
    isCompleted: async () => (await ctx.getJobState(id)) === 'completed',
    isFailed: async () => (await ctx.getJobState(id)) === 'failed',
    isWaitingChildren: async () => (await ctx.getJobState(id)) === 'waiting-children',

    // Mutation methods
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
      const res = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
      return res.ok === true ? duration : 0;
    },
    clearLogs: async () => {
      await tcp.send({ cmd: 'ClearLogs', id });
    },

    // Dependency methods — derive from child state queries since no dedicated API
    getDependencies: () => computeDependencies(id, queueName, tcp),
    getDependenciesCount: async () => {
      const deps = await computeDependencies(id, queueName, tcp);
      return {
        processed: Object.keys(deps.processed).length,
        unprocessed: deps.unprocessed.length,
      };
    },

    // Serialization methods
    toJSON: () => ({
      id,
      name,
      data,
      opts: r.opts,
      progress: 0,
      delay: r.delay,
      timestamp: ts,
      attemptsMade: 0,
      stacktrace: r.stacktrace,
      failedReason: r.failedReason,
      queueQualifiedName: `bull:${queueName}`,
      parentKey: r.parentKey,
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(data),
      opts: JSON.stringify(r.opts),
      progress: '0',
      delay: String(r.delay),
      timestamp: String(ts),
      attemptsMade: '0',
      stacktrace: r.stacktrace ? JSON.stringify(r.stacktrace) : null,
      failedReason: r.failedReason,
      parentKey: r.parentKey,
    }),

    // Move methods
    moveToCompleted: async (returnValue) => {
      await tcp.send({ cmd: 'ACK', id, result: returnValue });
      return null;
    },
    moveToFailed: async (error) => {
      // Forward stacktrace + UnrecoverableError (was dropped; #74/#111-class).
      await tcp.send(buildFailCommand(id, error));
    },
    moveToWait: async () => {
      const res = await tcp.send({ cmd: 'MoveToWait', id });
      return res.ok === true;
    },
    moveToDelayed: async (timestamp) => {
      const delay = Math.max(0, timestamp - Date.now());
      await tcp.send({ cmd: 'MoveToDelayed', id, delay });
    },
    moveToWaitingChildren: (): Promise<boolean> => {
      return Promise.reject(
        new Error(
          'moveToWaitingChildren is not supported in TCP mode — no server command available'
        )
      );
    },
    waitUntilFinished: async (_queueEvents, ttl) => {
      const timeout = ttl ?? 30000;
      const res = await tcp.send({ cmd: 'WaitJob', id, timeout });
      const typed = res as { completed?: boolean; result?: unknown };
      if (!typed.completed) throw new Error(`waitUntilFinished timed out after ${timeout}ms`);
      return typed.result;
    },

    // Additional methods
    discard: () => {
      void tcp.send({ cmd: 'Discard', id });
    },
    getFailedChildrenValues: async () => {
      const res = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
      return (res.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async () => {
      const res = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
      return (res.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async () => {
      const res = await tcp.send({ cmd: 'RemoveChildDependency', id });
      return (res.removed as boolean | undefined) ?? false;
    },
    removeDeduplicationKey: (): Promise<boolean> =>
      Promise.reject(
        new Error('removeDeduplicationKey is not implemented — no server primitive available')
      ),
    removeUnprocessedChildren: async () => {
      await tcp.send({ cmd: 'RemoveUnprocessedChildren', id });
    },
  };
}

/** Fetch child ids for a job via GetJob, then derive dependency state per child */
async function computeDependencies(
  id: string,
  queueName: string,
  tcp: TcpConnectionPool
): Promise<{ processed: Record<string, unknown>; unprocessed: string[] }> {
  const jobRes = await tcp.send({ cmd: 'GetJob', id });
  const parent = (jobRes as { job?: { childrenIds?: string[] } }).job;
  const childIds = parent?.childrenIds ?? [];
  const processed: Record<string, unknown> = {};
  const unprocessed: string[] = [];
  for (const cid of childIds) {
    const stateRes = await tcp.send({ cmd: 'GetState', id: cid });
    const state = (stateRes as { state?: string }).state ?? 'unknown';
    const key = `${queueName}:${cid}`;
    if (state === 'completed' || state === 'failed') {
      if (state === 'completed') {
        const resR = await tcp.send({ cmd: 'GetResult', id: cid });
        processed[key] = (resR as { result?: unknown }).result ?? null;
      } else {
        processed[key] = null;
      }
    } else {
      unprocessed.push(key);
    }
  }
  return { processed, unprocessed };
}

export interface SimpleJobContext {
  queueName: string;
  /** Execution mode — determines whether to use embedded manager or TCP */
  embedded?: boolean;
  /** TCP connection pool, required when !embedded */
  tcp?: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
  /** Requested options to reflect on the returned job (priority/delay/opts) — #88 */
  meta?: JobReflectionMeta;
}

/**
 * Create a simple Job with all BullMQ v5 methods wired.
 * Used by Queue.getJob / getJobs in both embedded and TCP modes.
 */
export function createSimpleJob<T>(
  id: string,
  name: string,
  data: T,
  timestamp: number,
  ctx: SimpleJobContext
): Job<T> {
  const { queueName, embedded, tcp, meta } = ctx;
  const r = reflectFields(id, queueName, meta);

  return {
    id,
    name,
    data,
    queueName,
    attemptsMade: 0,
    timestamp,
    progress: 0,
    delay: r.delay,
    processedOn: undefined,
    finishedOn: undefined,
    stacktrace: r.stacktrace,
    failedReason: r.failedReason,
    stalledCounter: 0,
    priority: r.priority,
    parent: r.parent,
    parentKey: r.parentKey,
    opts: r.opts,
    token: undefined,
    processedBy: undefined,
    deduplicationId: r.deduplicationId,
    repeatJobKey: r.repeatJobKey,
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

    // State check methods
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
      const res = await tcp.send({ cmd: 'ExtendLock', id, token, duration });
      return res.ok === true ? duration : 0;
    },
    clearLogs: async (keepLogs) => {
      if (embedded) {
        getSharedManager().clearLogs(jobId(id), keepLogs);
        return;
      }
      if (tcp) await tcp.send({ cmd: 'ClearLogs', id, keepLogs });
    },

    getDependencies: () => computeDepsSimple(id, queueName, embedded, tcp),
    getDependenciesCount: async () => {
      const deps = await computeDepsSimple(id, queueName, embedded, tcp);
      return {
        processed: Object.keys(deps.processed).length,
        unprocessed: deps.unprocessed.length,
      };
    },

    toJSON: () => ({
      id,
      name,
      data,
      opts: r.opts,
      progress: 0,
      delay: r.delay,
      timestamp,
      attemptsMade: 0,
      stacktrace: r.stacktrace,
      failedReason: r.failedReason,
      queueQualifiedName: `bull:${queueName}`,
      parentKey: r.parentKey,
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(data),
      opts: JSON.stringify(r.opts),
      progress: '0',
      delay: String(r.delay),
      timestamp: String(timestamp),
      attemptsMade: '0',
      stacktrace: r.stacktrace ? JSON.stringify(r.stacktrace) : null,
      failedReason: r.failedReason,
      parentKey: r.parentKey,
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
      if (embedded) {
        const mgr = getSharedManager();
        const state = await mgr.getJobState(jobId(id));
        if (state === 'active') return await mgr.moveActiveToWait(jobId(id));
        if (state === 'delayed') return await mgr.promote(jobId(id));
        if (state === 'failed') {
          const job = await mgr.getJob(jobId(id));
          if (!job) return false;
          return mgr.retryDlq(job.queue, jobId(id)) > 0;
        }
        if (state === 'waiting' || state === 'prioritized') return true;
        return false;
      }
      if (!tcp) return false;
      const res = await tcp.send({ cmd: 'MoveToWait', id });
      return res.ok === true;
    },
    moveToDelayed: async (ts) => {
      const delay = Math.max(0, ts - Date.now());
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
      if (embedded) {
        return await getSharedManager().getFailedChildrenValues(jobId(id));
      }
      if (!tcp) return {};
      const res = await tcp.send({ cmd: 'GetFailedChildrenValues', id });
      return (res.values as Record<string, string> | undefined) ?? {};
    },
    getIgnoredChildrenFailures: async () => {
      if (embedded) {
        return await getSharedManager().getIgnoredChildrenFailures(jobId(id));
      }
      if (!tcp) return {};
      const res = await tcp.send({ cmd: 'GetIgnoredChildrenFailures', id });
      return (res.values as Record<string, string> | undefined) ?? {};
    },
    removeChildDependency: async () => {
      if (embedded) {
        return await getSharedManager().removeChildDependency(jobId(id));
      }
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

/** Shared dependency computation for both embedded and TCP modes */
async function computeDepsSimple(
  id: string,
  queueName: string,
  embedded: boolean | undefined,
  tcp: TcpConnectionPool | null | undefined
): Promise<{ processed: Record<string, unknown>; unprocessed: string[] }> {
  const processed: Record<string, unknown> = {};
  const unprocessed: string[] = [];

  let childIds: string[] = [];
  if (embedded) {
    const job = await getSharedManager().getJob(jobId(id));
    childIds = (job?.childrenIds ?? []).map(String);
  } else if (tcp) {
    const jobRes = await tcp.send({ cmd: 'GetJob', id });
    const parent = (jobRes as { job?: { childrenIds?: string[] } }).job;
    childIds = (parent?.childrenIds ?? []).map(String);
  }

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
