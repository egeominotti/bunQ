/**
 * Queue Management Operations
 * remove, retry, clean, promote, updateProgress, logs
 */
import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import { jobId } from '../../../domain/types/job';

interface ManagementContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

// ============ Remove Operations ============

/** Remove a job (sync, fire-and-forget; use removeAsync to await the removal) */
export function remove(ctx: ManagementContext, id: string): void {
  // `void`: the cancellation is intentionally not awaited here (sync API).
  if (ctx.embedded) void getSharedManager().cancel(jobId(id));
  else void ctx.tcp!.send({ cmd: 'Cancel', id });
}

/** Remove a job (async) */
export async function removeAsync(ctx: ManagementContext, id: string): Promise<void> {
  if (ctx.embedded) {
    // Must await: cancel() does the removal inside an async write-lock, so without
    // the await the returned promise resolves before the job is actually removed
    // (and any cancel error is swallowed) — inconsistent with the TCP path below.
    await getSharedManager().cancel(jobId(id));
    return;
  }
  await ctx.tcp!.send({ cmd: 'Cancel', id });
}

// ============ Retry Operations ============

/** Retry a specific job — BullMQ contract: failed → waiting. */
export async function retryJob(ctx: ManagementContext, id: string): Promise<void> {
  if (ctx.embedded) {
    const mgr = getSharedManager();
    const state = await mgr.getJobState(jobId(id));
    if (state === 'failed') {
      const count = mgr.retryDlq(ctx.name, jobId(id));
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
  const res = await ctx.tcp!.send({ cmd: 'MoveToWait', id });
  if (res.ok !== true) {
    const err = typeof res.error === 'string' ? res.error : 'retry failed';
    throw new Error(err);
  }
}

/** Retry jobs matching criteria */
export async function retryJobs(
  ctx: ManagementContext,
  opts?: { state?: 'failed' | 'completed'; count?: number; timestamp?: number }
): Promise<void> {
  const state = opts?.state ?? 'failed';
  if (ctx.embedded) {
    const manager = getSharedManager();
    if (state === 'completed') {
      manager.retryCompleted(ctx.name, undefined, {
        limit: opts?.count,
        timestamp: opts?.timestamp,
      });
    } else if (opts?.timestamp !== undefined) {
      manager.retryDlqByFilter(ctx.name, {
        olderThan: opts.timestamp,
        limit: opts.count,
      });
    } else {
      manager.retryDlq(ctx.name, undefined, opts?.count);
    }
    return;
  }

  if (state === 'completed') {
    await ctx.tcp!.send({
      cmd: 'RetryCompleted',
      queue: ctx.name,
      count: opts?.count,
      timestamp: opts?.timestamp,
    });
  } else {
    await ctx.tcp!.send({
      cmd: 'RetryDlq',
      queue: ctx.name,
      count: opts?.count,
      filter:
        opts?.timestamp === undefined
          ? undefined
          : { olderThan: opts.timestamp, limit: opts.count },
    });
  }
}

// ============ Clean Operations ============

/** Clean old jobs (sync) */
export function clean(
  ctx: ManagementContext,
  grace: number,
  limit: number,
  type?: 'completed' | 'wait' | 'active' | 'paused' | 'delayed' | 'failed'
): string[] {
  if (!ctx.embedded) return [];
  return getSharedManager().clean(ctx.name, grace, type, limit);
}

/** Clean old jobs (async) */
export async function cleanAsync(
  ctx: ManagementContext,
  grace: number,
  limit: number,
  type?: 'completed' | 'wait' | 'active' | 'paused' | 'delayed' | 'failed'
): Promise<string[]> {
  if (ctx.embedded) return clean(ctx, grace, limit, type);

  const response = await ctx.tcp!.send({
    cmd: 'Clean',
    queue: ctx.name,
    grace,
    limit,
    // Handler reads `state`; sending `type` made the state filter a no-op.
    state: type,
  });

  if (!response.ok) return [];
  const ids = (response.ids ?? []) as string[];
  return ids;
}

// ============ Promote Operations ============

/** Promote delayed jobs to waiting */
export async function promoteJobs(
  ctx: ManagementContext,
  opts?: { count?: number }
): Promise<number> {
  if (ctx.embedded) {
    return getSharedManager().promoteJobs(ctx.name, opts?.count);
  }

  const response = await ctx.tcp!.send({
    cmd: 'PromoteJobs',
    queue: ctx.name,
    count: opts?.count,
  });

  if (!response.ok) return 0;
  // Handler returns `count`; reading `promoted` always yielded 0.
  return (response.count ?? 0) as number;
}

/** Promote a single job */
export async function promoteJob(ctx: ManagementContext, id: string): Promise<void> {
  if (ctx.embedded) {
    await getSharedManager().promote(jobId(id));
    return;
  }
  await ctx.tcp!.send({ cmd: 'Promote', id });
}

// ============ Progress Operations ============

/** Update job progress */
export async function updateJobProgress(
  ctx: ManagementContext,
  id: string,
  progress: number | object
): Promise<void> {
  const progressValue = typeof progress === 'number' ? progress : 0;
  const message = typeof progress === 'object' ? JSON.stringify(progress) : undefined;

  if (ctx.embedded) {
    await getSharedManager().updateProgress(jobId(id), progressValue, message);
    return;
  }

  await ctx.tcp!.send({ cmd: 'Progress', id, progress: progressValue, message });
}

// ============ Log Operations ============

/** Get job logs */
export async function getJobLogs(
  ctx: ManagementContext,
  id: string,
  start = 0,
  end = 100
): Promise<{ logs: string[]; count: number }> {
  if (ctx.embedded) {
    const logs = getSharedManager().getLogs(jobId(id));
    const logStrings = logs.slice(start, end).map((l) => `[${l.level}] ${l.message}`);
    return { logs: logStrings, count: logs.length };
  }

  const response = await ctx.tcp!.send({ cmd: 'GetLogs', id, start, end });
  if (!response.ok) return { logs: [], count: 0 };

  // Server returns { ok: true, data: { logs } }
  const data = (response as { data?: { logs?: Array<{ message: string; level: string }> } }).data;
  const logs = data?.logs ?? [];
  const logStrings = logs.map((l) => `[${l.level}] ${l.message}`);
  return { logs: logStrings, count: logs.length };
}

/** Add a log entry to a job */
export async function addJobLog(
  ctx: ManagementContext,
  id: string,
  logRow: string
): Promise<number> {
  if (ctx.embedded) {
    const success = getSharedManager().addLog(jobId(id), logRow);
    return success ? 1 : 0;
  }

  const response = await ctx.tcp!.send({ cmd: 'AddLog', id, message: logRow });
  return response.ok ? 1 : 0;
}

/** Clear job logs */
export async function clearJobLogs(
  ctx: ManagementContext,
  id: string,
  keepLogs?: number
): Promise<void> {
  if (ctx.embedded) {
    getSharedManager().clearLogs(jobId(id), keepLogs);
    return;
  }
  await ctx.tcp!.send({ cmd: 'ClearLogs', id, keepLogs });
}

// ============ Data Update Operations ============

/** Update job data */
export async function updateJobData(
  ctx: ManagementContext,
  id: string,
  data: unknown
): Promise<void> {
  if (ctx.embedded) {
    await getSharedManager().updateJobData(jobId(id), data);
    return;
  }
  await ctx.tcp!.send({ cmd: 'Update', id, data });
}

/** Change job delay */
export async function changeJobDelay(
  ctx: ManagementContext,
  id: string,
  delay: number
): Promise<void> {
  if (ctx.embedded) {
    await getSharedManager().changeDelay(jobId(id), delay);
    return;
  }
  await ctx.tcp!.send({ cmd: 'ChangeDelay', id, delay });
}

/** Change job priority */
export async function changeJobPriority(
  ctx: ManagementContext,
  id: string,
  opts: { priority: number; lifo?: boolean }
): Promise<void> {
  if (ctx.embedded) {
    await getSharedManager().changePriority(jobId(id), opts.priority, opts.lifo);
    return;
  }
  await ctx.tcp!.send({ cmd: 'ChangePriority', id, priority: opts.priority, lifo: opts.lifo });
}

/** Extend job lock */
export async function extendJobLock(
  ctx: ManagementContext,
  id: string,
  token: string,
  duration: number
): Promise<number> {
  if (ctx.embedded) {
    const success = await getSharedManager().extendLock(jobId(id), token, duration);
    return success ? duration : 0;
  }
  const response = await ctx.tcp!.send({ cmd: 'ExtendLock', id, token, duration });
  return response.ok ? duration : 0;
}
