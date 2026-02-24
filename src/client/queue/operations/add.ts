/* eslint-disable @typescript-eslint/no-non-null-assertion */
/**
 * Queue Add Operations
 * add, addBulk methods
 */

import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import type {
  Job,
  JobOptions,
  QueueOptions,
  JobStateType,
  GetDependenciesOpts,
  JobDependencies,
  JobDependenciesCount,
} from '../../types';
import { toPublicJob } from '../../types';
import { jobId } from '../../../domain/types/job';
import { createJobProxy, createSimpleJob } from '../jobProxy';

// Extended options that exist internally but not in public JobOptions
interface ExtendedJobOptions extends JobOptions {
  ttl?: number;
  dependsOn?: string[];
  tags?: string[];
  groupId?: string;
}

interface AddContext {
  name: string;
  opts: QueueOptions;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
  updateJobData: (id: string, data: unknown) => Promise<void>;
  promoteJob: (id: string) => Promise<void>;
  changeJobDelay: (id: string, delay: number) => Promise<void>;
  changeJobPriority: (id: string, opts: { priority: number }) => Promise<void>;
  extendJobLock: (id: string, token: string, duration: number) => Promise<number>;
  clearJobLogs: (id: string, keepLogs?: number) => Promise<void>;
  getJobDependencies: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependencies>;
  getJobDependenciesCount: (
    id: string,
    opts?: GetDependenciesOpts
  ) => Promise<JobDependenciesCount>;
  moveJobToCompleted: (id: string, result: unknown, token?: string) => Promise<unknown>;
  moveJobToFailed: (id: string, error: Error, token?: string) => Promise<void>;
  moveJobToWait: (id: string, token?: string) => Promise<boolean>;
  moveJobToDelayed: (id: string, timestamp: number, token?: string) => Promise<void>;
  moveJobToWaitingChildren: (
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) => Promise<boolean>;
  waitJobUntilFinished: (id: string, queueEvents: unknown, ttl?: number) => Promise<unknown>;
}

/** Add a single job */
export async function add<T>(
  ctx: AddContext,
  jobName: string,
  data: T,
  opts: JobOptions = {}
): Promise<Job<T>> {
  const merged = { ...ctx.opts.defaultJobOptions, ...opts } as ExtendedJobOptions;

  // Add parent info to data if specified
  const jobData: Record<string, unknown> = { name: jobName, ...(data as object) };
  if (merged.parent) {
    jobData.__parentId = merged.parent.id;
    jobData.__parentQueue = merged.parent.queue;
  }

  if (ctx.embedded) {
    const manager = getSharedManager();
    const removeOnComplete =
      typeof merged.removeOnComplete === 'boolean' ? merged.removeOnComplete : false;
    const removeOnFail = typeof merged.removeOnFail === 'boolean' ? merged.removeOnFail : false;

    const repeat = merged.repeat
      ? {
          every: merged.repeat.every,
          limit: merged.repeat.limit,
          pattern: merged.repeat.pattern,
          count: merged.repeat.count,
          startDate: parseDate(merged.repeat.startDate),
          endDate: parseDate(merged.repeat.endDate),
          tz: merged.repeat.tz,
          immediately: merged.repeat.immediately,
          prevMillis: merged.repeat.prevMillis,
          offset: merged.repeat.offset,
          jobId: merged.repeat.jobId,
        }
      : undefined;

    const job = await manager.push(ctx.name, {
      data: jobData,
      priority: merged.priority,
      delay: merged.delay,
      maxAttempts: merged.attempts,
      backoff: merged.backoff,
      ttl: merged.ttl,
      timeout: merged.timeout,
      uniqueKey: merged.deduplication?.id,
      customId: merged.jobId ?? merged.deduplication?.id,
      dependsOn: merged.dependsOn?.map((id: string) => jobId(id)),
      tags: merged.tags,
      groupId: merged.groupId,
      dedup: merged.deduplication
        ? {
            ttl: merged.deduplication.ttl,
            extend: merged.deduplication.extend,
            replace: merged.deduplication.replace,
          }
        : undefined,
      lifo: merged.lifo,
      removeOnComplete,
      removeOnFail,
      stallTimeout: merged.stallTimeout,
      durable: merged.durable,
      repeat,
      parentId: merged.parent ? jobId(merged.parent.id) : undefined,
      // BullMQ v5 options
      stackTraceLimit: merged.stackTraceLimit,
      keepLogs: merged.keepLogs,
      sizeLimit: merged.sizeLimit,
      failParentOnFailure: merged.failParentOnFailure,
      removeDependencyOnFailure: merged.removeDependencyOnFailure,
      debounceId: merged.debounce?.id,
      debounceTtl: merged.debounce?.ttl,
    });

    return toPublicJob<T>({
      job,
      name: jobName,
      getState: (jid) => ctx.getJobState(jid),
      remove: (jid) => ctx.removeAsync(jid),
      retry: (jid) => ctx.retryJob(jid),
      getChildrenValues: (jid) => ctx.getChildrenValues(jid),
      updateData: (jid, d) => ctx.updateJobData(jid, d),
      promote: (jid) => ctx.promoteJob(jid),
      changeDelay: (jid, delay) => ctx.changeJobDelay(jid, delay),
      changePriority: (jid, o) => ctx.changeJobPriority(jid, o),
      extendLock: (jid, token, duration) => ctx.extendJobLock(jid, token, duration),
      clearLogs: (jid, keepLogs) => ctx.clearJobLogs(jid, keepLogs),
      getDependencies: (jid, o) => ctx.getJobDependencies(jid, o),
      getDependenciesCount: (jid, o) => ctx.getJobDependenciesCount(jid, o),
      moveToCompleted: (jid, result, token) => ctx.moveJobToCompleted(jid, result, token),
      moveToFailed: (jid, error, token) => ctx.moveJobToFailed(jid, error, token),
      moveToWait: (jid, token) => ctx.moveJobToWait(jid, token),
      moveToDelayed: (jid, timestamp, token) => ctx.moveJobToDelayed(jid, timestamp, token),
      moveToWaitingChildren: (jid, token, o) => ctx.moveJobToWaitingChildren(jid, token, o),
      waitUntilFinished: (jid, queueEvents, ttl) => ctx.waitJobUntilFinished(jid, queueEvents, ttl),
    });
  }

  // TCP mode
  const tcp = ctx.tcp!;
  const response = await tcp.send({
    cmd: 'PUSH',
    queue: ctx.name,
    data: jobData,
    priority: merged.priority,
    delay: merged.delay,
    maxAttempts: merged.attempts,
    backoff: merged.backoff,
    timeout: merged.timeout,
    jobId: merged.jobId,
    removeOnComplete: merged.removeOnComplete,
    removeOnFail: merged.removeOnFail,
    stallTimeout: merged.stallTimeout,
    durable: merged.durable,
    repeat: merged.repeat,
    parentId: merged.parent?.id,
  });

  if (!response.ok) {
    throw new Error((response.error as string | undefined) ?? 'Failed to add job');
  }

  const jobIdStr = response.id as string;
  return createJobProxy(jobIdStr, jobName, data, {
    queueName: ctx.name,
    tcp,
    getJobState: ctx.getJobState,
    removeAsync: ctx.removeAsync,
    retryJob: ctx.retryJob,
    getChildrenValues: ctx.getChildrenValues,
  });
}

/** Add multiple jobs in bulk */
export async function addBulk<T>(
  ctx: AddContext,
  jobs: Array<{ name: string; data: T; opts?: JobOptions }>
): Promise<Job<T>[]> {
  if (jobs.length === 0) return [];
  const now = Date.now();

  if (ctx.embedded) {
    const manager = getSharedManager();
    const inputs = jobs.map(({ name, data, opts }) => {
      const m = { ...ctx.opts.defaultJobOptions, ...opts } as ExtendedJobOptions;
      const removeOnComplete = typeof m.removeOnComplete === 'boolean' ? m.removeOnComplete : false;
      const removeOnFail = typeof m.removeOnFail === 'boolean' ? m.removeOnFail : false;
      return {
        data: { name, ...data },
        priority: m.priority,
        delay: m.delay,
        maxAttempts: m.attempts,
        backoff: m.backoff,
        timeout: m.timeout,
        customId: m.jobId,
        removeOnComplete,
        removeOnFail,
        repeat: m.repeat
          ? {
              every: m.repeat.every,
              limit: m.repeat.limit,
              pattern: m.repeat.pattern,
              count: m.repeat.count,
              startDate: parseDate(m.repeat.startDate),
              endDate: parseDate(m.repeat.endDate),
              tz: m.repeat.tz,
              immediately: m.repeat.immediately,
              prevMillis: m.repeat.prevMillis,
              offset: m.repeat.offset,
              jobId: m.repeat.jobId,
            }
          : undefined,
        durable: m.durable,
        // BullMQ v5 options
        lifo: m.lifo,
        stackTraceLimit: m.stackTraceLimit,
        keepLogs: m.keepLogs,
        sizeLimit: m.sizeLimit,
        failParentOnFailure: m.failParentOnFailure,
        removeDependencyOnFailure: m.removeDependencyOnFailure,
        debounceId: m.debounce?.id,
        debounceTtl: m.debounce?.ttl,
      };
    });

    const ids = await manager.pushBatch(ctx.name, inputs);
    return ids.map((id, i) =>
      createSimpleJob(String(id), jobs[i].name, jobs[i].data, now, {
        queueName: ctx.name,
        getJobState: ctx.getJobState,
        removeAsync: ctx.removeAsync,
        retryJob: ctx.retryJob,
        getChildrenValues: ctx.getChildrenValues,
      })
    );
  }

  // TCP mode
  const tcp = ctx.tcp!;
  const batchJobs = jobs.map(({ name, data, opts }) => {
    const m = { ...ctx.opts.defaultJobOptions, ...opts } as ExtendedJobOptions;
    return {
      data: { name, ...data },
      priority: m.priority,
      delay: m.delay,
      maxAttempts: m.attempts,
      backoff: m.backoff,
      timeout: m.timeout,
      customId: m.jobId,
      repeat: m.repeat,
      durable: m.durable,
    };
  });

  const response = await tcp.send({
    cmd: 'PUSHB',
    queue: ctx.name,
    jobs: batchJobs,
  });

  if (!response.ok) return [];

  const ids = (response.ids ?? []) as string[];
  return ids.map((id, i) =>
    createJobProxy(id, jobs[i].name, jobs[i].data, {
      queueName: ctx.name,
      tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
    })
  );
}

function parseDate(date: Date | string | number | undefined): number | undefined {
  if (date instanceof Date) return date.getTime();
  if (typeof date === 'string') return new Date(date).getTime();
  return date;
}
