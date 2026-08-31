/**
 * Flow Push Operations
 * Handles job push logic for FlowProducer
 */

import { getSharedManager } from './manager';
import type { TcpConnectionPool } from './tcpPool';
import { jobId } from '../domain/types/job';
import type { JobOptions } from './types';
import { normalizeGroupId } from './groupId';

function groupId(opts: JobOptions): string | undefined {
  return opts.group ? normalizeGroupId(opts.group.id) : undefined;
}

export interface PushContext {
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

/** Parse removeOnComplete/removeOnFail options */
function parseRemoveOptions(opts: JobOptions): {
  removeOnComplete: boolean;
  removeOnFail: boolean;
} {
  return {
    removeOnComplete: typeof opts.removeOnComplete === 'boolean' ? opts.removeOnComplete : false,
    removeOnFail: typeof opts.removeOnFail === 'boolean' ? opts.removeOnFail : false,
  };
}

/** Normalize repeat startDate/endDate to epoch ms for the embedded manager. */
function normalizeRepeat(repeat: JobOptions['repeat']) {
  if (!repeat) return undefined;
  const toMs = (d: Date | string | number | undefined): number | undefined =>
    d instanceof Date ? d.getTime() : typeof d === 'string' ? new Date(d).getTime() : d;
  return {
    every: repeat.every,
    limit: repeat.limit,
    pattern: repeat.pattern,
    count: repeat.count,
    startDate: toMs(repeat.startDate),
    endDate: toMs(repeat.endDate),
    tz: repeat.tz,
    immediately: repeat.immediately,
    prevMillis: repeat.prevMillis,
    offset: repeat.offset,
    jobId: repeat.jobId,
  };
}

function dedupConfig(opts: JobOptions) {
  return opts.deduplication
    ? {
        ttl: opts.deduplication.ttl,
        extend: opts.deduplication.extend,
        replace: opts.deduplication.replace,
      }
    : undefined;
}

/**
 * Full option set for the embedded manager.push, mirroring Queue.add (#90).
 * Flow nodes must forward the same options as a direct add, not a subset.
 */
function managerOptions(opts: JobOptions) {
  return {
    priority: opts.priority,
    delay: opts.delay,
    maxAttempts: opts.attempts,
    backoff: opts.backoff,
    timeout: opts.timeout,
    customId: opts.jobId ?? opts.deduplication?.id,
    uniqueKey: opts.deduplication?.id,
    dedup: dedupConfig(opts),
    lifo: opts.lifo,
    stallTimeout: opts.stallTimeout,
    durable: opts.durable,
    stackTraceLimit: opts.stackTraceLimit,
    keepLogs: opts.keepLogs,
    sizeLimit: opts.sizeLimit,
    timestamp: opts.timestamp,
    repeat: normalizeRepeat(opts.repeat),
    debounceId: opts.debounce?.id,
    debounceTtl: opts.debounce?.ttl,
    failParentOnFailure: opts.failParentOnFailure,
    removeDependencyOnFailure: opts.removeDependencyOnFailure,
    ignoreDependencyOnFailure: opts.ignoreDependencyOnFailure,
    continueParentOnFailure: opts.continueParentOnFailure,
    groupId: groupId(opts),
  };
}

/** Full option set for the TCP PUSH command, mirroring buildPushPayload in add.ts (#90). */
function tcpOptions(opts: JobOptions) {
  return {
    priority: opts.priority,
    delay: opts.delay,
    maxAttempts: opts.attempts,
    backoff: opts.backoff,
    timeout: opts.timeout,
    jobId: opts.jobId,
    uniqueKey: opts.deduplication?.id,
    dedup: dedupConfig(opts),
    lifo: opts.lifo,
    stallTimeout: opts.stallTimeout,
    durable: opts.durable,
    stackTraceLimit: opts.stackTraceLimit,
    keepLogs: opts.keepLogs,
    sizeLimit: opts.sizeLimit,
    timestamp: opts.timestamp,
    repeat: opts.repeat,
    debounceId: opts.debounce?.id,
    debounceTtl: opts.debounce?.ttl,
    failParentOnFailure: opts.failParentOnFailure,
    removeDependencyOnFailure: opts.removeDependencyOnFailure,
    ignoreDependencyOnFailure: opts.ignoreDependencyOnFailure,
    continueParentOnFailure: opts.continueParentOnFailure,
    groupId: groupId(opts),
  };
}

/** Push a job via embedded manager or TCP */
export async function pushJob(
  ctx: PushContext,
  queueName: string,
  data: unknown,
  opts: JobOptions = {},
  dependsOn?: string[]
): Promise<string> {
  const { removeOnComplete, removeOnFail } = parseRemoveOptions(opts);

  if (ctx.embedded) {
    const manager = getSharedManager();
    const job = await manager.push(queueName, {
      name: 'default',
      data,
      ...managerOptions(opts),
      removeOnComplete,
      removeOnFail,
      dependsOn: dependsOn?.map((id) => jobId(id)),
    });
    return String(job.id);
  }

  if (!ctx.tcp) throw new Error('TCP connection not initialized');
  const response = await ctx.tcp.send({
    cmd: 'PUSH',
    queue: queueName,
    name: 'default',
    data,
    ...tcpOptions(opts),
    removeOnComplete,
    removeOnFail,
    dependsOn,
  });

  if (!response.ok) {
    throw new Error((response.error as string | undefined) ?? 'Failed to add job');
  }
  return response.id as string;
}

export interface PushWithParentOpts {
  queueName: string;
  name?: string;
  data: unknown;
  opts: JobOptions;
  parentRef: { id: string; queue: string } | null;
  childIds: string[];
}

/** Push a job with parent/children tracking */
export async function pushJobWithParent(
  ctx: PushContext,
  params: PushWithParentOpts
): Promise<string> {
  const { queueName, data, opts, parentRef, childIds, name = 'default' } = params;
  const { removeOnComplete, removeOnFail } = parseRemoveOptions(opts);
  if (ctx.embedded) {
    const manager = getSharedManager();
    const childJobIds = childIds.map((id) => jobId(id));
    const job = await manager.push(queueName, {
      name,
      data,
      ...managerOptions(opts),
      removeOnComplete,
      removeOnFail,
      parentId: parentRef ? jobId(parentRef.id) : undefined,
      dependsOn: childJobIds.length > 0 ? childJobIds : undefined,
      childrenIds: childJobIds.length > 0 ? childJobIds : undefined,
    });

    if (childIds.length > 0) {
      for (const childIdStr of childIds) {
        await manager.updateJobParent(jobId(childIdStr), job.id);
      }
    }

    return String(job.id);
  }

  if (!ctx.tcp) throw new Error('TCP connection not initialized');
  const response = await ctx.tcp.send({
    cmd: 'PUSH',
    queue: queueName,
    name,
    data,
    ...tcpOptions(opts),
    removeOnComplete,
    removeOnFail,
    parentId: parentRef?.id,
    childrenIds: childIds.length > 0 ? childIds : undefined,
    dependsOn: childIds.length > 0 ? childIds : undefined,
  });

  if (!response.ok) {
    throw new Error((response.error as string | undefined) ?? 'Failed to add job');
  }

  const parentJobId = response.id as string;

  // Update children with real parent ID (like embedded mode does)
  if (childIds.length > 0) {
    for (const childId of childIds) {
      const update = await ctx.tcp.send({ cmd: 'UpdateParent', childId, parentId: parentJobId });
      if (update.ok !== true) {
        throw new Error(
          typeof update.error === 'string' ? update.error : `Failed to link child ${childId}`
        );
      }
    }
  }

  return parentJobId;
}

/** Cleanup jobs that were created before a failure occurred */
export async function cleanupJobs(ctx: PushContext, jobIds: string[]): Promise<void> {
  if (jobIds.length === 0) return;

  if (ctx.embedded) {
    const manager = getSharedManager();
    const cleanupPromises = jobIds.map(async (id) => {
      try {
        await manager.cancel(jobId(id));
      } catch {
        // Ignore errors during cleanup
      }
    });
    await Promise.all(cleanupPromises);
  } else if (ctx.tcp) {
    const cleanupPromises = jobIds.map(async (id) => {
      try {
        await ctx.tcp?.send({ cmd: 'Cancel', id });
      } catch {
        // Ignore errors during cleanup
      }
    });
    await Promise.all(cleanupPromises);
  }
}
