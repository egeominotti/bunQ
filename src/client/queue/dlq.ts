/**
 * DLQ Operations Wrapper
 * Wraps dlqOps for Queue class usage
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import type { Job, DlqConfig, DlqEntry, DlqStats, DlqFilter } from '../types';
import type { Job as InternalJob } from '../../domain/types/job';
import type { DlqEntry as InternalDlqEntry } from '../../domain/types/dlq';
import { jobId } from '../../domain/types/job';
import { createSimpleJob } from './jobProxy';
import * as dlqOps from './dlqOps';
import { toDlqEntry } from '../jobConversion';
import { createDlqJobMethods, type DlqJobContext } from './dlqJobMethods';
import { resolvePublicJobPayload } from '../jobHelpers';
import { toPublicDlqStats } from './dlqStats';

interface DlqContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

/** Client-side cache for TCP mode */
const tcpDlqConfigCache = new Map<string, Partial<DlqConfig>>();

/** Set DLQ configuration */
export function setDlqConfig(ctx: DlqContext, config: Partial<DlqConfig>): void {
  if (ctx.embedded) {
    dlqOps.setDlqConfig(ctx.name, config);
  } else if (ctx.tcp) {
    const current = tcpDlqConfigCache.get(ctx.name) ?? {};
    tcpDlqConfigCache.set(ctx.name, { ...current, ...config });
    void ctx.tcp.send({ cmd: 'SetDlqConfig', queue: ctx.name, config });
  }
}

/** Set DLQ configuration and resolve once the server has applied it. */
export async function setDlqConfigAsync(
  ctx: DlqContext,
  config: Partial<DlqConfig>
): Promise<void> {
  if (ctx.embedded) {
    dlqOps.setDlqConfig(ctx.name, config);
    return;
  }
  if (!ctx.tcp) return;
  const current = tcpDlqConfigCache.get(ctx.name) ?? {};
  tcpDlqConfigCache.set(ctx.name, { ...current, ...config });
  await ctx.tcp.send({ cmd: 'SetDlqConfig', queue: ctx.name, config });
}

/** Get DLQ configuration */
export function getDlqConfig(ctx: DlqContext): DlqConfig {
  if (ctx.embedded) return dlqOps.getDlqConfigEmbedded(ctx.name);
  // Return cached config if available
  const cached = tcpDlqConfigCache.get(ctx.name);
  if (cached) return cached;
  return {};
}

/** Get DLQ configuration (async, works in TCP mode) */
export async function getDlqConfigAsync(ctx: DlqContext): Promise<DlqConfig> {
  if (ctx.embedded) return dlqOps.getDlqConfigEmbedded(ctx.name);
  if (!ctx.tcp) return {};
  const response = await ctx.tcp.send({ cmd: 'GetDlqConfig', queue: ctx.name });
  if (!response.ok) return {};
  return (response as { config: DlqConfig }).config;
}

/** Get DLQ entries */
export function getDlq<T>(ctx: DlqQueryContext, filter?: DlqFilter): DlqEntry<T>[] {
  if (!ctx.embedded) return [];
  const entries = getSharedManager().getDlqEntries(
    ctx.name,
    filter as unknown as import('../../domain/types/dlq').DlqFilter
  );
  const methods = createDlqJobMethods(ctx);
  return entries.map((entry) => toDlqEntry<T>(entry, methods));
}

type DlqQueryContext = DlqJobContext;

/** Read full DLQ metadata from the selected broker runtime. */
export async function getDlqAsync<T>(
  ctx: DlqQueryContext,
  filter?: DlqFilter
): Promise<DlqEntry<T>[]> {
  let entries: InternalDlqEntry[];
  if (ctx.embedded) {
    entries = getSharedManager().getDlqEntries(
      ctx.name,
      filter as unknown as import('../../domain/types/dlq').DlqFilter
    );
  } else if (ctx.tcp) {
    const response = await ctx.tcp.send({ cmd: 'Dlq', queue: ctx.name, filter });
    if (!response.ok) return [];
    entries = (response.entries ?? []) as InternalDlqEntry[];
  } else {
    return [];
  }
  const methods = createDlqJobMethods(ctx);
  return entries.map((entry) => toDlqEntry<T>(entry, methods));
}

/**
 * Get only the public jobs from the DLQ. Use getDlqAsync() when the full entry
 * metadata is needed; both views work over TCP through the same command.
 */
export async function getDlqJobsAsync<T>(ctx: DlqQueryContext, count?: number): Promise<Job<T>[]> {
  let raw: InternalJob[];
  if (ctx.embedded) {
    raw = getSharedManager().getDlq(ctx.name, count);
  } else if (ctx.tcp) {
    const response = await ctx.tcp.send({ cmd: 'Dlq', queue: ctx.name, count });
    if (!response.ok) return [];
    raw = (response.jobs ?? []) as InternalJob[];
  } else {
    return [];
  }

  return raw.map((job) => {
    const { name, data } = resolvePublicJobPayload(job);
    return createSimpleJob<T>(String(job.id), name, data as T, job.createdAt ?? Date.now(), {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
    });
  });
}

/** Get DLQ stats */
export function getDlqStats(ctx: DlqContext): DlqStats {
  if (!ctx.embedded) {
    return toPublicDlqStats(undefined);
  }
  return dlqOps.getDlqStatsEmbedded(ctx.name);
}

/** Read authoritative DLQ statistics from the selected broker runtime. */
export async function getDlqStatsAsync(ctx: DlqContext): Promise<DlqStats> {
  if (ctx.embedded) return dlqOps.getDlqStatsEmbedded(ctx.name);
  if (!ctx.tcp) return getDlqStats(ctx);
  const response = await ctx.tcp.send({ cmd: 'GetDlqStats', queue: ctx.name });
  if (!response.ok) return getDlqStats(ctx);
  return toPublicDlqStats((response.data as { stats: DlqStats }).stats);
}

/** Retry DLQ entries */
export function retryDlq(ctx: DlqContext, id?: string): number {
  if (ctx.embedded) return dlqOps.retryDlqEmbedded(ctx.name, id);
  if (ctx.tcp) void ctx.tcp.send({ cmd: 'RetryDlq', queue: ctx.name, jobId: id });
  return 0;
}

/**
 * Retry DLQ entries and resolve with the retried count once the server has
 * processed it. The fire-and-forget retryDlq() always returns 0 over TCP and
 * discards the server's count.
 */
export async function retryDlqAsync(ctx: DlqContext, id?: string): Promise<number> {
  if (ctx.embedded) return dlqOps.retryDlqEmbedded(ctx.name, id);
  if (!ctx.tcp) return 0;
  const response = await ctx.tcp.send({ cmd: 'RetryDlq', queue: ctx.name, jobId: id });
  if (!response.ok) return 0;
  return (response.count ?? 0) as number;
}

/** Retry DLQ entries by filter */
export function retryDlqByFilter(ctx: DlqContext, filter: DlqFilter): number {
  if (ctx.embedded) return dlqOps.retryDlqByFilterEmbedded(ctx.name, filter);
  if (ctx.tcp) {
    void ctx.tcp.send({ cmd: 'RetryDlq', queue: ctx.name, filter }).catch(() => undefined);
  }
  return 0;
}

/** Retry matching DLQ entries and return the applied count. */
export async function retryDlqByFilterAsync(ctx: DlqContext, filter: DlqFilter): Promise<number> {
  if (ctx.embedded) return dlqOps.retryDlqByFilterEmbedded(ctx.name, filter);
  if (!ctx.tcp) return 0;
  const response = await ctx.tcp.send({ cmd: 'RetryDlq', queue: ctx.name, filter });
  if (!response.ok) return 0;
  return (response.count ?? 0) as number;
}

/** Purge DLQ */
export function purgeDlq(ctx: DlqContext): number {
  if (ctx.embedded) return dlqOps.purgeDlqEmbedded(ctx.name);
  if (ctx.tcp) void ctx.tcp.send({ cmd: 'PurgeDlq', queue: ctx.name });
  return 0;
}

/**
 * Purge the DLQ and resolve with the purged count once the server has
 * processed it. The fire-and-forget purgeDlq() always returns 0 over TCP;
 * purge-then-assert-empty patterns need this variant.
 */
export async function purgeDlqAsync(ctx: DlqContext): Promise<number> {
  if (ctx.embedded) return dlqOps.purgeDlqEmbedded(ctx.name);
  if (!ctx.tcp) return 0;
  const response = await ctx.tcp.send({ cmd: 'PurgeDlq', queue: ctx.name });
  if (!response.ok) return 0;
  return (response.count ?? 0) as number;
}

/** Retry completed job */
export function retryCompleted(ctx: DlqContext, id?: string): number {
  if (ctx.embedded) {
    const jid = id ? jobId(id) : undefined;
    return getSharedManager().retryCompleted(ctx.name, jid);
  }
  if (ctx.tcp) void ctx.tcp.send({ cmd: 'RetryCompleted', queue: ctx.name, id });
  return 0;
}

/** Retry completed job (async) */
export async function retryCompletedAsync(ctx: DlqContext, id?: string): Promise<number> {
  if (ctx.embedded) return retryCompleted(ctx, id);
  if (!ctx.tcp) return 0;
  const response = await ctx.tcp.send({ cmd: 'RetryCompleted', queue: ctx.name, id });
  if (!response.ok) return 0;
  return (response.count ?? 0) as number;
}
