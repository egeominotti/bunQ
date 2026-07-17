/**
 * DLQ Operations Wrapper
 * Wraps dlqOps for Queue class usage
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import type { Job, DlqConfig, DlqEntry, DlqStats, DlqFilter, FailureReason } from '../types';
import type { Job as InternalJob } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import { createSimpleJob, type SimpleJobContext } from './jobProxy';
import * as dlqOps from './dlqOps';

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
export function getDlq<T>(ctx: DlqContext, filter?: DlqFilter): DlqEntry<T>[] {
  if (!ctx.embedded) return [];
  return dlqOps.getDlqEntries<T>(ctx.name, filter);
}

type DlqQueryContext = DlqContext &
  Pick<SimpleJobContext, 'getJobState' | 'removeAsync' | 'retryJob' | 'getChildrenValues'>;

/**
 * Get the dead jobs in the DLQ, working over TCP too (wire command `Dlq`).
 * Returns public Job objects; the wire does not carry DlqEntry metadata
 * (reason, enteredAt, attempts) — that stays embedded-only via getDlq().
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
    const name = (job.data as { name?: string } | null)?.name ?? 'unknown';
    return createSimpleJob<T>(String(job.id), name, job.data as T, job.createdAt ?? Date.now(), {
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
    return {
      total: 0,
      byReason: {} as Record<FailureReason, number>,
      pendingRetry: 0,
      expired: 0,
      oldestEntry: null,
      newestEntry: null,
    };
  }
  return dlqOps.getDlqStatsEmbedded(ctx.name);
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
  if (!ctx.embedded) return 0;
  return dlqOps.retryDlqByFilterEmbedded(ctx.name, filter);
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
