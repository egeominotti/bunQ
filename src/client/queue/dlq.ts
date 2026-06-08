/**
 * DLQ Operations Wrapper
 * Wraps dlqOps for Queue class usage
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import type { DlqConfig, DlqEntry, DlqStats, DlqFilter, FailureReason } from '../types';
import { jobId } from '../../domain/types/job';
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
