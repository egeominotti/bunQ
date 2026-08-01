/**
 * DLQ Operations
 * Dead Letter Queue management for embedded mode
 */

import { getSharedManager } from '../manager';
import { jobId } from '../../domain/types/job';
import type { DlqConfig, DlqEntry, DlqStats, DlqFilter, StallConfig } from '../types';
import { toDlqEntry } from '../types';
import type { PublicJobMethodContext } from '../jobConversionTypes';
import { getShard, getDlqContext, toDomainFilter, toDomainDlqConfig } from './helpers';
import * as dlqOps from '../../application/dlqManager';

/** Set DLQ configuration (embedded only) */
export function setDlqConfig(queue: string, config: Partial<DlqConfig>): void {
  getSharedManager().setDlqConfig(queue, toDomainDlqConfig(config));
}

/** Get DLQ configuration (embedded only) */
export function getDlqConfigEmbedded(queue: string): DlqConfig {
  const manager = getSharedManager();
  const ctx = getDlqContext(manager);
  return dlqOps.getDlqConfig(queue, ctx);
}

/** Get DLQ entries (embedded only) */
export function getDlqEntries<T>(
  queue: string,
  methods: PublicJobMethodContext,
  filter?: DlqFilter
): DlqEntry<T>[] {
  const manager = getSharedManager();
  const ctx = getDlqContext(manager);
  const entries = dlqOps.getDlqEntries(queue, ctx, toDomainFilter(filter));
  return entries.map((entry) => toDlqEntry<T>(entry, methods));
}

/** Get DLQ statistics (embedded only) */
export function getDlqStatsEmbedded(queue: string): DlqStats {
  const manager = getSharedManager();
  const ctx = getDlqContext(manager);
  const stats = dlqOps.getDlqStats(queue, ctx);
  return {
    total: stats.total,
    byReason: stats.byReason,
    pendingRetry: stats.pendingRetry,
    expired: stats.expired,
    oldestEntry: stats.oldestEntry,
    newestEntry: stats.newestEntry,
  };
}

/** Retry DLQ jobs by filter (embedded only) */
export function retryDlqByFilterEmbedded(queue: string, filter: DlqFilter): number {
  const manager = getSharedManager();
  const ctx = getDlqContext(manager);
  const domainFilter = toDomainFilter(filter);
  if (!domainFilter) return 0;
  return dlqOps.retryDlqByFilter(queue, ctx, domainFilter);
}

/** Retry DLQ jobs (embedded) */
export function retryDlqEmbedded(queue: string, id?: string): number {
  const manager = getSharedManager();
  return manager.retryDlq(queue, id ? jobId(id) : undefined);
}

/** Purge DLQ (embedded) */
export function purgeDlqEmbedded(queue: string): number {
  return getSharedManager().purgeDlq(queue);
}

/** Set stall config (embedded only) */
export function setStallConfigEmbedded(queue: string, config: Record<string, unknown>): void {
  const manager = getSharedManager();
  const shard = getShard(manager, queue);
  shard.setStallConfig(queue, config);
}

/** Get stall config (embedded only) */
export function getStallConfigEmbedded(queue: string): StallConfig {
  const manager = getSharedManager();
  const shard = getShard(manager, queue);
  return shard.getStallConfig(queue);
}
