/**
 * DlqShard - Dead Letter Queue operations for a shard
 */

import type { Job, JobId } from '../types/job';
import type { DlqEntry, DlqConfig, DlqFilter } from '../types/dlq';
import {
  DEFAULT_DLQ_CONFIG,
  FailureReason,
  createDlqEntry,
  isDlqEntryExpired,
  canAutoRetry,
} from '../types/dlq';
import type { StallConfig } from '../types/stall';
import { DEFAULT_STALL_CONFIG } from '../types/stall';

/** Stats callback for counter updates */
export interface DlqStatsCallback {
  incrementDlq(): void;
  decrementDlq(count?: number): void;
  onEvict?(entry: DlqEntry): void;
}

function isDefaultDlqConfig(config: DlqConfig): boolean {
  return (
    config.autoRetry === DEFAULT_DLQ_CONFIG.autoRetry &&
    config.autoRetryInterval === DEFAULT_DLQ_CONFIG.autoRetryInterval &&
    config.maxAutoRetries === DEFAULT_DLQ_CONFIG.maxAutoRetries &&
    config.maxAge === DEFAULT_DLQ_CONFIG.maxAge &&
    config.maxEntries === DEFAULT_DLQ_CONFIG.maxEntries
  );
}

function isDefaultStallConfig(config: StallConfig): boolean {
  return (
    config.enabled === DEFAULT_STALL_CONFIG.enabled &&
    config.stallInterval === DEFAULT_STALL_CONFIG.stallInterval &&
    config.maxStalls === DEFAULT_STALL_CONFIG.maxStalls &&
    config.gracePeriod === DEFAULT_STALL_CONFIG.gracePeriod
  );
}

/**
 * Manages Dead Letter Queue operations for a shard
 */
export class DlqShard {
  /** Dead letter queue by queue name */
  private readonly dlq = new Map<string, DlqEntry[]>();

  /** DLQ configuration per queue */
  private readonly dlqConfig = new Map<string, DlqConfig>();

  /** Stall configuration per queue */
  private readonly stallConfig = new Map<string, StallConfig>();

  /** Stats callback for counter updates */
  private readonly stats: DlqStatsCallback;

  constructor(stats: DlqStatsCallback) {
    this.stats = stats;
  }

  /** Get DLQ config for queue */
  getConfig(queue: string): DlqConfig {
    return this.dlqConfig.get(queue) ?? DEFAULT_DLQ_CONFIG;
  }

  /** Set DLQ config for queue */
  setConfig(queue: string, config: Partial<DlqConfig>): void {
    const current = this.getConfig(queue);
    const next: DlqConfig = { ...current };
    if (typeof config.autoRetry === 'boolean') next.autoRetry = config.autoRetry;
    if (Number.isFinite(config.autoRetryInterval)) {
      next.autoRetryInterval = Math.max(0, Math.floor(config.autoRetryInterval!));
    }
    if (Number.isFinite(config.maxAutoRetries)) {
      next.maxAutoRetries = Math.max(0, Math.floor(config.maxAutoRetries!));
    }
    if (config.maxAge === null) {
      next.maxAge = null;
    } else if (Number.isFinite(config.maxAge)) {
      next.maxAge = Math.max(0, Math.floor(config.maxAge!));
    }
    if (Number.isFinite(config.maxEntries)) {
      next.maxEntries = Math.max(1, Math.floor(config.maxEntries!));
    }
    this.dlqConfig.set(queue, next);

    const entries = this.dlq.get(queue);
    if (entries) this.evictUntil(entries, next.maxEntries, false);
  }

  /** Get stall config for queue */
  getStallConfig(queue: string): StallConfig {
    return this.stallConfig.get(queue) ?? DEFAULT_STALL_CONFIG;
  }

  /** Set stall config for queue */
  setStallConfig(queue: string, config: Partial<StallConfig>): void {
    const current = this.getStallConfig(queue);
    this.stallConfig.set(queue, { ...current, ...config });
  }

  /** Add job to DLQ with full metadata */
  add(
    job: Job,
    reason: FailureReason = FailureReason.Unknown,
    error: string | null = null
  ): DlqEntry {
    let entries = this.dlq.get(job.queue);
    if (!entries) {
      entries = [];
      this.dlq.set(job.queue, entries);
    }

    const config = this.getConfig(job.queue);
    const entry = createDlqEntry(job, reason, error, config);

    // Enforce max entries
    this.evictUntil(entries, config.maxEntries, true);

    entries.push(entry);
    this.stats.incrementDlq();
    return entry;
  }

  /** Restore an existing DlqEntry (for recovery from persistence) */
  restoreEntry(queue: string, entry: DlqEntry): void {
    let entries = this.dlq.get(queue);
    if (!entries) {
      entries = [];
      this.dlq.set(queue, entries);
    }

    const config = this.getConfig(queue);

    // Enforce max entries (mirror add(): evict oldest-first)
    this.evictUntil(entries, config.maxEntries, true);

    entries.push(entry);
    this.stats.incrementDlq();
  }

  /** Get DLQ entries (raw) */
  getEntries(queue: string): DlqEntry[] {
    return this.dlq.get(queue) ?? [];
  }

  /** Get DLQ jobs (for backward compatibility) */
  getJobs(queue: string, count?: number): Job[] {
    const entries = this.dlq.get(queue);
    if (!entries) return [];
    const slice = count ? entries.slice(0, count) : entries;
    return slice.map((e) => e.job);
  }

  /** Get DLQ entries with filter */
  getFiltered(queue: string, filter: DlqFilter): DlqEntry[] {
    const entries = this.dlq.get(queue);
    if (!entries) return [];

    const now = Date.now();
    let result = entries.filter((entry) => {
      if (filter.reason && entry.reason !== filter.reason) return false;
      if (filter.olderThan && entry.enteredAt > filter.olderThan) return false;
      if (filter.newerThan && entry.enteredAt < filter.newerThan) return false;
      if (filter.retriable && !canAutoRetry(entry, this.getConfig(queue), now)) return false;
      if (filter.expired && !isDlqEntryExpired(entry, now)) return false;
      return true;
    });

    if (filter.offset) {
      result = result.slice(filter.offset);
    }
    if (filter.limit) {
      result = result.slice(0, filter.limit);
    }

    return result;
  }

  /** Remove entry from DLQ by job ID */
  remove(queue: string, jobId: JobId): DlqEntry | null {
    const entries = this.dlq.get(queue);
    if (!entries) return null;
    const idx = entries.findIndex((e) => e.job.id === jobId);
    if (idx === -1) return null;
    this.stats.decrementDlq();
    return entries.splice(idx, 1)[0];
  }

  /** Remove every entry with the same job ID from one queue. */
  removeAll(queue: string, jobId: JobId): DlqEntry[] {
    const entries = this.dlq.get(queue);
    if (!entries) return [];
    const removed: DlqEntry[] = [];
    const remaining: DlqEntry[] = [];
    for (const entry of entries) {
      (entry.job.id === jobId ? removed : remaining).push(entry);
    }
    if (removed.length === 0) return removed;
    this.dlq.set(queue, remaining);
    this.stats.decrementDlq(removed.length);
    return removed;
  }

  /** Get entries ready for auto-retry */
  getAutoRetryEntries(queue: string, now: number = Date.now()): DlqEntry[] {
    const entries = this.dlq.get(queue);
    if (!entries) return [];
    const config = this.getConfig(queue);
    return entries.filter((entry) => canAutoRetry(entry, config, now));
  }

  /** Get expired entries for cleanup */
  getExpiredEntries(queue: string, now: number = Date.now()): DlqEntry[] {
    const entries = this.dlq.get(queue);
    if (!entries) return [];
    return entries.filter((entry) => isDlqEntryExpired(entry, now));
  }

  /** Remove expired entries */
  purgeExpired(queue: string, now: number = Date.now()): number {
    const entries = this.dlq.get(queue);
    if (!entries) return 0;

    const before = entries.length;
    const remaining = entries.filter((entry) => !isDlqEntryExpired(entry, now));

    if (remaining.length < before) {
      this.dlq.set(queue, remaining);
      const removed = before - remaining.length;
      this.stats.decrementDlq(removed);
      return removed;
    }
    return 0;
  }

  /** Clear DLQ for queue */
  clear(queue: string): number {
    const entries = this.dlq.get(queue);
    if (!entries) return 0;
    const count = entries.length;
    this.dlq.delete(queue);
    this.stats.decrementDlq(count);
    return count;
  }

  /** Get DLQ count for queue */
  getCount(queue: string): number {
    return this.dlq.get(queue)?.length ?? 0;
  }

  /** Get all queue names with DLQ entries */
  getQueueNames(): string[] {
    return Array.from(this.dlq.keys());
  }

  /** Get queue names with persisted DLQ or stall policy. */
  getConfiguredQueueNames(): string[] {
    const configured = new Set<string>();
    for (const [queue, config] of this.dlqConfig) {
      if (!isDefaultDlqConfig(config)) configured.add(queue);
    }
    for (const [queue, config] of this.stallConfig) {
      if (!isDefaultStallConfig(config)) configured.add(queue);
    }
    return Array.from(configured);
  }

  /** Delete queue data */
  deleteQueue(queue: string): number {
    const entries = this.dlq.get(queue);
    const count = entries?.length ?? 0;
    this.dlq.delete(queue);
    this.dlqConfig.delete(queue);
    this.stallConfig.delete(queue);
    return count;
  }

  private evictUntil(entries: DlqEntry[], maxEntries: number, reserveSlot: boolean): void {
    const target = reserveSlot ? maxEntries - 1 : maxEntries;
    while (entries.length > target) {
      const evicted = entries.shift();
      if (!evicted) break;
      this.stats.decrementDlq();
      this.stats.onEvict?.(evicted);
    }
  }
}
