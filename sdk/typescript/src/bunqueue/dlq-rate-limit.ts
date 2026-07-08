/**
 * Bunqueue Simple Mode — DLQ and rate limit helpers.
 * Port of src/client/bunqueue/dlqRateLimit.ts. Delegates to the TCP Queue;
 * the DLQ filter and stats are computed client-side from the fetched entries
 * (the embedded original reads them in-process).
 */

import type { Queue } from '../queue.js';
import type { BunqueueDlqConfig } from './types.js';

type Raw = Record<string, unknown>;

/** Filter for DLQ queries (mirrors the official DlqFilter). */
export interface DlqFilter {
  reason?: string;
  jobName?: string;
  since?: number;
}

/** Aggregate DLQ statistics (mirrors the official DlqStats). */
export interface DlqStats {
  total: number;
  byReason: Record<string, number>;
}

function entryReason(entry: Raw): string {
  return String(entry.reason ?? (entry.entry as Raw | undefined)?.reason ?? 'unknown');
}

function entryName(entry: Raw): string | undefined {
  const data = (entry.data ?? (entry.entry as Raw | undefined)?.data) as Raw | undefined;
  return data?.name === undefined ? undefined : String(data.name);
}

export class DlqRateLimitManager<T = unknown> {
  constructor(private readonly queue: Queue<T>) {}

  // ------------------------------------------------------------------- dlq

  async setDlqConfig(config: BunqueueDlqConfig): Promise<void> {
    await this.queue.setDlqConfig(config as Raw);
  }

  getDlqConfig(): Promise<Raw> {
    return this.queue.getDlqConfig();
  }

  async getDlq(filter?: DlqFilter): Promise<Raw[]> {
    const entries = await this.queue.getDlq();
    if (!filter) return entries;
    return entries.filter((entry) => {
      if (filter.reason && entryReason(entry) !== filter.reason) return false;
      if (filter.jobName && entryName(entry) !== filter.jobName) return false;
      if (filter.since !== undefined) {
        const at = Number(entry.enteredAt ?? entry.failedAt ?? 0);
        if (at < filter.since) return false;
      }
      return true;
    });
  }

  async getDlqStats(): Promise<DlqStats> {
    const entries = await this.queue.getDlq();
    const byReason: Record<string, number> = {};
    for (const entry of entries) {
      const reason = entryReason(entry);
      byReason[reason] = (byReason[reason] ?? 0) + 1;
    }
    return { total: entries.length, byReason };
  }

  retryDlq(id?: string): Promise<number> {
    return this.queue.retryDlq(id);
  }

  purgeDlq(): Promise<number> {
    return this.queue.purgeDlq();
  }

  // ----------------------------------------------------------- rate limiting

  async setGlobalRateLimit(max: number, duration?: number): Promise<void> {
    await this.queue.setGlobalRateLimit(max, duration);
  }

  async removeGlobalRateLimit(): Promise<void> {
    await this.queue.removeGlobalRateLimit();
  }
}
