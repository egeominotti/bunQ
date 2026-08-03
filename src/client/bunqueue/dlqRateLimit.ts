/**
 * Bunqueue — DLQ & Rate Limit helpers
 * Mixin-style functions that delegate to Queue methods.
 */

import type { Queue } from '../queue/queue';
import type { DlqConfig, DlqEntry, DlqStats, DlqFilter } from '../types';

export class DlqRateLimitManager<T = unknown> {
  constructor(private readonly queue: Queue<T>) {}

  // ── DLQ ──

  setDlqConfig(config: Partial<DlqConfig>): void {
    this.queue.setDlqConfig(config);
  }
  setDlqConfigAsync(config: Partial<DlqConfig>): Promise<void> {
    return this.queue.setDlqConfigAsync(config);
  }
  getDlqConfig(): DlqConfig {
    return this.queue.getDlqConfig();
  }
  getDlqConfigAsync(): Promise<DlqConfig> {
    return this.queue.getDlqConfigAsync();
  }
  getDlq(filter?: DlqFilter): DlqEntry<T>[] {
    return this.queue.getDlq(filter);
  }
  getDlqAsync(filter?: DlqFilter): Promise<DlqEntry<T>[]> {
    return this.queue.getDlqAsync(filter);
  }
  getDlqStats(): DlqStats {
    return this.queue.getDlqStats();
  }
  getDlqStatsAsync(): Promise<DlqStats> {
    return this.queue.getDlqStatsAsync();
  }
  retryDlq(id?: string) {
    return this.queue.retryDlq(id);
  }
  retryDlqAsync(id?: string): Promise<number> {
    return this.queue.retryDlqAsync(id);
  }
  purgeDlq() {
    return this.queue.purgeDlq();
  }
  purgeDlqAsync(): Promise<number> {
    return this.queue.purgeDlqAsync();
  }

  // ── Rate Limiting ──

  setGlobalRateLimit(max: number, duration?: number): void {
    this.queue.setGlobalRateLimit(max, duration);
  }
  setGlobalRateLimitAsync(max: number, duration?: number): Promise<void> {
    return this.queue.setGlobalRateLimitAsync(max, duration);
  }
  removeGlobalRateLimit(): void {
    this.queue.removeGlobalRateLimit();
  }
  removeGlobalRateLimitAsync(): Promise<void> {
    return this.queue.removeGlobalRateLimitAsync();
  }
}
