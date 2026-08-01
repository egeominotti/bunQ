import type { DlqConfig, DlqEntry, DlqFilter, DlqStats, StallConfig } from '../../types';
import * as dlqOps from '../dlq';
import * as rateLimitOps from '../rateLimit';
import * as stallOps from '../stall';
import { QueueControl } from './control';

/** Stall, DLQ, concurrency, and rate-limit configuration. */
export class QueueConfiguration<T> extends QueueControl<T> {
  setStallConfig(config: Partial<StallConfig>) {
    stallOps.setStallConfig(this.ctx, config);
  }

  setStallConfigAsync(config: Partial<StallConfig>) {
    return stallOps.setStallConfigAsync(this.ctx, config);
  }

  getStallConfig(): StallConfig {
    return stallOps.getStallConfig(this.ctx);
  }

  getStallConfigAsync(): Promise<StallConfig> {
    return stallOps.getStallConfigAsync(this.ctx);
  }

  setDlqConfig(config: Partial<DlqConfig>) {
    dlqOps.setDlqConfig(this.ctx, config);
  }

  setDlqConfigAsync(config: Partial<DlqConfig>) {
    return dlqOps.setDlqConfigAsync(this.ctx, config);
  }

  getDlqConfig(): DlqConfig {
    return dlqOps.getDlqConfig(this.ctx);
  }

  getDlqConfigAsync(): Promise<DlqConfig> {
    return dlqOps.getDlqConfigAsync(this.ctx);
  }

  getDlq(filter?: DlqFilter): DlqEntry<T>[] {
    return dlqOps.getDlq<T>(this.queryCtx, filter);
  }

  getDlqAsync(filter?: DlqFilter): Promise<DlqEntry<T>[]> {
    return dlqOps.getDlqAsync<T>(this.queryCtx, filter);
  }

  getDlqJobsAsync(count?: number) {
    return dlqOps.getDlqJobsAsync<T>(this.queryCtx, count);
  }

  getDlqStats(): DlqStats {
    return dlqOps.getDlqStats(this.ctx);
  }

  getDlqStatsAsync(): Promise<DlqStats> {
    return dlqOps.getDlqStatsAsync(this.ctx);
  }

  retryDlq(id?: string) {
    return dlqOps.retryDlq(this.ctx, id);
  }

  retryDlqAsync(id?: string) {
    return dlqOps.retryDlqAsync(this.ctx, id);
  }

  retryDlqByFilter(filter: DlqFilter) {
    return dlqOps.retryDlqByFilter(this.ctx, filter);
  }

  retryDlqByFilterAsync(filter: DlqFilter) {
    return dlqOps.retryDlqByFilterAsync(this.ctx, filter);
  }

  purgeDlq() {
    return dlqOps.purgeDlq(this.ctx);
  }

  purgeDlqAsync() {
    return dlqOps.purgeDlqAsync(this.ctx);
  }

  retryCompleted(id?: string) {
    return dlqOps.retryCompleted(this.ctx, id);
  }

  retryCompletedAsync(id?: string) {
    return dlqOps.retryCompletedAsync(this.ctx, id);
  }

  setGlobalConcurrency(concurrency: number) {
    rateLimitOps.setGlobalConcurrency(this.ctx, concurrency);
  }

  setGlobalConcurrencyAsync(concurrency: number) {
    return rateLimitOps.setGlobalConcurrencyAsync(this.ctx, concurrency);
  }

  removeGlobalConcurrency() {
    rateLimitOps.removeGlobalConcurrency(this.ctx);
  }

  removeGlobalConcurrencyAsync() {
    return rateLimitOps.removeGlobalConcurrencyAsync(this.ctx);
  }

  getGlobalConcurrency() {
    return rateLimitOps.getGlobalConcurrency(this.ctx);
  }

  setGlobalRateLimit(max: number, duration?: number) {
    rateLimitOps.setGlobalRateLimit(this.ctx, max, duration);
  }

  setGlobalRateLimitAsync(max: number, duration?: number) {
    return rateLimitOps.setGlobalRateLimitAsync(this.ctx, max, duration);
  }

  removeGlobalRateLimit() {
    rateLimitOps.removeGlobalRateLimit(this.ctx);
  }

  removeGlobalRateLimitAsync() {
    return rateLimitOps.removeGlobalRateLimitAsync(this.ctx);
  }

  getGlobalRateLimit() {
    return rateLimitOps.getGlobalRateLimit(this.ctx);
  }

  rateLimit(expireTimeMs: number) {
    return rateLimitOps.rateLimit(this.ctx, expireTimeMs);
  }

  getRateLimitTtl(maxJobs?: number) {
    return rateLimitOps.getRateLimitTtl(this.ctx, maxJobs);
  }

  isMaxed() {
    return rateLimitOps.isMaxed(this.ctx);
  }
}
