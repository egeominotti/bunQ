import {
  assertPositiveSafeInteger,
  type GroupPullOptions,
  type GroupRateLimitOverride,
} from '../types/group';

interface RateWindow {
  max: number;
  duration: number;
  startedAt: number;
  count: number;
}

export interface GroupLimitStatus {
  eligible: boolean;
  retryAt: number | null;
}

function key(queue: string, groupId: string): string {
  return `${queue}\0${groupId}`;
}

/** Stored group overrides and broker-authoritative runtime rate windows. */
export class GroupLimiterManager {
  private readonly rateOverrides = new Map<string, GroupRateLimitOverride>();
  private readonly concurrencyOverrides = new Map<string, number>();
  private readonly windows = new Map<string, RateWindow>();

  status(
    queue: string,
    groupId: string,
    defaults: GroupPullOptions | undefined,
    active: number,
    now = Date.now()
  ): GroupLimitStatus {
    const groupKey = key(queue, groupId);
    if (defaults?.concurrency !== undefined) {
      const concurrency = this.concurrencyOverrides.get(groupKey) ?? defaults.concurrency;
      if (active >= concurrency) return { eligible: false, retryAt: null };
    }

    if (!defaults?.limit) return { eligible: true, retryAt: null };
    const limit = this.rateOverrides.get(groupKey) ?? defaults.limit;
    const window = this.currentWindow(groupKey, limit.max, limit.duration, now);
    if (window && window.count >= limit.max) {
      return { eligible: false, retryAt: window.startedAt + window.duration };
    }
    return { eligible: true, retryAt: null };
  }

  acquire(
    queue: string,
    groupId: string,
    defaults: GroupPullOptions | undefined,
    active: number,
    now = Date.now()
  ): boolean {
    const status = this.status(queue, groupId, defaults, active, now);
    if (!status.eligible) return false;
    if (!defaults?.limit) return true;

    const groupKey = key(queue, groupId);
    const limit = this.rateOverrides.get(groupKey) ?? defaults.limit;
    let window = this.currentWindow(groupKey, limit.max, limit.duration, now);
    if (!window) {
      window = { max: limit.max, duration: limit.duration, startedAt: now, count: 0 };
      this.windows.set(groupKey, window);
    }
    window.count++;
    return true;
  }

  setRateLimit(queue: string, groupId: string, max: number, duration: number): void {
    assertPositiveSafeInteger(max, 'max');
    assertPositiveSafeInteger(duration, 'duration');
    const groupKey = key(queue, groupId);
    this.rateOverrides.set(groupKey, { max, duration });
    this.windows.delete(groupKey);
  }

  getRateLimit(queue: string, groupId: string): GroupRateLimitOverride | null {
    return this.rateOverrides.get(key(queue, groupId)) ?? null;
  }

  removeRateLimit(queue: string, groupId: string): number {
    const groupKey = key(queue, groupId);
    this.windows.delete(groupKey);
    return this.rateOverrides.delete(groupKey) ? 1 : 0;
  }

  getRateLimitTtl(queue: string, groupId: string, maxJobs?: number, now = Date.now()): number {
    const groupKey = key(queue, groupId);
    const window = this.windows.get(groupKey);
    if (!window) return -2;
    if (now >= window.startedAt + window.duration) {
      this.windows.delete(groupKey);
      return -2;
    }
    if (maxJobs !== undefined && window.count < maxJobs) return 0;
    return window.startedAt + window.duration - now;
  }

  setConcurrency(queue: string, groupId: string, concurrency: number): void {
    assertPositiveSafeInteger(concurrency, 'concurrency');
    this.concurrencyOverrides.set(key(queue, groupId), concurrency);
  }

  getConcurrency(queue: string, groupId: string): number | null {
    return this.concurrencyOverrides.get(key(queue, groupId)) ?? null;
  }

  removeConcurrency(queue: string, groupId: string): number {
    return this.concurrencyOverrides.delete(key(queue, groupId)) ? 1 : 0;
  }

  clearQueue(queue: string): void {
    const prefix = `${queue}\0`;
    for (const map of [this.rateOverrides, this.concurrencyOverrides, this.windows]) {
      for (const groupKey of map.keys()) {
        if (groupKey.startsWith(prefix)) map.delete(groupKey);
      }
    }
  }

  pruneExpiredWindows(now = Date.now()): void {
    for (const [groupKey, window] of this.windows) {
      if (now >= window.startedAt + window.duration) this.windows.delete(groupKey);
    }
  }

  private currentWindow(
    groupKey: string,
    max: number,
    duration: number,
    now: number
  ): RateWindow | null {
    const window = this.windows.get(groupKey);
    if (!window) return null;
    if (
      now >= window.startedAt + window.duration ||
      window.max !== max ||
      window.duration !== duration
    ) {
      this.windows.delete(groupKey);
      return null;
    }
    return window;
  }
}
