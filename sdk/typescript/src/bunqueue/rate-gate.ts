/**
 * Bunqueue Simple Mode — client-side rate limiter for the worker.
 *
 * The official client passes `rateLimit`/`limiter` to the embedded Worker,
 * which throttles job starts to `max` per `duration` window, optionally per
 * group (`groupKey` names a field of job.data). This SDK reproduces the same
 * semantics as a gate awaited before each job runs: a job whose window is
 * full waits (holding its concurrency slot) until the window frees.
 */

import type { RateLimiterOptions } from './types.js';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export class RateGate {
  private readonly max: number;
  private readonly duration: number;
  private readonly groupKey: string | undefined;
  private readonly windows = new Map<string, number[]>();

  constructor(options: RateLimiterOptions) {
    this.max = options.max;
    this.duration = options.duration;
    this.groupKey = options.groupKey;
  }

  /** Resolve the rate-limit group for a job's data. */
  groupFor(data: unknown): string {
    if (!this.groupKey) return '';
    if (typeof data === 'object' && data !== null) {
      const value = (data as Record<string, unknown>)[this.groupKey];
      if (value !== undefined && value !== null) return String(value);
    }
    return '';
  }

  /** Wait until the group's window has room, then record the start. */
  async acquire(group: string): Promise<void> {
    for (;;) {
      const now = Date.now();
      const window = this.windows.get(group) ?? [];
      const fresh = window.filter((t) => now - t < this.duration);
      if (fresh.length < this.max) {
        fresh.push(now);
        this.windows.set(group, fresh);
        return;
      }
      this.windows.set(group, fresh);
      const oldest = fresh[0];
      await sleep(Math.max(oldest + this.duration - now, 10));
    }
  }

  /**
   * Drop groups whose window is fully expired. Called on each acquire cycle
   * boundary by the owner; without it a high-cardinality groupKey (e.g. one
   * group per user id) grows the map forever.
   */
  prune(): void {
    const now = Date.now();
    for (const [group, window] of this.windows) {
      if (window.length === 0 || now - window[window.length - 1] >= this.duration) {
        this.windows.delete(group);
      }
    }
  }
}
