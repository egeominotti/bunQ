/** FIFO asynchronous mutex with direct ownership handoff. */

import { LockTimeoutError } from './lockError';
import { DEFAULT_LOCK_TIMEOUT_MS } from './lockTimeout';
import type { LockGuard } from './types/lock';

interface AsyncWaiter {
  resolve: (guard: LockGuard) => void;
  reject: (error: LockTimeoutError) => void;
  timer?: ReturnType<typeof setTimeout>;
  settled: boolean;
}

export class AsyncLock {
  private locked = false;
  private readonly queue: AsyncWaiter[] = [];

  acquire(timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS): Promise<LockGuard> {
    if (!this.locked && this.queue.length === 0) {
      this.locked = true;
      return Promise.resolve(this.createGuard());
    }
    if (timeoutMs <= 0) return Promise.reject(new LockTimeoutError());

    return new Promise<LockGuard>((resolve, reject) => {
      const waiter: AsyncWaiter = {
        resolve,
        reject,
        settled: false,
      };
      waiter.timer = setTimeout(() => this.timeout(waiter), timeoutMs);
      this.queue.push(waiter);
      this.drain();
    });
  }

  isLocked(): boolean {
    return this.locked;
  }

  /** Queue size includes lazily pruned timed-out entries. */
  getQueueLength(): number {
    return this.queue.length;
  }

  private timeout(waiter: AsyncWaiter): void {
    if (waiter.settled) return;
    waiter.settled = true;
    waiter.reject(new LockTimeoutError());
    this.drain();
  }

  private drain(): void {
    if (this.locked) return;
    let waiter = this.queue.shift();
    while (waiter?.settled) waiter = this.queue.shift();
    if (!waiter) return;

    waiter.settled = true;
    if (waiter.timer) clearTimeout(waiter.timer);
    this.locked = true;
    waiter.resolve(this.createGuard());
  }

  private createGuard(): LockGuard {
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.locked = false;
        this.drain();
      },
    };
  }
}
