/** Writer-priority read/write lock with FIFO writer handoff. */

import { LockTimeoutError } from './lockError';
import { DEFAULT_LOCK_TIMEOUT_MS } from './lockTimeout';
import type { LockGuard } from './types/lock';

interface RWWaiter {
  resolve: (guard: LockGuard) => void;
  reject: (error: LockTimeoutError) => void;
  timer?: ReturnType<typeof setTimeout>;
  settled: boolean;
}

export class RWLock {
  private readers = 0;
  private writer = false;
  private writerWaiting = 0;
  private readonly readerQueue: RWWaiter[] = [];
  private readonly writerQueue: RWWaiter[] = [];

  acquireRead(timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS): Promise<LockGuard> {
    if (!this.writer && this.writerWaiting === 0) {
      this.readers++;
      return Promise.resolve(this.createReadGuard());
    }
    if (timeoutMs <= 0) {
      return Promise.reject(new LockTimeoutError('Read lock acquisition timed out'));
    }

    return new Promise<LockGuard>((resolve, reject) => {
      const waiter = this.createWaiter(resolve, reject, timeoutMs, () => {
        waiter.reject(new LockTimeoutError('Read lock acquisition timed out'));
        this.drain();
      });
      this.readerQueue.push(waiter);
    });
  }

  acquireWrite(timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS): Promise<LockGuard> {
    if (!this.writer && this.readers === 0 && this.writerWaiting === 0) {
      this.writer = true;
      return Promise.resolve(this.createWriteGuard());
    }
    if (timeoutMs <= 0) {
      return Promise.reject(new LockTimeoutError('Write lock acquisition timed out'));
    }

    return new Promise<LockGuard>((resolve, reject) => {
      const waiter = this.createWaiter(resolve, reject, timeoutMs, () => {
        this.writerWaiting--;
        waiter.reject(new LockTimeoutError('Write lock acquisition timed out'));
        this.drain();
      });
      this.writerWaiting++;
      this.writerQueue.push(waiter);
      this.drain();
    });
  }

  getState(): { readers: number; writer: boolean; writerWaiting: number } {
    return { readers: this.readers, writer: this.writer, writerWaiting: this.writerWaiting };
  }

  private createWaiter(
    resolve: (guard: LockGuard) => void,
    reject: (error: LockTimeoutError) => void,
    timeoutMs: number,
    onTimeout: () => void
  ): RWWaiter {
    const waiter: RWWaiter = {
      resolve,
      reject,
      settled: false,
    };
    waiter.timer = setTimeout(() => {
      if (waiter.settled) return;
      waiter.settled = true;
      onTimeout();
    }, timeoutMs);
    return waiter;
  }

  private drain(): void {
    if (this.writer) return;
    if (this.writerWaiting > 0) {
      if (this.readers > 0) return;
      const waiter = this.takeWriter();
      if (waiter) {
        waiter.settled = true;
        if (waiter.timer) clearTimeout(waiter.timer);
        this.writerWaiting--;
        this.writer = true;
        waiter.resolve(this.createWriteGuard());
        return;
      }
      this.writerWaiting = 0;
    }
    this.grantReaders();
  }

  private takeWriter(): RWWaiter | undefined {
    let waiter = this.writerQueue.shift();
    while (waiter?.settled) waiter = this.writerQueue.shift();
    return waiter;
  }

  private grantReaders(): void {
    const waiters = this.readerQueue.splice(0);
    for (const waiter of waiters) {
      if (waiter.settled) continue;
      waiter.settled = true;
      if (waiter.timer) clearTimeout(waiter.timer);
      this.readers++;
      waiter.resolve(this.createReadGuard());
    }
  }

  private createReadGuard(): LockGuard {
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.readers--;
        this.drain();
      },
    };
  }

  private createWriteGuard(): LockGuard {
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.writer = false;
        this.drain();
      },
    };
  }
}
