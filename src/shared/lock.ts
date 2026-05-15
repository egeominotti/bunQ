/**
 * Async Lock implementation
 * Provides read-write locks with timeout support
 * Optimized with O(1) queue operations using wrapped resolvers
 */

/** Default lock timeout in milliseconds */
const DEFAULT_LOCK_TIMEOUT_MS = parseInt(Bun.env.LOCK_TIMEOUT_MS ?? '5000', 10);

/** Lock acquisition result */
export interface LockGuard {
  release(): void;
}

/** Lock timeout error */
export class LockTimeoutError extends Error {
  constructor(message: string = 'Lock acquisition timed out') {
    super(message);
    this.name = 'LockTimeoutError';
  }
}

/** Wrapper for queue entries to enable O(1) cancellation */
interface QueueEntry {
  resolve: () => void;
  cancelled: boolean;
}

/**
 * Simple async mutex lock
 * FIFO ordering for fairness
 * O(1) timeout cancellation using marked entries instead of indexOf+splice
 */
export class AsyncLock {
  private locked = false;
  private readonly queue: QueueEntry[] = [];

  /**
   * Acquire the lock
   * @param timeoutMs - Maximum time to wait (default: DEFAULT_LOCK_TIMEOUT_MS)
   */
  async acquire(timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS): Promise<LockGuard> {
    const start = Date.now();

    while (this.locked) {
      const remaining = timeoutMs - (Date.now() - start);
      if (remaining <= 0) {
        throw new LockTimeoutError();
      }

      await new Promise<void>((resolve) => {
        const entry: QueueEntry = { resolve, cancelled: false };

        const timer = setTimeout(() => {
          // O(1) cancellation - just mark as cancelled
          entry.cancelled = true;
          resolve();
        }, remaining);

        entry.resolve = () => {
          clearTimeout(timer);
          resolve();
        };

        this.queue.push(entry);
      });
    }

    this.locked = true;

    let released = false;
    return {
      release: () => {
        // Idempotent: ignore double-release on the same guard.
        // Prevents stale releases from clobbering the next owner's `locked` flag
        // and breaking the mutual-exclusion invariant.
        if (released) return;
        released = true;
        this.locked = false;
        // Skip cancelled entries - O(k) where k = cancelled entries at head
        let next = this.queue.shift();
        while (next) {
          if (!next.cancelled) {
            next.resolve();
            break;
          }
          next = this.queue.shift();
        }
      },
    };
  }

  /** Check if lock is held */
  isLocked(): boolean {
    return this.locked;
  }

  /** Get queue length (includes cancelled entries) */
  getQueueLength(): number {
    return this.queue.length;
  }
}

/**
 * Read-Write Lock
 * Multiple readers OR single writer
 * Writers have priority to prevent starvation
 * O(1) timeout cancellation using marked entries
 */
export class RWLock {
  private readers = 0;
  private writer = false;
  private writerWaiting = 0;
  private readonly readerQueue: QueueEntry[] = [];
  private readonly writerQueue: QueueEntry[] = [];

  /**
   * Acquire read lock
   * Multiple readers can hold simultaneously
   */
  async acquireRead(timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS): Promise<LockGuard> {
    const start = Date.now();

    // Wait if writer is active or writers are waiting (writer priority)
    while (this.writer || this.writerWaiting > 0) {
      const remaining = timeoutMs - (Date.now() - start);
      if (remaining <= 0) {
        throw new LockTimeoutError('Read lock acquisition timed out');
      }

      await new Promise<void>((resolve) => {
        const entry: QueueEntry = { resolve, cancelled: false };

        const timer = setTimeout(() => {
          // O(1) cancellation - just mark as cancelled
          entry.cancelled = true;
          resolve();
        }, remaining);

        entry.resolve = () => {
          clearTimeout(timer);
          resolve();
        };

        this.readerQueue.push(entry);
      });
    }

    this.readers++;

    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.readers--;
        if (this.readers === 0 && this.writerWaiting > 0) {
          // Skip cancelled entries
          let next = this.writerQueue.shift();
          while (next) {
            if (!next.cancelled) {
              next.resolve();
              break;
            }
            next = this.writerQueue.shift();
          }
        }
      },
    };
  }

  /**
   * Acquire write lock
   * Exclusive access, no readers or other writers
   * Optimized: synchronous fast path when uncontested
   */
  async acquireWrite(timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS): Promise<LockGuard> {
    // Fast path: uncontested - acquire synchronously without Promise overhead
    if (!this.writer && this.readers === 0) {
      this.writer = true;
      return this.createWriteGuard();
    }

    // Slow path: contention - wait asynchronously
    const start = Date.now();
    this.writerWaiting++;

    try {
      while (this.writer || this.readers > 0) {
        const remaining = timeoutMs - (Date.now() - start);
        if (remaining <= 0) {
          throw new LockTimeoutError('Write lock acquisition timed out');
        }

        await new Promise<void>((resolve) => {
          const entry: QueueEntry = { resolve, cancelled: false };

          const timer = setTimeout(() => {
            // O(1) cancellation - just mark as cancelled
            entry.cancelled = true;
            resolve();
          }, remaining);

          entry.resolve = () => {
            clearTimeout(timer);
            resolve();
          };

          this.writerQueue.push(entry);
        });
      }

      this.writerWaiting--;
      this.writer = true;

      return this.createWriteGuard();
    } catch (e) {
      this.writerWaiting--;
      throw e;
    }
  }

  /** Create write lock guard - extracted to avoid code duplication */
  private createWriteGuard(): LockGuard {
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.writer = false;
        // Notify waiting writers first (priority)
        if (this.writerWaiting > 0) {
          // Skip cancelled entries
          let next = this.writerQueue.shift();
          while (next) {
            if (!next.cancelled) {
              next.resolve();
              return;
            }
            next = this.writerQueue.shift();
          }
        }
        // Then notify all waiting readers (skip cancelled)
        const readers = this.readerQueue.splice(0);
        for (const entry of readers) {
          if (!entry.cancelled) {
            entry.resolve();
          }
        }
      },
    };
  }

  /** Get current state */
  getState(): { readers: number; writer: boolean; writerWaiting: number } {
    return {
      readers: this.readers,
      writer: this.writer,
      writerWaiting: this.writerWaiting,
    };
  }
}

/**
 * Execute with lock, ensuring release on error
 */
export async function withLock<T>(
  lock: AsyncLock,
  fn: () => T | Promise<T>,
  timeoutMs?: number
): Promise<T> {
  const guard = await lock.acquire(timeoutMs);
  try {
    return await fn();
  } finally {
    guard.release();
  }
}

/**
 * Execute with read lock
 */
export async function withReadLock<T>(
  lock: RWLock,
  fn: () => T | Promise<T>,
  timeoutMs?: number
): Promise<T> {
  const guard = await lock.acquireRead(timeoutMs);
  try {
    return await fn();
  } finally {
    guard.release();
  }
}

/**
 * Execute with write lock
 */
export async function withWriteLock<T>(
  lock: RWLock,
  fn: () => T | Promise<T>,
  timeoutMs?: number
): Promise<T> {
  const guard = await lock.acquireWrite(timeoutMs);
  try {
    return await fn();
  } finally {
    guard.release();
  }
}
