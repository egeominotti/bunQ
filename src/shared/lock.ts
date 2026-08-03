/** Async lock facade and scoped execution helpers. */

import { AsyncLock } from './asyncLock';
import { RWLock } from './rwLock';
import type { LockGuard } from './types/lock';

export { AsyncLock } from './asyncLock';
export { LockTimeoutError } from './lockError';
export { RWLock } from './rwLock';
export type { LockGuard } from './types/lock';

/** Execute with an exclusive lock and always release it. */
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

/** Execute with a read lock and always release it. */
export async function withReadLock<T>(
  lock: RWLock,
  fn: () => T | Promise<T>,
  timeoutMs?: number
): Promise<T> {
  const guard: LockGuard = await lock.acquireRead(timeoutMs);
  try {
    return await fn();
  } finally {
    guard.release();
  }
}

/** Execute with a write lock and always release it. */
export async function withWriteLock<T>(
  lock: RWLock,
  fn: () => T | Promise<T>,
  timeoutMs?: number
): Promise<T> {
  const guard: LockGuard = await lock.acquireWrite(timeoutMs);
  try {
    return await fn();
  } finally {
    guard.release();
  }
}
