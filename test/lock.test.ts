/**
 * Lock Tests
 */

import { describe, test, expect } from 'bun:test';
import {
  AsyncLock,
  RWLock,
  withLock,
  withReadLock,
  withWriteLock,
  LockTimeoutError,
} from '../src/shared/lock';

describe('AsyncLock', () => {
  test('should acquire and release lock', async () => {
    const lock = new AsyncLock();

    expect(lock.isLocked()).toBe(false);

    const guard = await lock.acquire();
    expect(lock.isLocked()).toBe(true);

    guard.release();
    expect(lock.isLocked()).toBe(false);
  });

  test('should queue concurrent acquisitions', async () => {
    const lock = new AsyncLock();
    const order: number[] = [];

    const p1 = lock.acquire().then((g) => {
      order.push(1);
      return new Promise<void>((resolve) => {
        setTimeout(() => {
          g.release();
          resolve();
        }, 50);
      });
    });

    const p2 = lock.acquire().then((g) => {
      order.push(2);
      g.release();
    });

    await Promise.all([p1, p2]);

    expect(order).toEqual([1, 2]);
  });

  test('does not let a new acquirer barge ahead of an already queued acquirer', async () => {
    const lock = new AsyncLock();
    const order: string[] = [];
    const initialGuard = await lock.acquire();

    const queued = lock.acquire(1000).then((guard) => {
      order.push('queued');
      guard.release();
    });

    initialGuard.release();
    const newcomer = lock.acquire(1000).then((guard) => {
      order.push('new');
      guard.release();
    });

    await Promise.all([queued, newcomer]);
    expect(order).toEqual(['queued', 'new']);
  });

  test('should timeout on lock acquisition', async () => {
    const lock = new AsyncLock();

    const guard = await lock.acquire();

    await expect(lock.acquire(100)).rejects.toThrow(LockTimeoutError);

    guard.release();
  });

  test('withLock should auto-release on success', async () => {
    const lock = new AsyncLock();

    const result = await withLock(lock, () => {
      expect(lock.isLocked()).toBe(true);
      return 'success';
    });

    expect(result).toBe('success');
    expect(lock.isLocked()).toBe(false);
  });

  test('withLock should auto-release on error', async () => {
    const lock = new AsyncLock();

    await expect(
      withLock(lock, () => {
        throw new Error('test error');
      })
    ).rejects.toThrow('test error');

    expect(lock.isLocked()).toBe(false);
  });
});

describe('RWLock', () => {
  test('should allow multiple readers', async () => {
    const lock = new RWLock();

    const guard1 = await lock.acquireRead();
    const guard2 = await lock.acquireRead();

    const state = lock.getState();
    expect(state.readers).toBe(2);
    expect(state.writer).toBe(false);

    guard1.release();
    guard2.release();

    expect(lock.getState().readers).toBe(0);
  });

  test('should block readers while writing', async () => {
    const lock = new RWLock();
    const order: string[] = [];

    const writeGuard = await lock.acquireWrite();
    order.push('write-start');

    const readPromise = lock.acquireRead(1000).then((g) => {
      order.push('read');
      g.release();
    });

    await Bun.sleep(50);
    order.push('write-end');
    writeGuard.release();

    await readPromise;

    expect(order).toEqual(['write-start', 'write-end', 'read']);
  });

  test('should block writers while reading', async () => {
    const lock = new RWLock();
    const order: string[] = [];

    const readGuard = await lock.acquireRead();
    order.push('read-start');

    const writePromise = lock.acquireWrite(1000).then((g) => {
      order.push('write');
      g.release();
    });

    await Bun.sleep(50);
    order.push('read-end');
    readGuard.release();

    await writePromise;

    expect(order).toEqual(['read-start', 'read-end', 'write']);
  });

  test('should prioritize writers', async () => {
    const lock = new RWLock();
    const order: string[] = [];

    const readGuard = await lock.acquireRead();

    // Queue a writer
    const writePromise = lock.acquireWrite().then((g) => {
      order.push('writer');
      g.release();
    });

    // Queue a reader after writer
    const readPromise = lock.acquireRead().then((g) => {
      order.push('reader');
      g.release();
    });

    await Bun.sleep(10);
    readGuard.release();

    await Promise.all([writePromise, readPromise]);

    // Writer should go before the second reader
    expect(order[0]).toBe('writer');
  });

  test('does not let a new writer barge ahead of an already queued writer', async () => {
    const lock = new RWLock();
    const order: string[] = [];
    const initialGuard = await lock.acquireWrite();

    const queuedWriter = lock.acquireWrite(1000).then((guard) => {
      order.push('queued');
      guard.release();
    });

    initialGuard.release();
    const newWriter = lock.acquireWrite(1000).then((guard) => {
      order.push('new');
      guard.release();
    });

    await Promise.all([queuedWriter, newWriter]);
    expect(order).toEqual(['queued', 'new']);
  });

  test('withReadLock should auto-release', async () => {
    const lock = new RWLock();

    const result = await withReadLock(lock, () => {
      expect(lock.getState().readers).toBe(1);
      return 42;
    });

    expect(result).toBe(42);
    expect(lock.getState().readers).toBe(0);
  });

  test('withWriteLock should auto-release', async () => {
    const lock = new RWLock();

    const result = await withWriteLock(lock, () => {
      expect(lock.getState().writer).toBe(true);
      return 'done';
    });

    expect(result).toBe('done');
    expect(lock.getState().writer).toBe(false);
  });

  test('write lock timeout', async () => {
    const lock = new RWLock();

    const guard = await lock.acquireWrite();

    await expect(lock.acquireWrite(100)).rejects.toThrow(LockTimeoutError);

    guard.release();
  });

  test('read lock timeout', async () => {
    const lock = new RWLock();

    const guard = await lock.acquireWrite();

    await expect(lock.acquireRead(100)).rejects.toThrow(LockTimeoutError);

    guard.release();
  });
});
