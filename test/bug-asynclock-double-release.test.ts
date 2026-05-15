/**
 * Regression: AsyncLock guard.release() must be idempotent.
 *
 * Bug history: src/shared/lock.ts release() previously had no idempotency,
 * so a stale double-release could clobber `locked=false` and resolve another
 * waiter, breaking mutual exclusion (jobIndex/completedJobs/shards lock
 * hierarchy in QueueManager).
 *
 * Fix: release() now tracks a per-guard `released` flag and short-circuits on
 * subsequent calls. The mutex invariant holds even with buggy callers.
 */

import { describe, test, expect } from 'bun:test';
import { AsyncLock, RWLock } from '../src/shared/lock';

describe('AsyncLock idempotent release', () => {
  test('double-release on a stale guard does not steal ownership from the next waiter', async () => {
    const lock = new AsyncLock();
    const inside: string[] = [];

    const g0 = await lock.acquire();
    expect(lock.isLocked()).toBe(true);

    const p1 = lock.acquire().then((g) => {
      inside.push('w1-inside');
      return g;
    });
    await Bun.sleep(5);

    g0.release(); // legit transfer to w1
    const guard1 = await p1;
    expect(inside).toEqual(['w1-inside']);
    expect(lock.isLocked()).toBe(true);

    const p2 = lock.acquire().then((g) => {
      inside.push('w2-inside');
      return g;
    });
    await Bun.sleep(5);

    // Stale double-release. Must be a no-op.
    g0.release();

    // w2 must remain blocked because w1 still holds the lock.
    await Bun.sleep(20);
    expect(inside).toEqual(['w1-inside']);
    expect(lock.isLocked()).toBe(true);

    guard1.release();
    const guard2 = await p2;
    expect(inside).toEqual(['w1-inside', 'w2-inside']);
    guard2.release();
    expect(lock.isLocked()).toBe(false);
  });

  test('legitimate release-then-stale-release does not reopen lock for a third acquirer', async () => {
    const lock = new AsyncLock();

    const g0 = await lock.acquire();
    const stale = g0.release.bind(g0);

    g0.release(); // legit
    expect(lock.isLocked()).toBe(false);

    const g1 = await lock.acquire();
    expect(lock.isLocked()).toBe(true);

    // Stale release must not affect g1's ownership.
    stale();
    expect(lock.isLocked()).toBe(true);

    // Third acquirer must wait — proves mutex still holds.
    let unlocked = false;
    const p2 = lock.acquire(200).then((g) => {
      unlocked = true;
      g.release();
    });
    await Bun.sleep(20);
    expect(unlocked).toBe(false);

    g1.release();
    await p2;
    expect(unlocked).toBe(true);
  });
});

describe('RWLock idempotent release', () => {
  test('write guard double-release is a no-op', async () => {
    const lock = new RWLock();
    const g0 = await lock.acquireWrite();
    expect(lock.getState().writer).toBe(true);

    g0.release();
    expect(lock.getState().writer).toBe(false);

    const g1 = await lock.acquireWrite();
    expect(lock.getState().writer).toBe(true);

    // Stale double-release on g0 must NOT clear g1's writer flag.
    g0.release();
    expect(lock.getState().writer).toBe(true);

    g1.release();
    expect(lock.getState().writer).toBe(false);
  });

  test('read guard double-release does not double-decrement counter', async () => {
    const lock = new RWLock();
    const g1 = await lock.acquireRead();
    const g2 = await lock.acquireRead();
    expect(lock.getState().readers).toBe(2);

    g1.release();
    expect(lock.getState().readers).toBe(1);

    // Stale double-release must not skew the counter to 0 (which would let a
    // writer in while g2 still believes it holds a read lock).
    g1.release();
    expect(lock.getState().readers).toBe(1);

    g2.release();
    expect(lock.getState().readers).toBe(0);
  });
});
