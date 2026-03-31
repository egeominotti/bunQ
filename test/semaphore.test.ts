/**
 * Semaphore Tests
 * Semaphore concurrency primitive and withSemaphore helper
 */

import { describe, test, expect } from 'bun:test';
import { Semaphore, withSemaphore } from '../src/shared/semaphore';

describe('Semaphore', () => {
  describe('constructor', () => {
    test('should initialize with correct number of permits', () => {
      const sem = new Semaphore(3);
      expect(sem.available()).toBe(3);
      expect(sem.waiting()).toBe(0);
    });

    test('should initialize with 1 permit (mutex)', () => {
      const sem = new Semaphore(1);
      expect(sem.available()).toBe(1);
    });
  });

  describe('acquire and release', () => {
    test('should acquire and release a single permit', async () => {
      const sem = new Semaphore(1);

      await sem.acquire();
      expect(sem.available()).toBe(0);

      sem.release();
      expect(sem.available()).toBe(1);
    });

    test('should acquire multiple permits sequentially', async () => {
      const sem = new Semaphore(3);

      await sem.acquire();
      expect(sem.available()).toBe(2);

      await sem.acquire();
      expect(sem.available()).toBe(1);

      await sem.acquire();
      expect(sem.available()).toBe(0);

      sem.release();
      expect(sem.available()).toBe(1);

      sem.release();
      expect(sem.available()).toBe(2);

      sem.release();
      expect(sem.available()).toBe(3);
    });

    test('should resolve immediately when permits are available', async () => {
      const sem = new Semaphore(2);
      let acquired = false;

      // Should resolve synchronously (within the same microtask)
      await sem.acquire();
      acquired = true;

      expect(acquired).toBe(true);
      expect(sem.available()).toBe(1);
    });
  });

  describe('blocking behavior', () => {
    test('should block when no permits available', async () => {
      const sem = new Semaphore(1);
      await sem.acquire(); // Takes the only permit

      let secondAcquired = false;
      const acquirePromise = sem.acquire().then(() => {
        secondAcquired = true;
      });

      // Let microtasks run
      await Bun.sleep(10);
      expect(secondAcquired).toBe(false);
      expect(sem.waiting()).toBe(1);

      sem.release(); // Unblock the waiter
      await acquirePromise;

      expect(secondAcquired).toBe(true);
      expect(sem.waiting()).toBe(0);
    });

    test('should block multiple waiters when no permits', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();

      const order: number[] = [];

      const p1 = sem.acquire().then(() => { order.push(1); });
      const p2 = sem.acquire().then(() => { order.push(2); });
      const p3 = sem.acquire().then(() => { order.push(3); });

      expect(sem.waiting()).toBe(3);

      // Release one at a time to verify ordering
      sem.release();
      await p1;
      expect(order).toEqual([1]);

      sem.release();
      await p2;
      expect(order).toEqual([1, 2]);

      sem.release();
      await p3;
      expect(order).toEqual([1, 2, 3]);
    });
  });

  describe('FIFO ordering of waiters', () => {
    test('should unblock waiters in FIFO order', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();

      const order: string[] = [];

      const pA = sem.acquire().then(() => { order.push('A'); });
      const pB = sem.acquire().then(() => { order.push('B'); });
      const pC = sem.acquire().then(() => { order.push('C'); });

      // Release all three permits
      sem.release();
      await pA;
      sem.release();
      await pB;
      sem.release();
      await pC;

      expect(order).toEqual(['A', 'B', 'C']);
    });
  });

  describe('tryAcquire', () => {
    test('should return true and consume permit when available', () => {
      const sem = new Semaphore(2);

      expect(sem.tryAcquire()).toBe(true);
      expect(sem.available()).toBe(1);

      expect(sem.tryAcquire()).toBe(true);
      expect(sem.available()).toBe(0);
    });

    test('should return false when no permits available', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();

      expect(sem.tryAcquire()).toBe(false);
      expect(sem.available()).toBe(0);
    });

    test('should not add to wait queue on failure', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();

      sem.tryAcquire();
      sem.tryAcquire();
      sem.tryAcquire();

      expect(sem.waiting()).toBe(0);
    });

    test('should work after release', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();
      expect(sem.tryAcquire()).toBe(false);

      sem.release();
      expect(sem.tryAcquire()).toBe(true);
      expect(sem.available()).toBe(0);
    });
  });

  describe('release', () => {
    test('should give permit to next waiter instead of incrementing count', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();

      let waiterResolved = false;
      const waiterPromise = sem.acquire().then(() => {
        waiterResolved = true;
      });

      expect(sem.waiting()).toBe(1);

      // Release should give the permit to the waiter, not increment available
      sem.release();
      await waiterPromise;

      expect(waiterResolved).toBe(true);
      // The permit went to the waiter, so available should still be 0
      expect(sem.available()).toBe(0);
      expect(sem.waiting()).toBe(0);
    });

    test('should not exceed maxPermits on release', () => {
      const sem = new Semaphore(2);

      // Release without acquiring first
      sem.release();
      expect(sem.available()).toBe(2); // Should be capped at maxPermits=2

      sem.release();
      expect(sem.available()).toBe(2); // Still capped

      sem.release();
      sem.release();
      sem.release();
      expect(sem.available()).toBe(2); // Never exceeds max
    });

    test('should cap permits at maxPermits after acquire/release cycle', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();
      sem.release();
      sem.release(); // Extra release
      sem.release(); // Another extra release

      expect(sem.available()).toBe(1); // Capped at maxPermits=1
    });
  });

  describe('available', () => {
    test('should return correct count after various operations', async () => {
      const sem = new Semaphore(3);

      expect(sem.available()).toBe(3);

      await sem.acquire();
      expect(sem.available()).toBe(2);

      sem.tryAcquire();
      expect(sem.available()).toBe(1);

      sem.release();
      expect(sem.available()).toBe(2);

      sem.release();
      expect(sem.available()).toBe(3);
    });

    test('should return 0 when all permits are taken', async () => {
      const sem = new Semaphore(2);
      await sem.acquire();
      await sem.acquire();

      expect(sem.available()).toBe(0);
    });
  });

  describe('waiting', () => {
    test('should return 0 when no waiters', () => {
      const sem = new Semaphore(1);
      expect(sem.waiting()).toBe(0);
    });

    test('should track waiting count accurately', async () => {
      const sem = new Semaphore(1);
      await sem.acquire();

      const p1 = sem.acquire();
      expect(sem.waiting()).toBe(1);

      const p2 = sem.acquire();
      expect(sem.waiting()).toBe(2);

      const p3 = sem.acquire();
      expect(sem.waiting()).toBe(3);

      // Resolve waiters
      sem.release();
      await p1;
      expect(sem.waiting()).toBe(2);

      sem.release();
      await p2;
      expect(sem.waiting()).toBe(1);

      sem.release();
      await p3;
      expect(sem.waiting()).toBe(0);
    });
  });

  describe('concurrent access', () => {
    test('should limit concurrency to maxPermits', async () => {
      const sem = new Semaphore(3);
      let concurrentCount = 0;
      let maxConcurrent = 0;

      const task = async (id: number) => {
        await sem.acquire();
        concurrentCount++;
        maxConcurrent = Math.max(maxConcurrent, concurrentCount);
        // Simulate async work
        await Bun.sleep(20);
        concurrentCount--;
        sem.release();
      };

      // Launch 10 tasks in parallel
      const tasks = Array.from({ length: 10 }, (_, i) => task(i));
      await Promise.all(tasks);

      expect(maxConcurrent).toBeLessThanOrEqual(3);
      expect(maxConcurrent).toBeGreaterThan(1); // At least some concurrency
      expect(sem.available()).toBe(3);
      expect(sem.waiting()).toBe(0);
    });

    test('should work as a mutex with maxPermits=1', async () => {
      const sem = new Semaphore(1);
      let concurrentCount = 0;
      let maxConcurrent = 0;

      const task = async () => {
        await sem.acquire();
        concurrentCount++;
        maxConcurrent = Math.max(maxConcurrent, concurrentCount);
        await Bun.sleep(5);
        concurrentCount--;
        sem.release();
      };

      const tasks = Array.from({ length: 5 }, () => task());
      await Promise.all(tasks);

      expect(maxConcurrent).toBe(1);
      expect(sem.available()).toBe(1);
    });

    test('should handle rapid acquire/release cycles', async () => {
      const sem = new Semaphore(2);
      const results: number[] = [];

      const task = async (id: number) => {
        await sem.acquire();
        results.push(id);
        sem.release();
      };

      const tasks = Array.from({ length: 100 }, (_, i) => task(i));
      await Promise.all(tasks);

      expect(results.length).toBe(100);
      expect(sem.available()).toBe(2);
    });
  });

  describe('multiple permits (semaphore with maxPermits > 1)', () => {
    test('should allow multiple concurrent acquires up to max', async () => {
      const sem = new Semaphore(5);

      // Acquire all 5
      for (let i = 0; i < 5; i++) {
        await sem.acquire();
      }
      expect(sem.available()).toBe(0);

      // 6th should block
      let sixthAcquired = false;
      const p = sem.acquire().then(() => { sixthAcquired = true; });

      await Bun.sleep(10);
      expect(sixthAcquired).toBe(false);
      expect(sem.waiting()).toBe(1);

      sem.release();
      await p;
      expect(sixthAcquired).toBe(true);
    });

    test('should release multiple permits back', async () => {
      const sem = new Semaphore(4);

      await sem.acquire();
      await sem.acquire();
      await sem.acquire();
      await sem.acquire();
      expect(sem.available()).toBe(0);

      sem.release();
      sem.release();
      expect(sem.available()).toBe(2);

      sem.release();
      sem.release();
      expect(sem.available()).toBe(4);
    });
  });
});

describe('withSemaphore', () => {
  test('should acquire before running and release after', async () => {
    const sem = new Semaphore(1);

    const result = await withSemaphore(sem, async () => {
      expect(sem.available()).toBe(0);
      return 42;
    });

    expect(result).toBe(42);
    expect(sem.available()).toBe(1);
  });

  test('should return the value from fn', async () => {
    const sem = new Semaphore(1);

    const result = await withSemaphore(sem, async () => {
      return { key: 'value', count: 10 };
    });

    expect(result).toEqual({ key: 'value', count: 10 });
  });

  test('should release permit on success', async () => {
    const sem = new Semaphore(2);

    await withSemaphore(sem, async () => {
      expect(sem.available()).toBe(1);
    });

    expect(sem.available()).toBe(2);
  });

  test('should release permit when fn throws', async () => {
    const sem = new Semaphore(1);

    try {
      await withSemaphore(sem, async () => {
        throw new Error('task failed');
      });
    } catch (e: any) {
      expect(e.message).toBe('task failed');
    }

    // Permit should be released even after error
    expect(sem.available()).toBe(1);
    expect(sem.waiting()).toBe(0);
  });

  test('should propagate the error from fn', async () => {
    const sem = new Semaphore(1);

    expect(
      withSemaphore(sem, async () => {
        throw new Error('specific error');
      })
    ).rejects.toThrow('specific error');
  });

  test('should limit concurrency of multiple withSemaphore calls', async () => {
    const sem = new Semaphore(2);
    let concurrentCount = 0;
    let maxConcurrent = 0;

    const task = async () => {
      return withSemaphore(sem, async () => {
        concurrentCount++;
        maxConcurrent = Math.max(maxConcurrent, concurrentCount);
        await Bun.sleep(15);
        concurrentCount--;
      });
    };

    const tasks = Array.from({ length: 8 }, () => task());
    await Promise.all(tasks);

    expect(maxConcurrent).toBeLessThanOrEqual(2);
    expect(concurrentCount).toBe(0);
    expect(sem.available()).toBe(2);
  });

  test('should release permit even if fn throws synchronously within async', async () => {
    const sem = new Semaphore(1);

    try {
      await withSemaphore(sem, async () => {
        // Synchronous throw inside async function
        throw new TypeError('type error');
      });
    } catch (e: any) {
      expect(e).toBeInstanceOf(TypeError);
    }

    expect(sem.available()).toBe(1);
  });

  test('should handle nested withSemaphore calls on different semaphores', async () => {
    const outer = new Semaphore(1);
    const inner = new Semaphore(1);

    const result = await withSemaphore(outer, async () => {
      expect(outer.available()).toBe(0);

      return withSemaphore(inner, async () => {
        expect(inner.available()).toBe(0);
        return 'nested';
      });
    });

    expect(result).toBe('nested');
    expect(outer.available()).toBe(1);
    expect(inner.available()).toBe(1);
  });

  test('should unblock queued withSemaphore callers after completion', async () => {
    const sem = new Semaphore(1);
    const order: number[] = [];

    const p1 = withSemaphore(sem, async () => {
      order.push(1);
      await Bun.sleep(20);
    });

    const p2 = withSemaphore(sem, async () => {
      order.push(2);
    });

    const p3 = withSemaphore(sem, async () => {
      order.push(3);
    });

    await Promise.all([p1, p2, p3]);

    expect(order).toEqual([1, 2, 3]);
    expect(sem.available()).toBe(1);
  });
});
