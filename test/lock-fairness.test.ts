import { describe, expect, test } from 'bun:test';
import { AsyncLock, LockTimeoutError, RWLock } from '../src/shared/lock';

describe('AsyncLock direct handoff', () => {
  test('skips a timed-out waiter without stranding the next waiter', async () => {
    const lock = new AsyncLock();
    const initial = await lock.acquire();
    const timedOut = lock.acquire(20);
    const next = lock.acquire(1000);

    await expect(timedOut).rejects.toThrow(LockTimeoutError);
    initial.release();
    const nextGuard = await next;

    expect(lock.isLocked()).toBe(true);
    nextGuard.release();
    expect(lock.isLocked()).toBe(false);
  });

  test('preserves FIFO order across 64 contending acquirers', async () => {
    const lock = new AsyncLock();
    const initial = await lock.acquire();
    const order: number[] = [];
    const contenders = Array.from({ length: 64 }, (_, index) =>
      lock.acquire(2000).then((guard) => {
        order.push(index);
        guard.release();
      })
    );

    initial.release();
    await Promise.all(contenders);
    expect(order).toEqual(Array.from({ length: 64 }, (_, index) => index));
  });
});

describe('RWLock direct handoff', () => {
  test('grants queued writers in FIFO order', async () => {
    const lock = new RWLock();
    const initial = await lock.acquireWrite();
    const order: number[] = [];
    const writers = Array.from({ length: 16 }, (_, index) =>
      lock.acquireWrite(2000).then((guard) => {
        order.push(index);
        guard.release();
      })
    );

    initial.release();
    await Promise.all(writers);
    expect(order).toEqual(Array.from({ length: 16 }, (_, index) => index));
    expect(lock.getState()).toEqual({ readers: 0, writer: false, writerWaiting: 0 });
  });

  test('reserves writer ownership before a late reader can acquire', async () => {
    const lock = new RWLock();
    const initialReader = await lock.acquireRead();
    let releaseWriter!: () => void;
    const writerReleased = new Promise<void>((resolve) => {
      releaseWriter = resolve;
    });
    const order: string[] = [];
    const writer = lock.acquireWrite(1000).then(async (guard) => {
      order.push('writer');
      await writerReleased;
      guard.release();
    });

    initialReader.release();
    expect(lock.getState().writer).toBe(true);
    const reader = lock.acquireRead(1000).then((guard) => {
      order.push('reader');
      guard.release();
    });
    await Promise.resolve();

    expect(order).toEqual(['writer']);
    releaseWriter();
    await Promise.all([writer, reader]);
    expect(order).toEqual(['writer', 'reader']);
  });

  test('skips a timed-out writer and grants the next writer', async () => {
    const lock = new RWLock();
    const reader = await lock.acquireRead();
    const timedOut = lock.acquireWrite(20);
    const next = lock.acquireWrite(1000);

    await expect(timedOut).rejects.toThrow('Write lock acquisition timed out');
    expect(lock.getState().writerWaiting).toBe(1);
    reader.release();
    const nextGuard = await next;

    expect(lock.getState()).toEqual({ readers: 0, writer: true, writerWaiting: 0 });
    nextGuard.release();
  });

  test('unblocks queued readers when the last waiting writer times out', async () => {
    const lock = new RWLock();
    const firstReader = await lock.acquireRead();
    const writer = lock.acquireWrite(20);
    const secondReader = lock.acquireRead(1000);

    await expect(writer).rejects.toThrow('Write lock acquisition timed out');
    const secondGuard = await secondReader;
    expect(lock.getState()).toEqual({ readers: 2, writer: false, writerWaiting: 0 });

    secondGuard.release();
    firstReader.release();
  });

  test('a timed-out reader cannot mutate state after the writer releases', async () => {
    const lock = new RWLock();
    const writer = await lock.acquireWrite();
    await expect(lock.acquireRead(20)).rejects.toThrow('Read lock acquisition timed out');

    expect(lock.getState()).toEqual({ readers: 0, writer: true, writerWaiting: 0 });
    writer.release();
    await Bun.sleep(30);
    expect(lock.getState()).toEqual({ readers: 0, writer: false, writerWaiting: 0 });
  });

  test('a granted writer remains owned after its former timeout deadline', async () => {
    const lock = new RWLock();
    const initial = await lock.acquireWrite();
    const next = lock.acquireWrite(50);
    initial.release();
    const nextGuard = await next;

    await Bun.sleep(70);
    expect(lock.getState()).toEqual({ readers: 0, writer: true, writerWaiting: 0 });
    nextGuard.release();
  });

  test('zero-timeout contention leaves no phantom waiters', async () => {
    const lock = new RWLock();
    const writer = await lock.acquireWrite();

    await expect(lock.acquireWrite(0)).rejects.toThrow('Write lock acquisition timed out');
    await expect(lock.acquireRead(0)).rejects.toThrow('Read lock acquisition timed out');
    expect(lock.getState()).toEqual({ readers: 0, writer: true, writerWaiting: 0 });

    writer.release();
    expect(lock.getState()).toEqual({ readers: 0, writer: false, writerWaiting: 0 });
  });
});
