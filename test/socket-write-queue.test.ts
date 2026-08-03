import { describe, expect, test } from 'bun:test';
import { SocketWriteQueue } from '../src/infrastructure/server/socketWriteQueue';

describe('SocketWriteQueue', () => {
  test('preserves byte order across short writes and consecutive drains', () => {
    const received: number[] = [];
    let capacity = 2;
    let writes = 0;
    const socket = {
      write(data: Uint8Array): number {
        writes++;
        const count = Math.min(capacity, data.length);
        received.push(...data.subarray(0, count));
        capacity -= count;
        return count;
      },
    };
    const queue = new SocketWriteQueue();

    expect(queue.write(socket, Uint8Array.from([1, 2, 3, 4]))).toBe(true);
    expect(queue.write(socket, Uint8Array.from([5, 6]))).toBe(true);
    expect(writes).toBe(1);
    expect(received).toEqual([1, 2]);

    expect(queue.flush(socket)).toBe(true);
    expect(received).toEqual([1, 2]);

    capacity = 1;
    expect(queue.flush(socket)).toBe(true);
    expect(received).toEqual([1, 2, 3]);

    capacity = 10;
    expect(queue.flush(socket)).toBe(true);
    expect(received).toEqual([1, 2, 3, 4, 5, 6]);
    expect(queue.hasPending).toBe(false);
    expect(queue.bytesQueued).toBe(0);
  });

  test('reports closed and throwing sockets during write and drain', () => {
    const closed = { write: () => -1 };
    const throwing = {
      write(): number {
        throw new Error('socket failed');
      },
    };

    expect(new SocketWriteQueue().write(closed, Uint8Array.of(1))).toBe(false);
    expect(new SocketWriteQueue().write(throwing, Uint8Array.of(1))).toBe(false);

    const closedOnDrain = new SocketWriteQueue();
    expect(closedOnDrain.write({ write: () => 0 }, Uint8Array.of(1))).toBe(true);
    expect(closedOnDrain.flush(closed)).toBe(false);

    const throwsOnDrain = new SocketWriteQueue();
    expect(throwsOnDrain.write({ write: () => 0 }, Uint8Array.of(1))).toBe(true);
    expect(throwsOnDrain.flush(throwing)).toBe(false);
  });

  test('exposes the configured byte bound before callers terminate the socket', () => {
    const queue = new SocketWriteQueue(3);
    const blocked = { write: () => 0 };

    expect(queue.write(blocked, Uint8Array.of(1, 2, 3, 4))).toBe(true);
    expect(queue.bytesQueued).toBe(4);
    expect(queue.isOverBudget).toBe(true);
    queue.clear();
    expect(queue.bytesQueued).toBe(0);
  });
});
