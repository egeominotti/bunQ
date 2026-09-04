import { describe, expect, test } from 'bun:test';
import { SlotSignal } from '../src/worker-slot-signal.js';

describe('worker slot signal', () => {
  test('wakes a saturated poll loop as soon as a job settles', async () => {
    const signal = new SlotSignal();
    let settled = false;
    const waiting = signal.wait(10_000).then(() => {
      settled = true;
    });

    await Promise.resolve();
    expect(settled).toBe(false);
    signal.notify();
    await waiting;
    expect(settled).toBe(true);
  });
});
