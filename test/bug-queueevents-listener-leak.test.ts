/**
 * Bug: QueueEvents.close() does not remove user-attached listeners
 *
 * When close() is called, the internal manager subscription is cleaned up,
 * but user-attached event listeners (via .on()) remain attached to the
 * EventEmitter. This prevents GC of the callback closures and causes
 * a memory leak when QueueEvents instances are repeatedly created/closed.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { QueueEvents, Queue } from '../src/client';
import { shutdownManager } from '../src/client';

describe('QueueEvents listener cleanup on close', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('close() should remove all user-attached listeners', () => {
    const events = new QueueEvents('leak-test');

    // Attach several listeners
    events.on('completed', () => {});
    events.on('failed', () => {});
    events.on('waiting', () => {});
    events.on('progress', () => {});

    expect(events.listenerCount('completed')).toBe(1);
    expect(events.listenerCount('failed')).toBe(1);
    expect(events.listenerCount('waiting')).toBe(1);
    expect(events.listenerCount('progress')).toBe(1);

    events.close();

    // After close, all listeners should be removed
    expect(events.listenerCount('completed')).toBe(0);
    expect(events.listenerCount('failed')).toBe(0);
    expect(events.listenerCount('waiting')).toBe(0);
    expect(events.listenerCount('progress')).toBe(0);
    expect(events.eventNames().length).toBe(0);
  });

  test('close() should remove once() listeners too', () => {
    const events = new QueueEvents('leak-test-once');

    events.once('completed', () => {});
    events.once('failed', () => {});

    expect(events.listenerCount('completed')).toBe(1);
    expect(events.listenerCount('failed')).toBe(1);

    events.close();

    expect(events.listenerCount('completed')).toBe(0);
    expect(events.listenerCount('failed')).toBe(0);
  });

  test('repeated create/close should not accumulate listeners', () => {
    for (let i = 0; i < 100; i++) {
      const events = new QueueEvents(`leak-cycle-${i}`);
      events.on('completed', () => {});
      events.on('failed', () => {});
      events.on('waiting', () => {});
      events.close();

      // Verify clean state after each close
      expect(events.listenerCount('completed')).toBe(0);
      expect(events.listenerCount('failed')).toBe(0);
      expect(events.listenerCount('waiting')).toBe(0);
    }
  });

  test('disconnect() should also remove all listeners', async () => {
    const events = new QueueEvents('leak-test-disconnect');

    events.on('completed', () => {});
    events.on('failed', () => {});

    await events.disconnect();

    expect(events.listenerCount('completed')).toBe(0);
    expect(events.listenerCount('failed')).toBe(0);
  });
});
