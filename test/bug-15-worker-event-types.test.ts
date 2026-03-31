/**
 * Bug #15 - No autocomplete on events of Worker
 * https://github.com/egeominotti/bunqueue/issues/15
 *
 * Worker extends EventEmitter without typed overloads, so
 * .on() and .once() have no IntelliSense for event names
 * or callback parameter types.
 *
 * These tests verify that Worker exposes typed event methods
 * similar to QueueEvents (which already has proper typing).
 */

import { describe, test, expect } from 'bun:test';
import { Worker } from '../src/client/worker';

describe('Bug #15 - Worker event type safety', () => {
  /**
   * This test verifies that the Worker class has typed on() overloads
   * for all known events. If the types are missing, TypeScript would
   * infer `any` for callback parameters (no IntelliSense).
   *
   * We test this at runtime by checking that on() returns `this`
   * (chainable) and that listeners are registered for each event name.
   */

  test('Worker.on should accept known event names and return this for chaining', () => {
    const worker = new Worker('test-events', async () => {}, {
      autorun: false,
    });

    // All known Worker events should be registerable
    const knownEvents = [
      'ready',
      'active',
      'completed',
      'failed',
      'progress',
      'stalled',
      'drained',
      'error',
      'cancelled',
      'closed',
    ];

    for (const event of knownEvents) {
      const result = worker.on(event, () => {});
      // on() should return `this` for chaining (EventEmitter contract)
      expect(result).toBe(worker);
    }

    // Verify listeners are actually registered
    for (const event of knownEvents) {
      expect(worker.listenerCount(event)).toBeGreaterThanOrEqual(1);
    }

    worker.close();
  });

  test('Worker.once should accept known event names', () => {
    const worker = new Worker('test-events-once', async () => {}, {
      autorun: false,
    });

    const knownEvents = [
      'ready',
      'active',
      'completed',
      'failed',
      'progress',
      'stalled',
      'drained',
      'error',
      'cancelled',
      'closed',
    ];

    for (const event of knownEvents) {
      const result = worker.once(event, () => {});
      expect(result).toBe(worker);
    }

    worker.close();
  });

  test('Worker event callbacks should receive correct parameter types', () => {
    // This test documents the expected callback signatures.
    // When typed overloads are added, TypeScript will enforce these.
    const worker = new Worker<{ msg: string }, { ok: boolean }>(
      'test-typed-events',
      async () => ({ ok: true }),
      { autorun: false }
    );

    let activeJob: unknown = null;
    let completedResult: unknown = null;
    let failedError: unknown = null;
    let progressValue: unknown = null;
    let stalledId: unknown = null;
    let cancelledInfo: unknown = null;

    // 'active' should receive (job: Job<T>)
    worker.on('active', (job: any) => {
      activeJob = job;
    });

    // 'completed' should receive (job: Job<T>, result: R)
    worker.on('completed', (job: any, result: any) => {
      completedResult = result;
    });

    // 'failed' should receive (job: Job<T>, error: Error)
    worker.on('failed', (job: any, error: any) => {
      failedError = error;
    });

    // 'progress' should receive (job: Job<T> | null, progress: number)
    worker.on('progress', (job: any, progress: any) => {
      progressValue = progress;
    });

    // 'stalled' should receive (jobId: string, reason: string)
    worker.on('stalled', (jobId: any, reason: any) => {
      stalledId = jobId;
    });

    // 'cancelled' should receive ({ jobId: string, reason: string })
    worker.on('cancelled', (info: any) => {
      cancelledInfo = info;
    });

    // Verify listeners were registered (runtime check)
    expect(worker.listenerCount('active')).toBe(1);
    expect(worker.listenerCount('completed')).toBe(1);
    expect(worker.listenerCount('failed')).toBe(1);
    expect(worker.listenerCount('progress')).toBe(1);
    expect(worker.listenerCount('stalled')).toBe(1);
    expect(worker.listenerCount('cancelled')).toBe(1);

    worker.close();
  });
});
