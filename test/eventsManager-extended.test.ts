/**
 * Extended Events Manager Tests
 * Tests for clear(), needsBroadcast(), mapEventToWebhook(), and webhook integration
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { EventsManager } from '../src/application/eventsManager';
import { WebhookManager } from '../src/application/webhookManager';
import { EventType } from '../src/domain/types/queue';

/** Helper to create an EventsManager with a real or mock WebhookManager */
function createEventsManager(webhookManager?: WebhookManager) {
  const wm = webhookManager ?? new WebhookManager();
  const em = new EventsManager(wm);
  return { em, wm };
}

/** Helper to build a minimal broadcast event */
function makeEvent(
  eventType: EventType,
  overrides?: { queue?: string; jobId?: string; data?: unknown; error?: string }
) {
  return {
    eventType,
    queue: overrides?.queue ?? 'test-queue',
    jobId: overrides?.jobId ?? 'job-1',
    timestamp: Date.now(),
    data: overrides?.data,
    error: overrides?.error,
  };
}

describe('EventsManager - extended', () => {
  let em: EventsManager;
  let wm: WebhookManager;

  beforeEach(() => {
    const ctx = createEventsManager();
    em = ctx.em;
    wm = ctx.wm;
  });

  // ─────────────────────────────────────────────
  // clear()
  // ─────────────────────────────────────────────
  describe('clear()', () => {
    test('should clear all subscribers so they no longer receive events', () => {
      const events: string[] = [];
      em.subscribe((event) => events.push(event.eventType));
      em.subscribe((event) => events.push(event.eventType));

      // Verify subscribers are active
      em.broadcast(makeEvent(EventType.Pushed));
      expect(events.length).toBe(2);

      // Clear
      em.clear();

      // Broadcast again - no subscribers should receive it
      em.broadcast(makeEvent(EventType.Pushed));
      expect(events.length).toBe(2); // unchanged
    });

    test('should resolve pending completion waiters', async () => {
      // Start a waiter with a long timeout
      const waiterPromise = em.waitForJobCompletion('job-100', 30000);

      // Clear should resolve all pending waiters
      em.clear();

      // The waiter should resolve with true (resolved, not timed out)
      const result = await waiterPromise;
      expect(result).toBe(true);
    });

    test('should resolve multiple pending completion waiters for different jobs', async () => {
      const waiter1 = em.waitForJobCompletion('job-a', 30000);
      const waiter2 = em.waitForJobCompletion('job-b', 30000);
      const waiter3 = em.waitForJobCompletion('job-c', 30000);

      em.clear();

      const results = await Promise.all([waiter1, waiter2, waiter3]);
      expect(results).toEqual([true, true, true]);
    });

    test('should resolve multiple waiters for the same job', async () => {
      const waiter1 = em.waitForJobCompletion('job-x', 30000);
      const waiter2 = em.waitForJobCompletion('job-x', 30000);

      em.clear();

      const results = await Promise.all([waiter1, waiter2]);
      expect(results).toEqual([true, true]);
    });

    test('after clear, needsBroadcast should return false (no subscribers or waiters)', () => {
      em.subscribe(() => {});
      em.waitForJobCompletion('job-1', 30000);

      expect(em.needsBroadcast()).toBe(true);

      em.clear();

      expect(em.needsBroadcast()).toBe(false);
    });

    test('should be safe to call clear multiple times', () => {
      em.subscribe(() => {});
      em.clear();
      em.clear(); // second call should not throw
      expect(em.needsBroadcast()).toBe(false);
    });

    test('should not affect new subscribers added after clear', () => {
      em.subscribe(() => {});
      em.clear();

      const events: string[] = [];
      em.subscribe((event) => events.push(event.eventType));

      em.broadcast(makeEvent(EventType.Pushed));
      expect(events).toEqual(['pushed']);
    });
  });

  // ─────────────────────────────────────────────
  // needsBroadcast()
  // ─────────────────────────────────────────────
  describe('needsBroadcast()', () => {
    test('should return false with no subscribers, waiters, or webhooks', () => {
      expect(em.needsBroadcast()).toBe(false);
    });

    test('should return true when there are subscribers', () => {
      em.subscribe(() => {});
      expect(em.needsBroadcast()).toBe(true);
    });

    test('should return true when there are completion waiters', () => {
      em.waitForJobCompletion('job-1', 30000);
      expect(em.needsBroadcast()).toBe(true);

      // Cleanup: clear the waiter to avoid dangling timer
      em.clear();
    });

    test('should return true when there are enabled webhooks', () => {
      wm.add('https://example.com/hook', ['job.completed']);
      expect(em.needsBroadcast()).toBe(true);
    });

    test('should return false after removing all subscribers', () => {
      const unsub1 = em.subscribe(() => {});
      const unsub2 = em.subscribe(() => {});

      expect(em.needsBroadcast()).toBe(true);

      unsub1();
      unsub2();

      expect(em.needsBroadcast()).toBe(false);
    });

    test('should return false after removing a subscriber when no webhooks or waiters exist', () => {
      const unsub = em.subscribe(() => {});
      expect(em.needsBroadcast()).toBe(true);

      unsub();
      expect(em.needsBroadcast()).toBe(false);
    });

    test('should still return true if only webhooks remain after removing subscribers', () => {
      wm.add('https://example.com/hook', ['job.completed']);
      const unsub = em.subscribe(() => {});

      expect(em.needsBroadcast()).toBe(true);

      unsub();

      // Still true because webhooks are present
      expect(em.needsBroadcast()).toBe(true);
    });

    test('should still return true if only waiters remain after removing subscribers', () => {
      const unsub = em.subscribe(() => {});
      em.waitForJobCompletion('job-1', 30000);

      unsub();

      // Still true because completion waiters exist
      expect(em.needsBroadcast()).toBe(true);

      // Cleanup
      em.clear();
    });
  });

  // ─────────────────────────────────────────────
  // mapEventToWebhook() - tested via broadcast + webhook integration
  // ─────────────────────────────────────────────
  describe('mapEventToWebhook() via broadcast webhook integration', () => {
    // mapEventToWebhook is private, so we test it indirectly through broadcast.
    // We register a webhook listening for all events and check which events trigger it.

    test('Completed event should trigger job.completed webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed']);

      // Override trigger to capture calls without making real HTTP requests
      const originalTrigger = wm.trigger.bind(wm);
      wm.trigger = async (event, jobId, queue, extra) => {
        triggered = true;
        expect(event).toBe('job.completed');
      };

      em.broadcast(makeEvent(EventType.Completed));
      // Allow async trigger to execute
      await Bun.sleep(10);
      expect(triggered).toBe(true);

      wm.trigger = originalTrigger;
    });

    test('Failed event should trigger job.failed webhook', async () => {
      let triggeredEvent: string | null = null;
      wm.add('https://example.com/hook', ['job.failed']);

      wm.trigger = async (event) => {
        triggeredEvent = event;
      };

      em.broadcast(makeEvent(EventType.Failed, { error: 'something went wrong' }));
      await Bun.sleep(10);
      expect(triggeredEvent).toBe('job.failed');
    });

    test('Pushed event should trigger job.pushed webhook', async () => {
      let triggeredEvent: string | null = null;
      wm.add('https://example.com/hook', ['job.pushed']);

      wm.trigger = async (event) => {
        triggeredEvent = event;
      };

      em.broadcast(makeEvent(EventType.Pushed));
      await Bun.sleep(10);
      expect(triggeredEvent).toBe('job.pushed');
    });

    test('Pulled event should trigger job.started webhook', async () => {
      let triggeredEvent: string | null = null;
      wm.add('https://example.com/hook', ['job.started']);

      wm.trigger = async (event) => {
        triggeredEvent = event;
      };

      em.broadcast(makeEvent(EventType.Pulled));
      await Bun.sleep(10);
      expect(triggeredEvent).toBe('job.started');
    });

    test('Progress event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.progress']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Progress));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('Stalled event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.stalled']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Stalled));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('Delayed event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed', 'job.failed']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Delayed));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('Removed event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Removed));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('Duplicated event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Duplicated));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('Retried event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Retried));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('WaitingChildren event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.WaitingChildren));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('Drained event should NOT trigger any webhook', async () => {
      let triggered = false;
      wm.add('https://example.com/hook', ['job.completed']);

      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Drained));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });
  });

  // ─────────────────────────────────────────────
  // broadcast() - webhook integration details
  // ─────────────────────────────────────────────
  describe('broadcast() webhook integration', () => {
    test('should not call webhook trigger when no webhooks are registered', async () => {
      let triggered = false;
      wm.trigger = async () => {
        triggered = true;
      };

      em.broadcast(makeEvent(EventType.Completed));
      await Bun.sleep(10);
      expect(triggered).toBe(false);
    });

    test('should pass correct jobId and queue to webhook trigger', async () => {
      let capturedJobId: string | null = null;
      let capturedQueue: string | null = null;

      wm.add('https://example.com/hook', ['job.completed']);
      wm.trigger = async (_event, jobId, queue) => {
        capturedJobId = jobId;
        capturedQueue = queue;
      };

      em.broadcast(makeEvent(EventType.Completed, { jobId: 'my-job-42', queue: 'emails' }));
      await Bun.sleep(10);

      expect(capturedJobId).toBe('my-job-42');
      expect(capturedQueue).toBe('emails');
    });

    test('should pass data and error in extra to webhook trigger', async () => {
      let capturedExtra: { data?: unknown; error?: string } | undefined;

      wm.add('https://example.com/hook', ['job.failed']);
      wm.trigger = async (_event, _jobId, _queue, extra) => {
        capturedExtra = extra;
      };

      em.broadcast(
        makeEvent(EventType.Failed, { error: 'timeout', data: { attempt: 3 } })
      );
      await Bun.sleep(10);

      expect(capturedExtra).toBeDefined();
      expect(capturedExtra!.error).toBe('timeout');
      expect(capturedExtra!.data).toEqual({ attempt: 3 });
    });

    test('broadcast should not throw even if webhook trigger fails', async () => {
      wm.add('https://example.com/hook', ['job.completed']);
      wm.trigger = async () => {
        throw new Error('Network error');
      };

      // Should not throw
      expect(() => {
        em.broadcast(makeEvent(EventType.Completed));
      }).not.toThrow();

      // Wait for the async error to be caught
      await Bun.sleep(10);
    });

    test('broadcast fast path: does nothing when no subscribers, webhooks, or waiters', () => {
      // With no subscribers, no webhooks, and no waiters, broadcast should be a no-op
      // We verify it does not throw and completes instantly
      expect(() => {
        em.broadcast(makeEvent(EventType.Pushed));
      }).not.toThrow();
    });

    test('broadcast should notify completion waiters for the matching job only', async () => {
      const waiterA = em.waitForJobCompletion('job-a', 5000);
      const waiterB = em.waitForJobCompletion('job-b', 200);

      // Complete only job-a
      em.broadcast(makeEvent(EventType.Completed, { jobId: 'job-a' }));

      const resultA = await waiterA;
      expect(resultA).toBe(true);

      // job-b should time out since it was not completed
      const resultB = await waiterB;
      expect(resultB).toBe(false);
    });

    test('broadcast of non-Completed event should not resolve completion waiters', async () => {
      const waiter = em.waitForJobCompletion('job-1', 200);

      // Broadcast a non-completion event for the same job
      em.broadcast(makeEvent(EventType.Pushed, { jobId: 'job-1' }));
      em.broadcast(makeEvent(EventType.Pulled, { jobId: 'job-1' }));
      em.broadcast(makeEvent(EventType.Progress, { jobId: 'job-1' }));

      // Should timeout since none of these are Completed
      const result = await waiter;
      expect(result).toBe(false);
    });
  });

  // ─────────────────────────────────────────────
  // subscribe() edge cases
  // ─────────────────────────────────────────────
  describe('subscribe() edge cases', () => {
    test('subscribing the same function twice should only add it once (Set behavior)', () => {
      const events: string[] = [];
      const callback = (event: { eventType: string }) => events.push(event.eventType);

      em.subscribe(callback);
      em.subscribe(callback);

      em.broadcast(makeEvent(EventType.Pushed));
      // Set deduplicates, so callback is called only once
      expect(events.length).toBe(1);
    });

    test('unsubscribing a function that was never subscribed should not throw', () => {
      const unsub = em.subscribe(() => {});
      unsub();
      // Calling unsub again should be safe
      expect(() => unsub()).not.toThrow();
    });
  });
});
