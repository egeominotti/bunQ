/**
 * Bug Reproduction: pushJobBatch does NOT emit "pushed" events
 *
 * In src/application/operations/push.ts:
 * - pushJob (single) correctly broadcasts { eventType: 'pushed', queue, jobId, timestamp }
 *   after inserting a job (lines 215-220)
 * - pushJobBatch does NOT call ctx.broadcast() after inserting jobs (lines 271-274)
 *
 * This means subscribers and webhooks are never notified about batch-pushed jobs.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { EventType, type JobEvent } from '../src/domain/types/queue';

describe('Bug: pushJobBatch missing "pushed" events', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager({ dependencyCheckMs: 50 });
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('single pushJob emits "pushed" event (baseline)', async () => {
    const events: JobEvent[] = [];

    qm.subscribe((event) => {
      if (event.eventType === EventType.Pushed) {
        events.push(event);
      }
    });

    const job = await qm.push('events-q', { data: { msg: 'single' } });

    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe('pushed');
    expect(events[0].queue).toBe('events-q');
    expect(events[0].jobId).toBe(job.id);
    expect(events[0].timestamp).toBeGreaterThan(0);
  });

  test('pushJobBatch emits "pushed" event for each job', async () => {
    const events: JobEvent[] = [];

    qm.subscribe((event) => {
      if (event.eventType === EventType.Pushed) {
        events.push(event);
      }
    });

    const ids = await qm.pushBatch('events-q', [
      { data: { id: 1 } },
      { data: { id: 2 } },
      { data: { id: 3 } },
    ]);

    // Each job in the batch should emit a "pushed" event
    expect(events.length).toBe(3);
    expect(events[0].eventType).toBe('pushed');
    expect(events[0].queue).toBe('events-q');

    // All job IDs from the batch should appear in events
    const eventJobIds = events.map((e) => e.jobId);
    for (const id of ids) {
      expect(eventJobIds).toContain(id);
    }
  });

  test('pushJobBatch emits events for webhook triggers', async () => {
    const allEvents: JobEvent[] = [];

    qm.subscribe((event) => {
      allEvents.push(event);
    });

    // Push a batch of 5 jobs
    await qm.pushBatch('webhook-q', [
      { data: { task: 'email' } },
      { data: { task: 'sms' } },
      { data: { task: 'push-notif' } },
      { data: { task: 'slack' } },
      { data: { task: 'discord' } },
    ]);

    // Filter only "pushed" events for the webhook queue
    const pushedEvents = allEvents.filter(
      (e) => e.eventType === EventType.Pushed && e.queue === 'webhook-q'
    );

    // Webhooks rely on these events - each batch job must emit one
    expect(pushedEvents.length).toBe(5);
    for (const event of pushedEvents) {
      expect(event.queue).toBe('webhook-q');
      expect(event.timestamp).toBeGreaterThan(0);
    }
  });

  test('single push and batch push emit same number of events for same job count', async () => {
    // First: push 3 jobs individually and count events
    const singleEvents: JobEvent[] = [];
    const unsub1 = qm.subscribe((event) => {
      if (event.eventType === EventType.Pushed && event.queue === 'single-q') {
        singleEvents.push(event);
      }
    });

    await qm.push('single-q', { data: { id: 1 } });
    await qm.push('single-q', { data: { id: 2 } });
    await qm.push('single-q', { data: { id: 3 } });

    unsub1();

    // Then: push 3 jobs as batch and count events
    const batchEvents: JobEvent[] = [];
    qm.subscribe((event) => {
      if (event.eventType === EventType.Pushed && event.queue === 'batch-q') {
        batchEvents.push(event);
      }
    });

    await qm.pushBatch('batch-q', [
      { data: { id: 1 } },
      { data: { id: 2 } },
      { data: { id: 3 } },
    ]);

    // Both should emit the same number of "pushed" events
    expect(singleEvents.length).toBe(3);
    expect(batchEvents.length).toBe(singleEvents.length);
  });
});
