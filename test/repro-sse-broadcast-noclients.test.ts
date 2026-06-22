/**
 * Repro: SseHandler.broadcast does heavy per-event work even with ZERO clients.
 *
 * The TCP server subscribes (wsHandler + sseHandler).broadcast to every job
 * event. wsHandler.broadcast early-returns when clients.size === 0, but
 * sseHandler.broadcast did NOT — so every job event still:
 *   - JSON.stringify + TextEncoder.encode (twice, incl. the queue:counts event),
 *   - bufferEvent (push + shift on the ring),
 *   - getQueueJobCounts(queue), which is O(queue size + jobIndex size).
 *
 * During a bulk push the broadcast fires once per job AFTER all jobs are in the
 * queue, so getQueueJobCounts walks the full queue every time => O(N²) per
 * batch, even though no dashboard is connected.
 *
 * Fix: mirror wsHandler — `if (this.clients.size === 0) return;` at the top of
 * sseHandler.broadcast. With no clients there is nothing to send and nothing to
 * buffer that anyone could replay.
 */
import { describe, expect, test } from 'bun:test';
import { SseHandler } from '../src/infrastructure/server/sseHandler';
import { EventType } from '../src/domain/types/queue';
import type { JobId } from '../src/domain/types/job';

describe('sse broadcast with no clients', () => {
  test('does not compute queue counts and stays cheap when no clients connected', () => {
    const sse = new SseHandler();
    let countCalls = 0;
    // Inject a queueManager stub that records getQueueJobCounts invocations.
    (sse as unknown as { queueManager: unknown }).queueManager = {
      getQueueJobCounts: () => {
        countCalls++;
        return {
          waiting: 0,
          prioritized: 0,
          active: 0,
          completed: 0,
          failed: 0,
          delayed: 0,
        };
      },
    };

    const N = 20000;
    const t0 = Bun.nanoseconds();
    for (let i = 0; i < N; i++) {
      sse.broadcast({
        eventType: EventType.Pushed,
        queue: 'q',
        jobId: String(i) as JobId,
        timestamp: 0,
      });
    }
    const ms = (Bun.nanoseconds() - t0) / 1e6;

    // With zero clients, broadcast must do no per-event count computation...
    expect(countCalls).toBe(0);
    // ...and must be cheap (no JSON.stringify/encode/buffer churn per event).
    expect(ms).toBeLessThan(200);
  });
});
