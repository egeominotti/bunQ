/**
 * AUDIT — BUG H4: AckBatcher overflow silently drops oldest pending ACKs.
 *
 * src/client/worker/ackBatcher.ts:48-53 — when pendingAcks reaches
 * MAX_PENDING_ACKS (10000), the batcher splices out and DROPS the oldest
 * ~10% (1000) of pending ACKs. Their promises are rejected (fire-and-forget)
 * but those ACKs are NEVER sent to the server. The acked jobs therefore stay
 * 'processing' on the server forever (re-queued indefinitely after a
 * stall/reconnect). The drop happens with NO retry and NO logging.
 *
 * This test fills the batcher beyond its overflow threshold while a flush is
 * blocked/in-flight, using a fake ACK sink that records exactly which job IDs
 * were sent. CORRECT behavior asserted: every queued ack ID must eventually
 * reach the sink (or the overflow be surfaced as back-pressure) — none may be
 * silently discarded.
 *
 * Expected with the current buggy code: RED (acks dropped without send).
 */

import { describe, test, expect } from 'bun:test';
import { AckBatcher } from '../src/client/worker/ackBatcher';
import type { TcpConnection } from '../src/client/worker/types';

// MAX_PENDING_ACKS is a private constant in AckBatcher = 10000.
const MAX_PENDING_ACKS = 10_000;

/**
 * Fake ACK sink. Records every job ID that actually reaches `send` (i.e. that
 * the batcher actually tried to ACK on the server). The first flush is held
 * open via a gate so that pending ACKs accumulate past the overflow threshold
 * while a flush is in-flight.
 */
function createFakeSink() {
  const sentIds = new Set<string>();
  let firstCall = true;
  let releaseGate!: () => void;
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve;
  });

  const tcp: TcpConnection = {
    send: async (cmd: Record<string, unknown>): Promise<Record<string, unknown>> => {
      const ids = (cmd.ids as string[]) ?? [];
      if (firstCall) {
        firstCall = false;
        // Hold the very first flush open so the buffer can fill up while a
        // flush is in-flight (simulating a slow / stalled server round-trip).
        await gate;
      }
      for (const id of ids) sentIds.add(id);
      return { ok: true };
    },
  };

  return { tcp, sentIds, releaseGate: () => releaseGate() };
}

describe('AUDIT BUG H4 — AckBatcher overflow must not silently drop ACKs', () => {
  test('every queued ack id eventually reaches the sink when buffer overflows during in-flight flush', async () => {
    const { tcp, sentIds, releaseGate } = createFakeSink();

    // Large batchSize so a single queued ack does not immediately flush;
    // we want to control flushing manually and let the buffer grow.
    const batcher = new AckBatcher({
      batchSize: 1_000_000, // never auto-flush on size during fill
      interval: 1_000_000, // never auto-flush on timer during fill
      embedded: false,
    });
    batcher.setTcp(tcp);

    // Total jobs we will ACK: enough to trigger at least one overflow drop.
    // Overflow drops floor(MAX * 0.1) = 1000 oldest entries each time the
    // buffer hits MAX_PENDING_ACKS.
    const TOTAL = MAX_PENDING_ACKS + 1_500; // 11500 -> guarantees an overflow event

    const allIds: string[] = [];
    const queuePromises: Array<Promise<void>> = [];
    // We must NOT let unhandled rejections crash the test; attach a catch that
    // records which ids were rejected (i.e. dropped without being sent).
    const rejectedIds = new Set<string>();

    // Kick off ONE in-flight flush that is held open by the gate. This keeps a
    // flush "in progress" (in real life: a slow server) so that subsequent
    // queue() calls accumulate in pendingAcks until overflow.
    // We trigger it by queueing one ack then calling flush() manually.
    const primerId = 'job-primer';
    allIds.push(primerId);
    const primerP = batcher.queue(primerId, { ok: true }).catch(() => {
      rejectedIds.add(primerId);
    });
    queuePromises.push(primerP);
    const heldFlush = batcher.flush(); // sends primer batch, blocks on gate

    // Now fill the buffer well past MAX_PENDING_ACKS. These all stay pending
    // because no flush can complete while the gate is held and we use huge
    // batchSize/interval so nothing auto-flushes.
    for (let i = 0; i < TOTAL; i++) {
      const id = `job-${i}`;
      allIds.push(id);
      const p = batcher.queue(id, { i }).catch(() => {
        // A rejection here means this ack was dropped/abandoned.
        rejectedIds.add(id);
      });
      queuePromises.push(p);
    }

    // Give microtasks a chance so any overflow rejections settle.
    await Promise.resolve();
    await Promise.resolve();

    // Release the held flush so the in-flight ACKB completes.
    releaseGate();
    await heldFlush;

    // Flush whatever remains in the buffer (the non-dropped survivors).
    await batcher.flush();
    await batcher.waitForInFlight();

    // Let all queued promises settle.
    await Promise.allSettled(queuePromises);

    // CORRECT behavior: NO acked job is silently discarded. Every id we queued
    // must have either reached the sink, OR — if back-pressure were the design
    // — the buffer would never have accepted it. With the current code, the
    // ack is ACCEPTED (queue() called) then dropped without ever being sent.
    const neverSent = allIds.filter((id) => !sentIds.has(id));

    // Diagnostic: the dropped ids should equal the never-sent ids (proving the
    // overflow path drops without sending).
    const droppedAndNeverSent = neverSent.filter((id) => rejectedIds.has(id));

    if (neverSent.length > 0) {
      // eslint-disable-next-line no-console
      console.error(
        `[H4] ${neverSent.length} acks were accepted but NEVER sent to the sink ` +
          `(${droppedAndNeverSent.length} of them were rejected/dropped). ` +
          `Examples: ${neverSent.slice(0, 5).join(', ')}`
      );
    }

    // The assertion proving the bug: every accepted ack must reach the sink.
    // Buggy code drops ~1000 oldest -> neverSent.length > 0 -> FAILS (RED).
    expect(neverSent).toEqual([]);
  });
});
