/**
 * Batches completed-job ACKs into ACKB round-trips for higher throughput.
 *
 * Opt-in (see WorkerOptions.ackBatch). Items flush when the buffer reaches
 * `maxSize` or after `maxDelayMs`, whichever comes first; `flush()` drains the
 * rest on shutdown. Each item's `onSettled` fires after the batch is acked
 * (or errored), so the worker keeps a job "active" — and its lock renewed —
 * until the server has confirmed the ack.
 */

import type { Connection } from './connection.js';
import { compact } from './frame.js';
import { ignoredAckIndices } from './terminal-outcome.js';

export interface AckItem {
  id: string;
  token: string;
  result: unknown;
  /** `applied` is defined only after a successful, authoritative response. */
  onSettled: (err?: unknown, applied?: boolean) => void;
}

export class AckBatcher {
  private buffer: AckItem[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly connection: Connection,
    private readonly maxSize: number,
    private readonly maxDelayMs: number
  ) {}

  add(item: AckItem): void {
    this.buffer.push(item);
    if (this.buffer.length >= this.maxSize) {
      void this.flush();
    } else if (!this.timer) {
      this.timer = setTimeout(() => void this.flush(), this.maxDelayMs);
      this.timer.unref?.(); // don't keep the process alive for a pending flush
    }
  }

  /** Send the buffered ACKs as one ACKB (no-op when empty). */
  async flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.buffer.length === 0) return;
    const batch = this.buffer;
    this.buffer = [];
    // Capture the wire outcome first; settle callbacks run OUTSIDE the try so
    // a throwing callback can neither be re-invoked with an error (double
    // settle) nor starve the remaining items of their callback.
    let error: unknown;
    let ignoredIndices: ReadonlySet<number> = new Set();
    try {
      const response = await this.connection.call(
        compact({
          cmd: 'ACKB',
          ids: batch.map((b) => b.id),
          tokens: batch.map((b) => b.token),
          results: batch.map((b) => b.result),
        }) as { cmd: string }
      );
      ignoredIndices = ignoredAckIndices(response.data, batch);
    } catch (err) {
      error = err ?? new Error('ACKB failed');
    }
    for (let index = 0; index < batch.length; index++) {
      try {
        batch[index].onSettled(error, error === undefined ? !ignoredIndices.has(index) : undefined);
      } catch {
        // A callback error (e.g. an unhandled 'error' emit in the worker)
        // must never prevent the other items from settling.
      }
    }
  }
}
