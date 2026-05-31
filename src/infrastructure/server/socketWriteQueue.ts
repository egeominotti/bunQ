/**
 * Per-socket write queue
 * =================================================================
 * Bun's `socket.write()` may write FEWER bytes than provided when the kernel
 * send buffer is full (backpressure). The unwritten tail is NOT retained by
 * Bun — the caller is responsible for buffering it and resending once the
 * socket fires `drain`. Ignoring the short count silently drops response
 * bytes, corrupting the length-prefixed frame protocol.
 *
 * This helper buffers any unwritten tail per-socket and preserves ordering:
 * while a pending tail exists, all subsequent writes are appended to the queue
 * (never written ahead of older bytes), and the queue is flushed in order from
 * the `drain` handler.
 */

import type { Socket } from 'bun';

/** Minimal write surface a socket must expose for the queue. */
interface WritableSocket {
  write(data: Uint8Array): number;
}

/** Per-socket pending write state. Stored on `socket.data`. */
export class SocketWriteQueue {
  /** Buffered chunks waiting to be (re)written, in order. */
  private pending: Uint8Array[] = [];
  /** Byte offset into pending[0] already written by a short write. */
  private offset = 0;
  /** Total queued bytes (for bounds / observability). */
  private queuedBytes = 0;
  /** Max queued bytes before the queue reports over-budget (0 = unbounded). */
  private readonly maxBytes: number;

  /**
   * @param maxBytes Soft cap on buffered bytes. When `bytesQueued` exceeds this,
   *   `isOverBudget` becomes true and the owner should drop the connection
   *   (bounds write-side memory for a client that stopped reading). 0 disables.
   */
  constructor(maxBytes = 0) {
    this.maxBytes = maxBytes > 0 ? maxBytes : 0;
  }

  /**
   * True once the buffered byte count exceeds the configured cap. The owner is
   * expected to terminate the connection and `clear()` the queue.
   */
  get isOverBudget(): boolean {
    return this.maxBytes > 0 && this.queuedBytes > this.maxBytes;
  }

  /**
   * Enqueue `data` for delivery on `socket`, preserving order.
   * Returns false if the socket appears closed (write threw / returned < 0).
   */
  write(socket: WritableSocket, data: Uint8Array): boolean {
    if (data.length === 0) return true;

    // If a tail is already pending, do NOT write ahead of it — append and wait
    // for drain to flush in order.
    if (this.pending.length > 0) {
      this.pending.push(data);
      this.queuedBytes += data.length;
      return true;
    }

    let written: number;
    try {
      written = socket.write(data);
    } catch {
      return false;
    }

    if (written < 0) {
      // Socket closed.
      return false;
    }

    if (written < data.length) {
      // Short write: buffer the unwritten tail; flush on drain.
      const tail = data.subarray(written);
      this.pending.push(tail);
      this.queuedBytes += tail.length;
    }
    return true;
  }

  /**
   * Flush as much of the pending queue as the socket accepts.
   * Call from the socket's `drain` handler. Stops at the first short write,
   * leaving the remainder queued for the next drain.
   */
  flush(socket: WritableSocket): void {
    while (this.pending.length > 0) {
      const chunk = this.pending[0];
      const view = this.offset > 0 ? chunk.subarray(this.offset) : chunk;

      let written: number;
      try {
        written = socket.write(view);
      } catch {
        return;
      }

      if (written <= 0) {
        // Socket not ready / closed; wait for the next drain.
        return;
      }

      if (written < view.length) {
        // Still backpressured: advance offset within the current chunk.
        this.offset += written;
        this.queuedBytes -= written;
        return;
      }

      // Whole chunk flushed.
      this.queuedBytes -= view.length;
      this.pending.shift();
      this.offset = 0;
    }
  }

  /** Number of bytes still buffered awaiting drain. */
  get bytesQueued(): number {
    return this.queuedBytes;
  }

  /** Whether there is unwritten data awaiting drain. */
  get hasPending(): boolean {
    return this.pending.length > 0;
  }

  /** Drop all buffered data (e.g. on close). */
  clear(): void {
    this.pending = [];
    this.offset = 0;
    this.queuedBytes = 0;
  }
}

/**
 * Convenience: write through a socket whose `data` carries a `writeQueue`.
 * Keeps call sites in tcp.ts terse and consistent.
 */
export function queuedWrite<T extends { writeQueue: SocketWriteQueue }>(
  socket: Socket<T>,
  data: Uint8Array
): void {
  socket.data.writeQueue.write(socket, data);
}
