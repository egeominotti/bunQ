/**
 * ACK Batcher
 * Batches acknowledgments for efficient TCP communication
 */

import type { PendingAck, TcpConnection } from './types';
import { getSharedManager } from '../manager';
import { jobId } from '../../domain/types/job';
import { ignoredAckIndices } from './ackOutcome';

/** ACK batcher configuration */
export interface AckBatcherConfig {
  batchSize: number;
  interval: number;
  embedded: boolean;
  maxBatchSize?: (pendingCount: number) => number;
  maxRetries?: number;
  retryDelayMs?: number;
}

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_DELAY_MS = 100;

/**
 * Batches ACK operations for efficient processing
 */
export class AckBatcher {
  private readonly MAX_PENDING_ACKS = 10000;
  private readonly pendingAcks: PendingAck[] = [];
  private ackTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly config: AckBatcherConfig;
  private tcp: TcpConnection | null = null;
  private stopped = false;
  // Track in-flight flush operations to ensure all complete before close
  private readonly inFlightFlushes: Set<Promise<void>> = new Set();

  constructor(config: AckBatcherConfig) {
    this.config = config;
  }

  /** Set TCP connection */
  setTcp(tcp: TcpConnection | null): void {
    this.tcp = tcp;
  }

  /** Queue ACK for batch processing (with optional lock token) */
  async queue(
    id: string,
    result: unknown,
    token?: string,
    removeOnComplete?: boolean,
    onQueued?: () => void
  ): Promise<boolean> {
    // Backpressure: never silently drop accepted ACKs. When the buffer is at/over
    // capacity, force a flush of the pending ACKs (sending them to the server)
    // before accepting more. If a flush is already in-flight, await it first to
    // avoid reentrancy/double-send, then re-check and flush any remainder.
    while (this.pendingAcks.length >= this.MAX_PENDING_ACKS && !this.stopped) {
      // Wait for any in-flight flush to drain the buffer it owns.
      await this.waitForInFlight();
      // If still over capacity (buffer refilled or nothing was in-flight),
      // flush the current pending ACKs ourselves to make room. The enclosing
      // while-loop already re-checks `stopped`, so at most one extra flush runs
      // if stop() lands during the await above (flushing on shutdown is fine).
      if (this.pendingAcks.length >= this.MAX_PENDING_ACKS) {
        await this.startTrackedFlush();
      }
    }

    return new Promise<boolean>((resolve, reject) => {
      // Transfer the delivery from the Worker's unqueued frontier into this
      // pending batch in one synchronous turn.
      onQueued?.();
      this.pendingAcks.push({
        id,
        result,
        token,
        removeOnComplete,
        resolve,
        reject,
      });

      if (!this.flushIfThresholdReached()) {
        this.ackTimer ??= setTimeout(() => {
          this.ackTimer = null;
          void this.startTrackedFlush();
        }, this.config.interval);
      }
    });
  }

  /** Re-evaluate a dynamic batch ceiling after worker capacity changes. */
  notifyCapacityChanged(): void {
    this.flushIfThresholdReached();
  }

  /** Flush pending ACKs with retry logic */
  async flush(): Promise<void> {
    if (this.pendingAcks.length === 0) return;

    const batch = this.pendingAcks.splice(0, this.pendingAcks.length);

    if (this.ackTimer) {
      clearTimeout(this.ackTimer);
      this.ackTimer = null;
    }

    await this.sendBatchWithRetry(batch);
  }

  /** Send batch with exponential backoff retry */
  private async sendBatchWithRetry(batch: PendingAck[]): Promise<void> {
    const maxRetries = this.config.maxRetries ?? DEFAULT_MAX_RETRIES;
    const baseDelay = this.config.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (this.stopped) {
        // If stopped, don't retry - just fail remaining
        const error = new Error('AckBatcher stopped');
        for (const ack of batch) {
          ack.reject(error);
        }
        return;
      }

      try {
        let ignoredIndices: ReadonlySet<number> = new Set();
        if (this.config.embedded) {
          const manager = getSharedManager();
          // Include tokens for lock verification
          const items = batch.map((a) => ({
            id: jobId(a.id),
            result: a.result,
            token: a.token,
            removeOnComplete: a.removeOnComplete,
          }));
          const outcome = await manager.ackBatchWithResults(items);
          ignoredIndices = ignoredAckIndices(outcome, batch);
        } else if (this.tcp) {
          // Include tokens for lock verification
          const removeOnCompletes = batch.map((a) => a.removeOnComplete ?? null);
          const response = await this.tcp.send({
            cmd: 'ACKB',
            ids: batch.map((a) => a.id),
            results: batch.map((a) => a.result),
            tokens: batch.map((a) => a.token ?? ''),
            ...(removeOnCompletes.some((value) => value === true) ? { removeOnCompletes } : {}),
          });

          if (!response.ok) {
            throw new Error((response.error as string | undefined) ?? 'Batch ACK failed');
          }
          ignoredIndices = ignoredAckIndices(response.data, batch);
        }

        // Success
        for (let index = 0; index < batch.length; index++) {
          batch[index].resolve(!ignoredIndices.has(index));
        }
        return;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));

        if (attempt < maxRetries) {
          // Exponential backoff: 100ms, 200ms, 400ms
          const delay = baseDelay * Math.pow(2, attempt);
          await Bun.sleep(delay);
        }
      }
    }

    // All retries exhausted
    console.error(
      `[AckBatcher] Flush failed after ${maxRetries + 1} attempts:`,
      lastError?.message,
      `(${batch.length} acks lost)`
    );
    for (const ack of batch) {
      ack.reject(lastError ?? new Error('Unknown error'));
    }
  }

  /** Stop and cleanup - clears pending acks without processing */
  stop(): void {
    this.stopped = true;
    if (this.ackTimer) {
      clearTimeout(this.ackTimer);
      this.ackTimer = null;
    }
    // Clear any pending acks (they should have been flushed before stop)
    this.pendingAcks.length = 0;
  }

  /** Check if there are pending acks */
  hasPending(): boolean {
    return this.pendingAcks.length > 0;
  }

  /** Wait for all in-flight flush operations to complete */
  async waitForInFlight(): Promise<void> {
    if (this.inFlightFlushes.size === 0) return;
    await Promise.all(this.inFlightFlushes);
  }

  private effectiveBatchSize(): number {
    const maximum = this.config.maxBatchSize?.(this.pendingAcks.length);
    return maximum === undefined ? this.config.batchSize : Math.min(this.config.batchSize, maximum);
  }

  private flushIfThresholdReached(): boolean {
    if (
      this.stopped ||
      this.pendingAcks.length === 0 ||
      this.pendingAcks.length < this.effectiveBatchSize()
    ) {
      return false;
    }
    void this.startTrackedFlush();
    return true;
  }

  private startTrackedFlush(): Promise<void> {
    const flushPromise = this.flush();
    this.inFlightFlushes.add(flushPromise);
    void flushPromise.finally(() => this.inFlightFlushes.delete(flushPromise));
    return flushPromise;
  }
}
