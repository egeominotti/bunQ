import type { SQL } from 'bun';
import {
  latestPostgresEventJournalBaseline,
  loadPostgresJournalEventsAfter,
  type PostgresEventJournalCursor,
  type PostgresJournalEvent,
} from './eventJournal';
import { PostgresEventCatchupCursors } from './eventCatchupCursors';
import { loadPostgresEventPruneWatermarks } from './eventPruneWatermarks';
import { POSTGRES_EVENT_CHANNEL } from './schema';
import { tryTrimPostgresQueueEvents } from './telemetry';
import type { PostgresContext } from './context';
import type { PostgresDeliveredStoreEvent, PostgresStoreEvent } from './types';

type EventListener = (event: PostgresDeliveredStoreEvent) => void;
type InvalidationListener = (queue: string) => void;
type DrainStatusListener = (error: Error | null) => void;
type WakeResolver = () => void;

const MIN_EVENT_DRAIN_BATCH = 1_000;
const MAX_EVENT_DRAIN_BATCH = 4_096;

export function postgresEventDrainBatchSize(maxQueueEvents: number): number {
  return Math.max(MIN_EVENT_DRAIN_BATCH, Math.min(MAX_EVENT_DRAIN_BATCH, maxQueueEvents));
}

function requiresQueueInvalidation(event: PostgresStoreEvent): boolean {
  return event.type.startsWith('queue-') || (!event.job && event.state === null);
}

interface WakeRegistration {
  readonly resolve: WakeResolver;
  readonly timer: ReturnType<typeof setTimeout>;
  readonly signal?: AbortSignal;
  readonly onAbort?: () => void;
}

/** Commit-ordered journal polling with LISTEN/NOTIFY used only as a wakeup hint. */
export class PostgresEventStream {
  private subscription: SQL.ListenSubscription | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private readonly listeners = new Set<EventListener>();
  private readonly invalidationListeners = new Set<InvalidationListener>();
  private readonly drainStatusListeners = new Set<DrainStatusListener>();
  private readonly waiters = new Map<string, Set<WakeRegistration>>();
  private readonly queueCursors = new PostgresEventCatchupCursors();
  private readonly retentionQueues = new Set<string>();
  private journalCursor: PostgresEventJournalCursor = { commitSeq: 0, eventId: 0 };
  private cursor = 0;
  private checkpointScanRequested = false;
  private drainRequested = false;
  private draining: Promise<void> | null = null;
  private retentionRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(private readonly ctx: PostgresContext) {}

  async start(): Promise<void> {
    const [baseline, watermarks] = await Promise.all([
      latestPostgresEventJournalBaseline(this.ctx),
      loadPostgresEventPruneWatermarks(this.ctx),
    ]);
    this.cursor = baseline.latestEventId;
    this.journalCursor = { commitSeq: baseline.commitSeq, eventId: Number.MAX_SAFE_INTEGER };
    this.queueCursors.reset(baseline.commitSeq);
    for (const watermark of watermarks) {
      if (watermark.commitSeq > baseline.commitSeq) {
        this.queueCursors.applied(watermark.queue, watermark.commitSeq);
      }
      // `PostgresQueueManagerState` loads a full snapshot of every queue after
      // this call, so recorded prunes are already covered and must not refresh
      // the freshly loaded read model.
      this.queueCursors.remember(watermark);
    }
    this.subscription = await this.ctx.sql.listen(
      POSTGRES_EVENT_CHANNEL,
      (payload) => this.handleNotification(payload),
      () => void this.drain()
    );
    this.pollTimer = setInterval(() => void this.drain(), this.ctx.config.pollIntervalMs);
  }

  subscribe(listener: EventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  subscribeInvalidation(listener: InvalidationListener): () => void {
    this.invalidationListeners.add(listener);
    return () => this.invalidationListeners.delete(listener);
  }

  subscribeDrainStatus(listener: DrainStatusListener): () => void {
    this.drainStatusListeners.add(listener);
    return () => this.drainStatusListeners.delete(listener);
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.pollTimer = null;
    if (this.retentionRetryTimer) clearTimeout(this.retentionRetryTimer);
    this.retentionRetryTimer = null;
    await this.subscription?.unlisten();
    this.subscription = null;
    for (const registrations of this.waiters.values()) {
      for (const registration of [...registrations]) registration.resolve();
    }
    this.waiters.clear();
    await this.draining;
  }

  wait(queue: string, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    if (this.closed || timeoutMs <= 0 || signal?.aborted) return Promise.resolve();
    return new Promise((resolve) => {
      const finish = () => {
        this.removeWaiter(queue, registration);
        resolve();
      };
      const onAbort = () => finish();
      const registration: WakeRegistration = {
        resolve: finish,
        timer: setTimeout(finish, timeoutMs),
        ...(signal && { signal, onAbort }),
      };
      let registrations = this.waiters.get(queue);
      if (!registrations) {
        registrations = new Set();
        this.waiters.set(queue, registrations);
      }
      registrations.add(registration);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }

  private handleNotification(payload: string): void {
    try {
      const parsed = JSON.parse(payload) as {
        namespace?: string;
        queue?: string;
        prunedThrough?: number;
        retentionRequested?: boolean;
        scanPruneWatermarks?: boolean;
      };
      if (parsed.namespace !== this.ctx.config.namespace) return;
      if (parsed.queue) this.wake(parsed.queue);
      if (
        parsed.scanPruneWatermarks === true ||
        (Number.isSafeInteger(parsed.prunedThrough) && parsed.prunedThrough! > 0)
      ) {
        this.checkpointScanRequested = true;
      }
      if (parsed.queue && parsed.retentionRequested === true) {
        this.retentionQueues.add(parsed.queue);
      }
    } catch {
      this.checkpointScanRequested = true;
      this.wakeAll();
    }
    void this.drain(false);
  }

  private wake(queue: string): void {
    const registrations = this.waiters.get(queue);
    if (!registrations) return;
    for (const registration of [...registrations]) registration.resolve();
  }

  private wakeAll(): void {
    for (const queue of this.waiters.keys()) this.wake(queue);
  }

  private removeWaiter(queue: string, registration: WakeRegistration): void {
    const registrations = this.waiters.get(queue);
    registrations?.delete(registration);
    if (registrations?.size === 0) this.waiters.delete(queue);
    clearTimeout(registration.timer);
    if (registration.signal && registration.onAbort) {
      registration.signal.removeEventListener('abort', registration.onAbort);
    }
  }

  private async drain(scanPruneWatermarks = true): Promise<void> {
    if (scanPruneWatermarks) this.checkpointScanRequested = true;
    this.drainRequested = true;
    if (this.closed) return;
    if (this.draining) return this.draining;
    this.draining = this.drainLoop()
      .then(() => {
        if (!this.closed && !this.retentionRetryTimer) this.reportDrainStatus(null);
      })
      .catch((error: unknown) => {
        this.reportDrainStatus(error instanceof Error ? error : new Error(String(error)));
        this.scheduleRetentionRetry();
      })
      .finally(() => {
        this.draining = null;
        if (this.drainRequested && !this.closed) void this.drain(false);
      });
    return this.draining;
  }

  private async drainLoop(): Promise<void> {
    const batchSize = postgresEventDrainBatchSize(this.ctx.config.maxQueueEvents);
    while (!this.closed) {
      this.drainRequested = false;
      await this.convergeEventRetention();
      await this.scanPruneWatermarks();
      const entries = await loadPostgresJournalEventsAfter(this.ctx, this.journalCursor, batchSize);
      // Always re-check retention against the pre-batch position: a prune that
      // committed after the previous scan can have removed queue history this
      // batch does not contain, and applying the batch would hide the gap.
      if (entries.length > 0) this.checkpointScanRequested = true;
      await this.scanPruneWatermarks();
      for (const entry of entries) this.applyEvent(entry);
      if (entries.length >= batchSize || this.drainRequested) continue;
      return;
    }
  }

  private async convergeEventRetention(): Promise<void> {
    if (this.retentionRetryTimer) return;
    const maxLength = this.ctx.config.maxQueueEvents;
    const queues = [...this.retentionQueues].sort();
    for (const queue of queues) {
      this.retentionQueues.delete(queue);
      try {
        const removed = await tryTrimPostgresQueueEvents(this.ctx, queue, maxLength);
        if (removed === null) {
          this.retentionQueues.add(queue);
          this.scheduleRetentionRetry();
        }
      } catch (error) {
        this.retentionQueues.add(queue);
        throw error;
      }
    }
  }

  private scheduleRetentionRetry(): void {
    if (this.closed || this.retentionRetryTimer || this.retentionQueues.size === 0) return;
    const delay = Math.min(this.ctx.config.pollIntervalMs, 250);
    this.retentionRetryTimer = setTimeout(() => {
      this.retentionRetryTimer = null;
      if (!this.closed && this.retentionQueues.size > 0) void this.drain(false);
    }, delay);
  }

  private applyEvent(entry: PostgresJournalEvent): void {
    const { event, commitSeq } = entry;
    this.journalCursor = { commitSeq, eventId: event.id };
    this.cursor = Math.max(this.cursor, event.id);
    this.queueCursors.applied(event.queue, commitSeq);
    if (requiresQueueInvalidation(event)) this.invalidateQueue(event.queue);
    const delivered = { ...event, commitSeq };
    for (const listener of this.listeners) {
      try {
        listener(delivered);
      } catch {
        // Listener isolation: one observer cannot stop durable cursor progress.
      }
    }
  }

  private invalidateQueue(queue: string): void {
    for (const listener of this.invalidationListeners) {
      try {
        listener(queue);
      } catch {
        // Invalidation observers cannot interrupt durable cursor progress.
      }
    }
  }

  private async scanPruneWatermarks(): Promise<void> {
    if (!this.checkpointScanRequested) return;
    this.checkpointScanRequested = false;
    try {
      const watermarks = await loadPostgresEventPruneWatermarks(this.ctx);
      for (const watermark of watermarks) {
        if (this.queueCursors.requiresRefresh(watermark)) this.invalidateQueue(watermark.queue);
      }
    } catch (error) {
      this.checkpointScanRequested = true;
      throw error;
    }
  }

  private reportDrainStatus(error: Error | null): void {
    for (const listener of this.drainStatusListeners) {
      try {
        listener(error);
      } catch {
        // Diagnostics must not stop the next polling retry.
      }
    }
  }
}
