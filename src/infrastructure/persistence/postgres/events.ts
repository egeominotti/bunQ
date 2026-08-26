import type { SQL } from 'bun';
import {
  latestPostgresEventJournalBaseline,
  loadPostgresJournalEventsAfter,
  type PostgresEventJournalCursor,
  type PostgresJournalEvent,
} from './eventJournal';
import { loadPostgresEventPruneWatermarks } from './eventPruneWatermarks';
import { POSTGRES_EVENT_CHANNEL } from './schema';
import { trimPostgresQueueEvents } from './telemetry';
import type { PostgresContext } from './context';
import type { PostgresStoreEvent } from './types';

type EventListener = (event: PostgresStoreEvent) => void;
type InvalidationListener = (queue: string) => void;
type DrainStatusListener = (error: Error | null) => void;
type WakeResolver = () => void;

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
  private readonly queueEventCursors = new Map<string, number>();
  private readonly retentionQueues = new Set<string>();
  private journalCursor: PostgresEventJournalCursor = { commitSeq: 0, eventId: 0 };
  private cursor = 0;
  private baselineCursor = 0;
  private checkpointScanRequested = false;
  private drainRequested = false;
  private draining: Promise<void> | null = null;
  private closed = false;

  constructor(private readonly ctx: PostgresContext) {}

  async start(): Promise<void> {
    const [baseline, watermarks] = await Promise.all([
      latestPostgresEventJournalBaseline(this.ctx),
      loadPostgresEventPruneWatermarks(this.ctx),
    ]);
    this.cursor = baseline.latestEventId;
    this.baselineCursor = baseline.commitSeq;
    this.journalCursor = { commitSeq: baseline.commitSeq, eventId: Number.MAX_SAFE_INTEGER };
    for (const watermark of watermarks) {
      if (watermark.commitSeq > baseline.commitSeq) {
        this.queueEventCursors.set(watermark.queue, watermark.commitSeq);
      }
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
        if (!this.closed) this.reportDrainStatus(null);
      })
      .catch((error: unknown) =>
        this.reportDrainStatus(error instanceof Error ? error : new Error(String(error)))
      )
      .finally(() => {
        this.draining = null;
        if (this.drainRequested && !this.closed) void this.drain(false);
      });
    return this.draining;
  }

  private async drainLoop(): Promise<void> {
    while (!this.closed) {
      this.drainRequested = false;
      await this.convergeEventRetention();
      await this.scanPruneWatermarks();
      const entries = await loadPostgresJournalEventsAfter(this.ctx, this.journalCursor);
      await this.scanPruneWatermarks();
      for (const entry of entries) this.applyEvent(entry);
      if (entries.length >= 1000 || this.drainRequested) continue;
      return;
    }
  }

  private async convergeEventRetention(): Promise<void> {
    const queues = [...this.retentionQueues].sort();
    for (const queue of queues) {
      this.retentionQueues.delete(queue);
      try {
        await trimPostgresQueueEvents(this.ctx, queue, this.ctx.config.maxQueueEvents);
      } catch (error) {
        this.retentionQueues.add(queue);
        throw error;
      }
    }
  }

  private applyEvent(entry: PostgresJournalEvent): void {
    const { event, commitSeq } = entry;
    this.journalCursor = { commitSeq, eventId: event.id };
    this.cursor = Math.max(this.cursor, event.id);
    this.queueEventCursors.set(
      event.queue,
      Math.max(this.queueEventCursors.get(event.queue) ?? this.baselineCursor, commitSeq)
    );
    if (requiresQueueInvalidation(event)) this.invalidateQueue(event.queue);
    for (const listener of this.listeners) {
      try {
        listener(event);
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

  private applyPruneWatermark(queue: string, commitSeq: number, prunedCommitSeq: number): void {
    const appliedThrough = this.queueEventCursors.get(queue) ?? this.baselineCursor;
    if (appliedThrough >= prunedCommitSeq) return;
    this.queueEventCursors.set(queue, commitSeq);
    this.invalidateQueue(queue);
  }

  private async scanPruneWatermarks(): Promise<void> {
    if (!this.checkpointScanRequested) return;
    this.checkpointScanRequested = false;
    try {
      const watermarks = await loadPostgresEventPruneWatermarks(this.ctx);
      for (const watermark of watermarks) {
        this.applyPruneWatermark(watermark.queue, watermark.commitSeq, watermark.prunedCommitSeq);
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
