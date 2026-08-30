/**
 * Events Manager
 * Job event subscription and broadcasting
 */

import type { JobId } from '../domain/types/job';
import { EventType, type JobEvent } from '../domain/types/queue';
import type { WebhookManager } from './webhookManager';
import type { WebhookEvent } from '../domain/types/webhook';
import { webhookLog } from '../shared/logger';

/** Event subscriber callback */
export type EventSubscriber = (event: JobEvent) => void;
export type EventBatchSubscriber = (events: readonly JobEvent[]) => void;

/** Waiter entry with cancellation flag for O(1) cleanup */
interface CompletionWaiter {
  resolve: () => void;
  cancel: () => void;
  cancelled: boolean;
}

/** Events manager class */
export class EventsManager {
  /** Use Set for O(1) subscribe/unsubscribe instead of indexOf+splice */
  private readonly subscribers = new Set<EventSubscriber>();
  /** Optional batch path for internal subscribers such as durable telemetry. */
  private readonly batchSubscribers = new Map<EventSubscriber, EventBatchSubscriber>();
  /** Waiters for specific job completions - for efficient WaitJob implementation */
  private readonly completionWaiters = new Map<string, Set<CompletionWaiter>>();

  constructor(private readonly webhookManager: WebhookManager) {}

  /** Number of active event subscribers */
  get subscriberCount(): number {
    return this.subscribers.size;
  }

  /** Number of jobs with active completion waiters */
  get completionWaiterCount(): number {
    return this.completionWaiters.size;
  }

  /** Subscribe to job events - O(1) add and remove */
  subscribe(callback: EventSubscriber, batchCallback?: EventBatchSubscriber): () => void {
    this.subscribers.add(callback);
    if (batchCallback) this.batchSubscribers.set(callback, batchCallback);
    return () => {
      this.subscribers.delete(callback); // O(1) instead of O(n)
      this.batchSubscribers.delete(callback);
    };
  }

  /** Clear all subscribers (for shutdown) */
  clear(): void {
    this.subscribers.clear();
    this.batchSubscribers.clear();
    // Clear all waiters
    for (const waiters of this.completionWaiters.values()) {
      for (const waiter of waiters) {
        if (!waiter.cancelled) waiter.resolve();
      }
    }
    this.completionWaiters.clear();
  }

  /**
   * Wait for a specific job to complete - event-driven, no polling
   * Returns true if job completed, false if timeout
   */
  waitForJobCompletion(jobId: JobId, timeoutMs: number, signal?: AbortSignal): Promise<boolean> {
    const jobKey = String(jobId);
    if (signal?.aborted) return Promise.resolve(false);

    return new Promise((resolve) => {
      let settled = false;
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
        const waiters = this.completionWaiters.get(jobKey);
        waiters?.delete(waiter);
        if (waiters?.size === 0) this.completionWaiters.delete(jobKey);
      };
      const finish = (completed: boolean) => {
        if (settled) return;
        settled = true;
        waiter.cancelled = !completed;
        cleanup();
        resolve(completed);
      };
      const onAbort = () => finish(false);
      const waiter: CompletionWaiter = {
        resolve: () => finish(true),
        cancel: () => finish(false),
        cancelled: false,
      };

      // Add to waiters
      let waiters = this.completionWaiters.get(jobKey);
      if (!waiters) {
        waiters = new Set();
        this.completionWaiters.set(jobKey, waiters);
      }
      waiters.add(waiter);
      const timer = setTimeout(() => finish(false), timeoutMs);
      signal?.addEventListener('abort', onAbort, { once: true });
      if (signal?.aborted) onAbort();
    });
  }

  /** Check if broadcast has any listeners - for batch optimizations */
  needsBroadcast(): boolean {
    return (
      this.subscribers.size > 0 ||
      this.webhookManager.hasEnabledWebhooks() ||
      this.completionWaiters.size > 0
    );
  }

  /** Broadcast event to all subscribers - optimized to skip work when no listeners */
  broadcast(
    event: Partial<JobEvent> & {
      eventType: EventType;
      queue: string;
      jobId: JobId;
      timestamp: number;
      error?: string;
    }
  ): void {
    const hasSubscribers = this.subscribers.size > 0;
    const hasWebhooks = this.webhookManager.hasEnabledWebhooks();
    const isCompletion = event.eventType === EventType.Completed;
    const hasWaiters = isCompletion && this.completionWaiters.size > 0;

    // Fast path: nothing to notify
    if (!hasSubscribers && !hasWebhooks && !hasWaiters) {
      return;
    }

    // Notify subscribers
    if (hasSubscribers) {
      for (const sub of this.subscribers) {
        try {
          sub(event);
        } catch {
          // Ignore subscriber errors
        }
      }
    }

    if (hasWaiters) this.notifyCompletionWaiters(event);
    if (hasWebhooks) this.triggerWebhook(event);
  }

  /** Broadcast an ordered batch while allowing internal subscribers to persist once. */
  broadcastBatch(events: readonly JobEvent[]): void {
    if (events.length === 0) return;
    const hasSubscribers = this.subscribers.size > 0;
    const hasWebhooks = this.webhookManager.hasEnabledWebhooks();
    const hasWaiters = this.completionWaiters.size > 0;
    if (!hasSubscribers && !hasWebhooks && !hasWaiters) return;

    if (hasSubscribers) {
      for (const [subscriber, batchSubscriber] of this.batchSubscribers) {
        if (!this.subscribers.has(subscriber)) continue;
        try {
          batchSubscriber(events);
        } catch {
          // Ignore subscriber errors.
        }
      }
    }

    for (const event of events) {
      if (hasSubscribers) {
        for (const subscriber of this.subscribers) {
          if (this.batchSubscribers.has(subscriber)) continue;
          try {
            subscriber(event);
          } catch {
            // Ignore subscriber errors and continue with the ordered batch.
          }
        }
      }
      if (hasWaiters && event.eventType === EventType.Completed) {
        this.notifyCompletionWaiters(event);
      }
      if (hasWebhooks) this.triggerWebhook(event);
    }
  }

  private notifyCompletionWaiters(event: JobEvent): void {
    const jobKey = String(event.jobId);
    const waiters = this.completionWaiters.get(jobKey);
    if (!waiters) return;
    this.completionWaiters.delete(jobKey);
    for (const waiter of waiters) {
      if (!waiter.cancelled) waiter.resolve();
    }
  }

  private triggerWebhook(event: JobEvent): void {
    const webhookEvent = this.mapEventToWebhook(event.eventType);
    if (!webhookEvent) return;
    this.webhookManager
      .trigger(webhookEvent, String(event.jobId), event.queue, {
        data: event.data,
        error: event.error,
      })
      .catch((err: unknown) => {
        webhookLog.error('Webhook trigger failed', {
          event: webhookEvent,
          jobId: String(event.jobId),
          queue: event.queue,
          error: String(err),
        });
      });
  }

  /** Map internal event type to webhook event */
  private mapEventToWebhook(eventType: EventType): WebhookEvent | null {
    switch (eventType) {
      case EventType.Pushed:
        return 'job.pushed';
      case EventType.Pulled:
        return 'job.started';
      case EventType.Completed:
        return 'job.completed';
      case EventType.Failed:
        return 'job.failed';
      case EventType.Progress:
      case EventType.Stalled:
      case EventType.Removed:
      case EventType.Delayed:
      case EventType.Duplicated:
      case EventType.Retried:
      case EventType.WaitingChildren:
      case EventType.Drained:
      case EventType.Paused:
      case EventType.Resumed:
        // These events don't have webhook mappings yet
        return null;
    }
  }
}
