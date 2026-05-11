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

/** Waiter entry with cancellation flag for O(1) cleanup */
interface CompletionWaiter {
  resolve: () => void;
  cancelled: boolean;
}

/** Events manager class */
export class EventsManager {
  /** Use Set for O(1) subscribe/unsubscribe instead of indexOf+splice */
  private readonly subscribers = new Set<EventSubscriber>();
  /** Waiters for specific job completions - for efficient WaitJob implementation */
  private readonly completionWaiters = new Map<string, CompletionWaiter[]>();

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
  subscribe(callback: EventSubscriber): () => void {
    this.subscribers.add(callback);
    return () => {
      this.subscribers.delete(callback); // O(1) instead of O(n)
    };
  }

  /** Clear all subscribers (for shutdown) */
  clear(): void {
    this.subscribers.clear();
    // Clear all waiters
    for (const waiters of this.completionWaiters.values()) {
      for (const waiter of waiters) {
        if (!waiter.cancelled) {
          waiter.resolve();
        }
      }
    }
    this.completionWaiters.clear();
  }

  /**
   * Wait for a specific job to complete - event-driven, no polling
   * Returns true if job completed, false if timeout
   */
  waitForJobCompletion(jobId: JobId, timeoutMs: number): Promise<boolean> {
    const jobKey = String(jobId);

    return new Promise((resolve) => {
      const waiter: CompletionWaiter = {
        resolve: () => {
          clearTimeout(timer);
          resolve(true);
        },
        cancelled: false,
      };

      const timer = setTimeout(() => {
        // Timeout - mark as cancelled and remove from map to prevent memory leak
        waiter.cancelled = true;

        // Remove the waiter from the array
        const waiters = this.completionWaiters.get(jobKey);
        if (waiters) {
          const index = waiters.indexOf(waiter);
          if (index !== -1) {
            waiters.splice(index, 1);
          }
          // Clean up empty arrays to free memory
          if (waiters.length === 0) {
            this.completionWaiters.delete(jobKey);
          }
        }

        resolve(false);
      }, timeoutMs);

      // Add to waiters
      let waiters = this.completionWaiters.get(jobKey);
      if (!waiters) {
        waiters = [];
        this.completionWaiters.set(jobKey, waiters);
      }
      waiters.push(waiter);
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

    // Notify completion waiters for WaitJob - O(1) lookup
    if (hasWaiters) {
      const jobKey = String(event.jobId);
      const waiters = this.completionWaiters.get(jobKey);
      if (waiters) {
        this.completionWaiters.delete(jobKey);
        // Only notify non-cancelled waiters
        for (const waiter of waiters) {
          if (!waiter.cancelled) {
            waiter.resolve();
          }
        }
      }
    }

    // Trigger webhooks - only if there are enabled webhooks
    if (hasWebhooks) {
      const webhookEvent = this.mapEventToWebhook(event.eventType);
      if (webhookEvent) {
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
    }
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
