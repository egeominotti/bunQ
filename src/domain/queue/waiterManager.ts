/**
 * WaiterManager - Manages queue-scoped job availability notifications.
 * Handles worker polling with timeout-based waiting.
 */

const DEFAULT_QUEUE = '';
const COMPACT_MIN_HEAD = 1024;

interface Waiter {
  resolve: () => void;
  timer: ReturnType<typeof setTimeout>;
  cancelled: boolean;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface QueueWaiters {
  entries: Waiter[];
  head: number;
  active: number;
  pending: boolean;
}

export class WaiterManager {
  private readonly queues = new Map<string, QueueWaiters>();
  private activeWaiters = 0;

  notify(queue?: string): void {
    this.notifyQueue(queue ?? DEFAULT_QUEUE, 1);
  }

  notifyBatch(count: number): void;
  notifyBatch(queue: string, count: number): void;
  notifyBatch(queueOrCount: string | number, maybeCount?: number): void {
    const queue = typeof queueOrCount === 'string' ? queueOrCount : DEFAULT_QUEUE;
    const count = typeof queueOrCount === 'number' ? queueOrCount : (maybeCount ?? 0);
    if (count <= 0) return;
    this.notifyQueue(queue, count);
  }

  waitForJob(timeoutMs: number, signal?: AbortSignal): Promise<void>;
  waitForJob(queue: string, timeoutMs: number, signal?: AbortSignal): Promise<void>;
  waitForJob(
    queueOrTimeout: string | number,
    maybeTimeoutOrSignal?: number | AbortSignal,
    maybeSignal?: AbortSignal
  ): Promise<void> {
    const queue = typeof queueOrTimeout === 'string' ? queueOrTimeout : DEFAULT_QUEUE;
    const timeoutMs =
      typeof queueOrTimeout === 'number'
        ? queueOrTimeout
        : typeof maybeTimeoutOrSignal === 'number'
          ? maybeTimeoutOrSignal
          : 0;
    const signal =
      typeof queueOrTimeout === 'number'
        ? (maybeTimeoutOrSignal as AbortSignal | undefined)
        : maybeSignal;
    if (timeoutMs <= 0 || signal?.aborted) return Promise.resolve();

    const state = this.queues.get(queue);
    if (state?.pending) {
      state.pending = false;
      this.deleteIdleQueue(queue, state);
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      const queueState = state ?? this.createQueue(queue);
      const waiter = {
        resolve,
        cancelled: false,
        timer: undefined as unknown as ReturnType<typeof setTimeout>,
        signal,
        onAbort: undefined as (() => void) | undefined,
      };
      const settle = () => {
        if (waiter.cancelled) return;
        waiter.cancelled = true;
        clearTimeout(waiter.timer);
        if (waiter.signal && waiter.onAbort) {
          waiter.signal.removeEventListener('abort', waiter.onAbort);
        }
        queueState.active--;
        this.activeWaiters--;
        resolve();
        this.compact(queue, queueState);
      };
      waiter.timer = setTimeout(settle, timeoutMs);
      waiter.onAbort = settle;
      signal?.addEventListener('abort', settle, { once: true });
      queueState.entries.push(waiter);
      queueState.active++;
      this.activeWaiters++;
      if (signal?.aborted) settle();
    });
  }

  private notifyQueue(queue: string, count: number): void {
    const state = this.queues.get(queue);
    if (!state) {
      this.createQueue(queue).pending = true;
      return;
    }

    let remaining = count;
    while (remaining > 0 && state.active > 0) {
      const waiter = state.entries[state.head++];
      if (!waiter || waiter.cancelled) continue;
      waiter.cancelled = true;
      clearTimeout(waiter.timer);
      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener('abort', waiter.onAbort);
      }
      state.active--;
      this.activeWaiters--;
      remaining--;
      waiter.resolve();
    }

    // Availability is edge-triggered: surplus notifications coalesce into one
    // retry hint instead of accumulating an unbounded notification debt.
    if (remaining > 0) state.pending = true;
    this.compact(queue, state);
  }

  private createQueue(queue: string): QueueWaiters {
    const state = { entries: [], head: 0, active: 0, pending: false };
    this.queues.set(queue, state);
    return state;
  }

  private compact(queue: string, state: QueueWaiters): void {
    if (state.active === 0) {
      state.entries = [];
      state.head = 0;
      this.deleteIdleQueue(queue, state);
      return;
    }
    if (state.head >= COMPACT_MIN_HEAD && state.head * 2 >= state.entries.length) {
      state.entries = state.entries.slice(state.head);
      state.head = 0;
    }
  }

  private deleteIdleQueue(queue: string, state: QueueWaiters): void {
    if (state.active === 0 && !state.pending) this.queues.delete(queue);
  }

  get length(): number {
    return this.activeWaiters;
  }
}
