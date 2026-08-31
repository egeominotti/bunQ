import type { Job } from './job';

export interface FlowJobData {
  __flowParentId?: string;
  __flowParentIds?: string[];
  __parentId?: string;
  __parentQueue?: string;
  __childrenIds?: string[];
}

export interface ProcessorContext {
  signal: AbortSignal;
}

export interface ObservableLike<T> {
  subscribe(observer: {
    next(value: T): void;
    error(error: unknown): void;
    complete(): void;
  }): { unsubscribe(): void } | (() => void) | undefined;
}

export type Processor<T = unknown, R = unknown> = (
  job: Job<T & FlowJobData>,
  context?: ProcessorContext
) => Promise<R> | ObservableLike<R> | R;

export type QueueEventType =
  | 'waiting'
  | 'active'
  | 'completed'
  | 'failed'
  | 'progress'
  | 'removed'
  | 'drained';
