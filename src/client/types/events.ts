import type { ConnectionOptions } from './connection';

export interface QueueEventsOptions {
  /** Select the embedded manager instead of the TCP broker. */
  embedded?: boolean;
  /** TCP connection used when embedded is false. */
  connection?: ConnectionOptions;
  /** Embedded SQLite path; must match an already-active shared manager. */
  dataPath?: string;
  /** Namespace prefix applied to the queue name. */
  prefixKey?: string;
}

export interface WaitingEvent {
  jobId: string;
}

export interface ActiveEvent {
  jobId: string;
}

export interface CompletedEvent<R = unknown> {
  jobId: string;
  returnvalue: R;
}

export interface FailedEvent {
  jobId: string;
  failedReason: string;
  data?: unknown;
}

export interface ProgressEvent<P = unknown> {
  jobId: string;
  data: P;
}

export interface StalledEvent {
  jobId: string;
}

export interface RemovedEvent {
  jobId: string;
  prev: string;
}

export interface DelayedEvent {
  jobId: string;
  delay: number;
}

export interface DuplicatedEvent {
  jobId: string;
}

export interface RetriedEvent {
  jobId: string;
  prev: string;
}

export interface WaitingChildrenEvent {
  jobId: string;
}

export interface DrainedEvent {
  id: string;
}
