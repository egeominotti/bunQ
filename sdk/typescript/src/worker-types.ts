/** Worker option types and shared constants. */

import type { TlsOption } from './connection.js';
import type { Job } from './job.js';

export type Processor<T = unknown, R = unknown> = (job: Job<T>) => R | Promise<R>;

export interface WorkerOptions {
  host?: string;
  port?: number;
  token?: string;
  tls?: TlsOption;
  /** Max jobs processed in parallel (default 4). */
  concurrency?: number;
  /** Max jobs fetched per PULLB (default 10, capped by free slots). */
  batchSize?: number;
  /** Server-side long-poll timeout in ms (default 5000, max 30000). */
  pollTimeoutMs?: number;
  /** Job lock TTL in ms (default 30000). */
  lockTtlMs?: number;
  /** Worker + job heartbeat interval in seconds (default 10). */
  heartbeatIntervalS?: number;
  /** Start the loop at construction (default true, mirrors the TS client). */
  autorun?: boolean;
  name?: string;
}

export const MAX_POLL_TIMEOUT_MS = 30_000;
export const MAX_STACK_LINES = 20;
export const RECONNECT_BACKOFF_MS = [500, 1000, 2000, 5000];

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
