/**
 * Queue Internal Types
 * Shared types for queue module operations
 */

import type { TcpConnectionPool } from '../../tcpPool';
import type { JobStateType, QueueOptions } from '../../types';

/** Internal queue context for operations */
export interface QueueContext {
  name: string;
  opts: QueueOptions;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
}

/** Job creation context for proxy */
export interface JobCreationContext {
  queueName: string;
  tcp: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<string>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
}

/** TCP response with optional data */
export interface TcpResponse {
  ok: boolean;
  error?: string;
  data?: unknown;
  [key: string]: unknown;
}
