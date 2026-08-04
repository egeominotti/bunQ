import type { ClientTlsOptions } from '../tcp/types';
import type { JobOptions } from './options';

export type { ClientTlsOptions } from '../tcp/types';

export interface ConnectionOptions {
  host?: string;
  port?: number;
  socketPath?: string;
  tls?: boolean | ClientTlsOptions;
  token?: string;
  poolSize?: number;
  pingInterval?: number;
  commandTimeout?: number;
  maxCommandTimeouts?: number;
  pipelining?: boolean;
  maxInFlight?: number;
}

export interface AutoBatchOptions {
  enabled?: boolean;
  maxSize?: number;
  maxDelayMs?: number;
}

export interface QueueOptions {
  defaultJobOptions?: JobOptions;
  connection?: ConnectionOptions;
  embedded?: boolean;
  /** Must match the process-wide embedded manager when one is already active. */
  dataPath?: string;
  autoBatch?: AutoBatchOptions;
  prefixKey?: string;
}
