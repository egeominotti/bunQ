import type { ConnectionOptions } from './connection';

export interface RateLimiterOptions {
  max: number;
  duration: number;
  groupKey?: string;
}

export interface GroupWorkerOptions {
  concurrency?: number;
  limit?: { max: number; duration: number };
}

export interface WorkerOptions {
  concurrency?: number;
  autorun?: boolean;
  heartbeatInterval?: number;
  connection?: ConnectionOptions;
  embedded?: boolean;
  /** Must match the process-wide embedded manager when one is already active. */
  dataPath?: string;
  batchSize?: number;
  pollTimeout?: number;
  useLocks?: boolean;
  limiter?: RateLimiterOptions;
  group?: GroupWorkerOptions;
  lockDuration?: number;
  maxStalledCount?: number;
  skipStalledCheck?: boolean;
  skipLockRenewal?: boolean;
  drainDelay?: number;
  removeOnComplete?: boolean | number | { age?: number; count?: number };
  removeOnFail?: boolean | number | { age?: number; count?: number };
  prefixKey?: string;
}

export interface StallConfig {
  enabled?: boolean;
  stallInterval?: number;
  maxStalls?: number;
  gracePeriod?: number;
}
