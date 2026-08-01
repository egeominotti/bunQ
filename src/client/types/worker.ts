import type { ConnectionOptions } from './connection';

export interface RateLimiterOptions {
  max: number;
  duration: number;
  groupKey?: string;
}

export interface WorkerOptions {
  concurrency?: number;
  autorun?: boolean;
  heartbeatInterval?: number;
  connection?: ConnectionOptions;
  embedded?: boolean;
  dataPath?: string;
  batchSize?: number;
  pollTimeout?: number;
  useLocks?: boolean;
  limiter?: RateLimiterOptions;
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
