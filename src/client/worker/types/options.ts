import type { ConnectionOptions, GroupWorkerOptions } from '../../types';

export interface ExtendedWorkerOptions {
  concurrency: number;
  autorun: boolean;
  heartbeatInterval: number;
  batchSize: number;
  pollTimeout: number;
  embedded: boolean;
  useLocks: boolean;
  skipLockRenewal: boolean;
  skipStalledCheck: boolean;
  drainDelay: number;
  lockDuration: number;
  maxStalledCount: number;
  removeOnComplete?: boolean | number | { age?: number; count?: number };
  removeOnFail?: boolean | number | { age?: number; count?: number };
  connection?: ConnectionOptions;
  group?: GroupWorkerOptions;
}
