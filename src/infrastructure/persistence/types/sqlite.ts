import type { Job } from '../../../domain/types/job';

export type SqliteCriticalLossCallback = (jobs: Job[], lastError: Error, attempts: number) => void;

export interface SqliteCriticalLoss {
  jobs: Job[];
  error: string;
  attempts: number;
  at: number;
}

export interface SqliteConfig {
  path: string;
  walMode?: boolean;
  synchronous?: 'OFF' | 'NORMAL' | 'FULL';
  cacheSize?: number;
  writeBufferSize?: number;
  writeBufferFlushMs?: number;
  onCriticalLoss?: SqliteCriticalLossCallback;
}
