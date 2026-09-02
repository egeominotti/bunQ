import type { Job, JobId, JobTimelineEntry } from '../../../domain/types/job';

export interface ActiveJobWrite {
  jobId: JobId;
  startedAt: number;
  timeline?: JobTimelineEntry[];
}

export interface CompletedJobWrite {
  jobId: JobId;
  completedAt: number;
  timeline?: JobTimelineEntry[];
}

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
