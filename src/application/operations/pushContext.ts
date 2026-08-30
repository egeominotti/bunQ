import type { Shard } from '../../domain/queue/shard';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import { type EventType, type JobEvent } from '../../domain/types/queue';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import type { MapLike, SetLike } from '../../shared/lru';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import type { RetiredTimeoutGeneration } from '../types/background';

/** Shared mutable state required by queue and flow insertion operations. */
export interface PushContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  customIdLock: RWLock;
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
  timedOutJobs?: MapLike<JobId, RetiredTimeoutGeneration>;
  retiredTimeoutLeaseTokens?: MapLike<string, RetiredTimeoutGeneration>;
  retiredCronLeaseTokens?: MapLike<JobId, string>;
  jobResults: MapLike<JobId, unknown>;
  dependencyResults: DependencyResultTracker;
  customIdMap: MapLike<string, JobId>;
  jobIndex: Map<JobId, JobLocation>;
  totalPushed: { value: bigint };
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
  }) => void;
  broadcastBatch?: (events: readonly JobEvent[]) => void;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
  registerQueueName?: (queue: string) => void;
}
