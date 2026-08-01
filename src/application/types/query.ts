import type { Shard } from '../../domain/queue/shard';
import type { JobLocation } from '../../domain/types/queue';
import type { Job, JobId } from '../../domain/types/job';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import type { MapLike, SetLike } from '../../shared/lru';
import type { DependencyResultTracker } from '../dependencyResultTracker';

export interface QueryContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  jobResults: MapLike<JobId, unknown>;
  dependencyResults: DependencyResultTracker;
  customIdMap: MapLike<string, JobId>;
}

export interface GetJobsContext extends QueryContext {
  shardCount: number;
}
