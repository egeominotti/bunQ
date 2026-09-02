import type { Shard } from '../../domain/queue/shard';
import type { FlowFailureRecord } from '../../domain/types/flow';
import type { FailureReason } from '../../domain/types/dlq';
import type { Job, JobId } from '../../domain/types/job';
import type { EventType, JobEvent, JobLocation } from '../../domain/types/queue';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import type { MapLike, SetLike } from '../../shared/lru';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { DependencyResultTracker } from '../dependencyResultTracker';

export interface AckContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  retiredCronLeaseTokens?: MapLike<JobId, string>;
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
  jobResults: MapLike<JobId, unknown>;
  jobResultQueues: Map<JobId, string>;
  dependencyResults: DependencyResultTracker;
  jobIndex: Map<JobId, JobLocation>;
  customIdMap?: MapLike<string, JobId>;
  totalCompleted: { value: bigint };
  totalFailed: { value: bigint };
  perQueueMetrics?: MapLike<string, { totalCompleted: bigint; totalFailed: bigint }>;
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
    data?: unknown;
    error?: string;
    prev?: string;
    terminal?: boolean;
  }) => void;
  broadcastBatch?: (events: readonly JobEvent[]) => void;
  onJobCompleted: (jobId: JobId) => void;
  onJobFailed?: (jobId: JobId) => void;
  onJobsCompleted?: (jobIds: JobId[]) => void;
  needsBroadcast?: () => boolean;
  emitDashboardEvent?: (event: string, data: Record<string, unknown>) => void;
  hasPendingDeps?: () => boolean;
  onRepeat?: (job: Job) => void;
  onChildTerminalFailure?: (childJob: Job, error: string | undefined) => Promise<void>;
  onChildDependencyOption?: (childJob: Job, error: string | undefined) => Promise<void>;
}

export interface ExtractedJob<T = unknown> {
  id: JobId;
  job: Job;
  result?: T;
  removeOnComplete?: boolean;
}

export interface CompletionOptions {
  removeOnComplete?: boolean;
  /** Internal lease identity used only to match a retired cron generation. */
  leaseToken?: string;
}

export interface AckAlreadyFinalized {
  readonly applied: false;
  readonly reason: 'already-finalized';
}

/** Normal ACKs preserve the historical undefined return value. */
export type AckOutcome = undefined | AckAlreadyFinalized;

export interface AckBatchIgnored {
  readonly ignoredIds: JobId[];
  /** Input positions preserve generation identity when a job ID is repeated. */
  readonly ignoredIndices: number[];
}

export type AckBatchOutcome = undefined | AckBatchIgnored;

export interface BatchContext {
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  shards: Shard[];
  shardLocks: RWLock[];
}

export interface FinalizeContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
  jobResults: MapLike<JobId, unknown>;
  jobResultQueues: Map<JobId, string>;
  dependencyResults?: DependencyResultTracker;
  jobIndex: Map<JobId, JobLocation>;
  customIdMap?: MapLike<string, JobId>;
  totalCompleted: { value: bigint };
  perQueueMetrics?: MapLike<string, { totalCompleted: bigint; totalFailed: bigint }>;
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
    data?: unknown;
  }) => void;
  broadcastBatch?: (events: readonly JobEvent[]) => void;
  onJobCompleted: (jobId: JobId) => void;
  onJobsCompleted?: (jobIds: JobId[]) => void;
  needsBroadcast?: () => boolean;
  hasPendingDeps?: () => boolean;
  onRepeat?: (job: Job) => void;
}

export interface TerminalFailureInput {
  job: Job;
  jobId: JobId;
  error: string | undefined;
  shard: Shard;
  ctx: AckContext;
  flowFailure: FlowFailureRecord | null;
  reason: FailureReason;
}

export interface FailJobOptions {
  unrecoverable?: boolean;
  stack?: string[];
  failureReason?: FailureReason;
  removeOnFail?: boolean;
  /** Runs synchronously while the exact processing generation is claimed. */
  onClaim?: (job: Job) => void;
}
