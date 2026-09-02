import type { Shard } from '../../domain/queue/shard';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { JobLogEntry } from '../../domain/types/worker';
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
  jobResultQueues: Map<JobId, string>;
  jobLogs: MapLike<JobId, JobLogEntry[]>;
  jobLogQueues: Map<JobId, string>;
  dependencyResults: DependencyResultTracker;
  customIdMap: MapLike<string, JobId>;
  jobIndex: Map<JobId, JobLocation>;
  pendingQueueAdmissions: Map<string, number>;
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
  onQueueAdmissionsDrained?: (queue: string) => void;
}

/** Keep every target queue owned while an asynchronous admission is in flight. */
export async function withPendingQueueAdmissions<T>(
  queues: Iterable<string>,
  ctx: Pick<
    PushContext,
    'pendingQueueAdmissions' | 'registerQueueName' | 'onQueueAdmissionsDrained'
  >,
  operation: () => Promise<T>
): Promise<T> {
  const uniqueQueues = [...new Set(queues)];
  for (const queue of uniqueQueues) {
    ctx.pendingQueueAdmissions.set(queue, (ctx.pendingQueueAdmissions.get(queue) ?? 0) + 1);
  }

  try {
    for (const queue of uniqueQueues) ctx.registerQueueName?.(queue);
    return await operation();
  } finally {
    for (const queue of uniqueQueues) {
      const count = ctx.pendingQueueAdmissions.get(queue);
      if (count === undefined || count <= 1) {
        ctx.pendingQueueAdmissions.delete(queue);
        ctx.onQueueAdmissionsDrained?.(queue);
      } else {
        ctx.pendingQueueAdmissions.set(queue, count - 1);
      }
    }
  }
}
