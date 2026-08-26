import type { Job, JobId } from '../../domain/types/job';
import type { PostgresJobState } from '../../infrastructure/persistence/postgres';
import type { PostgresQueueSnapshot } from './snapshot';

interface BatchCompletionItem {
  readonly id: JobId;
  readonly result?: unknown;
}

interface BatchCompletionTransition {
  readonly applied: boolean;
  readonly job: Job | null;
  readonly state: PostgresJobState | null;
  readonly removed: boolean;
}

/** Apply an authoritative batch result without reloading every job in its queue. */
export function applyPostgresBatchCompletion(
  snapshot: PostgresQueueSnapshot,
  item: BatchCompletionItem,
  transition: BatchCompletionTransition
): string | null {
  if (!transition.applied || !transition.job || transition.state !== 'completed') return null;
  snapshot.apply({
    id: 0,
    queue: transition.job.queue,
    type: 'completed',
    jobId: item.id,
    occurredAt: transition.job.completedAt ?? Date.now(),
    job: transition.job,
    state: 'completed',
    result: item.result,
    removed: transition.removed,
  });
  return transition.job.repeat === null ? null : transition.job.queue;
}
