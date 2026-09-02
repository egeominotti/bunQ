import { reconcileDependencyCompletionPins } from '../../dependencyCompletions';
import type { BackgroundContext } from '../../types';
import { storageLog } from '../../../shared/logger';
import { recoverActiveJobs, restoreGroupPolicies, restoreRecoveryPolicies } from './active';
import { recoverPendingJobs } from './pending';
import { recoverCompletedJobs, restoreDlq, restoreQueueState } from './restore';

export function recover(ctx: BackgroundContext): void {
  if (!ctx.storage) return;

  const startedAt = Date.now();
  storageLog.debugToStderr('Starting SQLite recovery');
  const dependencyCompletions = ctx.storage.loadDependencyCompletions();
  const completionProofs = new Set(dependencyCompletions.map((record) => record.jobId));
  const dlqJobIds = ctx.storage.loadDlqJobIds();
  const queueStates = ctx.storage.loadQueueState();
  const groupStates = ctx.storage.loadGroupState();
  for (const queue of ctx.storage.loadCompletedQueueNames()) ctx.registerQueueName(queue);
  storageLog.debugToStderr('SQLite recovery progress', {
    phase: 'metadata',
    durationMs: Date.now() - startedAt,
    completionProofs: completionProofs.size,
    dlqJobs: dlqJobIds.size,
  });

  restoreRecoveryPolicies(ctx, queueStates);
  restoreGroupPolicies(ctx, groupStates);
  const now = Date.now();
  recoverActiveJobs(ctx, dlqJobIds, now);
  storageLog.debugToStderr('SQLite recovery progress', {
    phase: 'active',
    durationMs: Date.now() - startedAt,
  });
  recoverPendingJobs(ctx, completionProofs, now);
  storageLog.debugToStderr('SQLite recovery progress', {
    phase: 'pending',
    durationMs: Date.now() - startedAt,
  });
  restoreDlq(ctx);
  restoreQueueState(ctx, queueStates);
  const completedBefore = ctx.completedJobs.size;
  recoverCompletedJobs(ctx);
  storageLog.debugToStderr('SQLite recovery progress', {
    phase: 'completed-hot-cache',
    durationMs: Date.now() - startedAt,
    loaded: ctx.completedJobs.size - completedBefore,
  });
  reconcileDependencyCompletionPins(ctx);
  storageLog.debugToStderr('Completed SQLite recovery', {
    durationMs: Date.now() - startedAt,
    indexedJobs: ctx.jobIndex.size,
    queues: ctx.queueNamesCache.size,
  });
}
