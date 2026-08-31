import { reconcileDependencyCompletionPins } from '../../dependencyCompletions';
import type { BackgroundContext } from '../../types';
import { recoverActiveJobs, restoreGroupPolicies, restoreRecoveryPolicies } from './active';
import { recoverPendingJobs } from './pending';
import { recoverCompletedJobs, restoreDlq, restoreQueueState } from './restore';

export function recover(ctx: BackgroundContext): void {
  if (!ctx.storage) return;

  const dependencyCompletions = ctx.storage.loadDependencyCompletions();
  const completedInDatabase = ctx.storage.loadCompletedJobIds();
  for (const record of dependencyCompletions) completedInDatabase.add(record.jobId);
  const dlqJobIds = ctx.storage.loadDlqJobIds();
  const queueStates = ctx.storage.loadQueueState();
  const groupStates = ctx.storage.loadGroupState();

  restoreRecoveryPolicies(ctx, queueStates);
  restoreGroupPolicies(ctx, groupStates);
  const now = Date.now();
  recoverActiveJobs(ctx, dlqJobIds, now);
  recoverPendingJobs(ctx, completedInDatabase, now);
  restoreDlq(ctx);
  restoreQueueState(ctx, queueStates);
  recoverCompletedJobs(ctx);
  reconcileDependencyCompletionPins(ctx);
}
