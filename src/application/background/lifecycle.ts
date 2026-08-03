import type { CronScheduler } from '../../infrastructure/scheduler/cronScheduler';
import { cleanup } from '../cleanupTasks';
import { processPendingDependencies } from '../dependencyProcessor';
import { checkExpiredLocks } from '../lockManager';
import { runMonitoringChecks } from '../monitoringChecks';
import { checkStalledJobs } from '../stallDetection';
import { handleTaskError, handleTaskSuccess } from '../taskErrorTracking';
import type { BackgroundContext, BackgroundTaskHandles, LockContext } from '../types';
import { performDlqMaintenance } from './dlq';

function getLockContext(ctx: BackgroundContext): LockContext {
  return {
    jobIndex: ctx.jobIndex,
    jobLocks: ctx.jobLocks,
    retiredCronLeaseTokens: ctx.retiredCronLeaseTokens,
    clientJobs: ctx.clientJobs,
    processingShards: ctx.processingShards,
    processingLocks: ctx.processingLocks,
    shards: ctx.shards,
    shardLocks: ctx.shardLocks,
    eventsManager: ctx.eventsManager,
    dashboardEmit: ctx.dashboardEmit,
    storage: ctx.storage,
    timeoutScheduler: ctx.timeoutScheduler,
  };
}

export function startBackgroundTasks(
  ctx: BackgroundContext,
  cronScheduler: CronScheduler
): BackgroundTaskHandles {
  const cleanupInterval = setInterval(() => {
    cleanup(ctx)
      .then(() => {
        handleTaskSuccess('cleanup');
        runMonitoringChecks({
          queueNamesCache: ctx.queueNamesCache,
          shards: ctx.shards,
          processingShards: ctx.processingShards,
          workerManager: ctx.workerManager,
          storage: ctx.storage,
          dashboardEmit: ctx.dashboardEmit,
          state: ctx.monitoringState,
        });
      })
      .catch((error: unknown) => {
        handleTaskError('cleanup', error);
      });
  }, ctx.config.cleanupIntervalMs);

  const timeoutScheduler = ctx.timeoutScheduler;
  timeoutScheduler.start(ctx);

  const depCheckInterval = setInterval(() => {
    if (ctx.pendingDepChecks.size === 0) return;
    processPendingDependencies(ctx)
      .then(() => {
        handleTaskSuccess('dependency');
      })
      .catch((error: unknown) => {
        handleTaskError('dependency', error);
      });
  }, ctx.config.dependencyCheckMs);

  const stallCheckInterval = setInterval(() => {
    checkStalledJobs(ctx);
  }, ctx.config.stallCheckMs);

  const dlqMaintenanceInterval = setInterval(() => {
    performDlqMaintenance(ctx);
  }, ctx.config.dlqMaintenanceMs);

  const lockCheckInterval = setInterval(() => {
    checkExpiredLocks(getLockContext(ctx))
      .then(() => {
        handleTaskSuccess('lockExpiration');
      })
      .catch((error: unknown) => {
        handleTaskError('lockExpiration', error);
      });
  }, ctx.config.stallCheckMs);

  cronScheduler.start();
  return {
    cleanupInterval,
    timeoutScheduler,
    depCheckInterval,
    stallCheckInterval,
    dlqMaintenanceInterval,
    lockCheckInterval,
    cronScheduler,
  };
}

export function stopBackgroundTasks(handles: BackgroundTaskHandles): void {
  clearInterval(handles.cleanupInterval);
  handles.timeoutScheduler.stop();
  clearInterval(handles.depCheckInterval);
  clearInterval(handles.stallCheckInterval);
  clearInterval(handles.dlqMaintenanceInterval);
  clearInterval(handles.lockCheckInterval);
  handles.cronScheduler.stop();
}
