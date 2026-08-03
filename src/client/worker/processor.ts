/**
 * Job Processor
 * Handles job execution and result reporting
 */

import type { Job, FlowJobData } from '../types';
import { createPublicJob } from '../types';
import type { Job as InternalJob } from '../../domain/types/job';
import { getSharedManager } from '../manager';
import type { ProcessorConfig } from './types';
import {
  createProgressHandler,
  createLogHandler,
  createGetStateHandler,
  createGetChildrenValuesHandler,
  createGetFailedChildrenValuesHandler,
  createGetIgnoredChildrenFailuresHandler,
  createRemoveChildDependencyHandler,
  createRemoveUnprocessedChildrenHandler,
  createMoveToFailedHandler,
  createMoveToCompletedHandler,
  createRemoveHandler,
  createRetryHandler,
  createUpdateDataHandler,
  createPromoteHandler,
  createChangeDelayHandler,
  createChangePriorityHandler,
  createExtendLockHandler,
  createClearLogsHandler,
  createMoveToWaitHandler,
  createMoveToDelayedHandler,
  createMoveToWaitingChildrenHandler,
  createWaitUntilFinishedHandler,
  createDiscardHandler,
  createGetDependenciesHandler,
  createGetDependenciesCountHandler,
  createRemoveDeduplicationKeyHandler,
} from './processorHandlers';
import { handleJobFailure, handleManualMove, isJobNotFoundError } from './processorOutcome';
import type { ManualMove } from './types';

export type { ProcessorConfig } from './types';

/**
 * Process a single job
 */
export async function processJob<T, R>(
  internalJob: InternalJob,
  config: ProcessorConfig<T, R>
): Promise<void> {
  const { processor, embedded, tcp, ackBatcher, emitter, token } = config;
  const jobName = internalJob.name;
  const jobIdStr = String(internalJob.id);

  // Use a holder to break the circular reference between job and progress handler
  type JobData = T & FlowJobData;
  const jobHolder: { current: Job<JobData> | null } = { current: null };

  // Track whether the processor explicitly consumed this delivery generation.
  // Use a mutable holder so TS does not narrow through callback mutation.
  const manualMove: ManualMove = { result: null };
  const onTransitionApplied = () => {
    manualMove.result = { type: 'transitioned' };
  };

  const moveToFailedHandler = createMoveToFailedHandler({
    embedded,
    tcp,
    internalJob,
    token,
    removeOnFail: config.removeOnFail,
    onCalled: (error: Error) => {
      manualMove.result = { type: 'failed', error };
    },
    onIgnored: () => {
      manualMove.result = { type: 'ignored' };
    },
  });

  const moveToCompletedHandler = createMoveToCompletedHandler({
    embedded,
    ackBatcher,
    internalJob,
    token,
    removeOnComplete: config.removeOnComplete,
    onCalled: (value: unknown) => {
      manualMove.result = { type: 'completed', value };
    },
    onIgnored: () => {
      manualMove.result = { type: 'ignored' };
    },
  });

  const job = createPublicJob<JobData>({
    job: internalJob,
    name: jobName,
    updateProgress: createProgressHandler(embedded, tcp, emitter, jobHolder),
    log: createLogHandler(embedded, tcp, emitter, jobHolder),
    getState: createGetStateHandler(embedded, tcp),
    getChildrenValues: createGetChildrenValuesHandler(embedded, tcp),
    getFailedChildrenValues: createGetFailedChildrenValuesHandler(embedded, tcp),
    getIgnoredChildrenFailures: createGetIgnoredChildrenFailuresHandler(embedded, tcp),
    removeChildDependency: createRemoveChildDependencyHandler(embedded, tcp),
    removeUnprocessedChildren: createRemoveUnprocessedChildrenHandler(embedded, tcp),
    moveToFailed: moveToFailedHandler,
    moveToCompleted: moveToCompletedHandler,
    // Issue #82 follow-up: wire all remaining mutation/query methods so they
    // no longer silently no-op inside the Worker processor.
    remove: createRemoveHandler(embedded, tcp),
    retry: createRetryHandler(embedded, tcp, {
      internalJob,
      token: token ?? undefined,
      onTransitionApplied,
    }),
    updateData: createUpdateDataHandler(embedded, tcp),
    promote: createPromoteHandler(embedded, tcp),
    changeDelay: createChangeDelayHandler(embedded, tcp, token ?? undefined, onTransitionApplied),
    changePriority: createChangePriorityHandler(embedded, tcp),
    extendLock: createExtendLockHandler(embedded, tcp),
    clearLogs: createClearLogsHandler(embedded, tcp),
    moveToWait: createMoveToWaitHandler(embedded, tcp, onTransitionApplied),
    moveToDelayed: createMoveToDelayedHandler(embedded, tcp, onTransitionApplied),
    moveToWaitingChildren: createMoveToWaitingChildrenHandler(embedded, tcp, onTransitionApplied),
    waitUntilFinished: createWaitUntilFinishedHandler(embedded, tcp),
    discard: createDiscardHandler(embedded, tcp, {
      token: token ?? undefined,
      onPending: (pending) => {
        if (manualMove.result !== null) return;
        manualMove.result = { type: 'pending-transition', context: 'discard', pending };
      },
    }),
    getDependencies: createGetDependenciesHandler(embedded, tcp, internalJob),
    getDependenciesCount: createGetDependenciesCountHandler(embedded, tcp, internalJob),
    removeDeduplicationKey: createRemoveDeduplicationKeyHandler(embedded, tcp),
    token: token ?? undefined,
  });

  jobHolder.current = job;

  emitter.emit('active', job);

  try {
    const result = await processor(job);
    if (config.shouldAbandonOutcome?.()) return;

    // An explicit broker disposition owns the generation, so skip auto-ACK.
    if (await handleManualMove(manualMove, job, config, internalJob)) return;

    // Normal path: auto-ACK
    try {
      let applied = true;
      if (embedded) {
        const manager = getSharedManager();
        // Pass token for lock verification
        const outcome = await manager.ack(internalJob.id, result, token ?? undefined, {
          removeOnComplete: config.removeOnComplete,
        });
        applied = outcome?.applied !== false;
      } else {
        // Queue with token for batch ACK
        applied = await ackBatcher.queue(
          jobIdStr,
          result,
          token ?? undefined,
          config.removeOnComplete
        );
      }
      if (!applied) return;
    } catch (ackErr) {
      if (config.shouldAbandonOutcome?.()) return;
      // If stall detection already removed the job from processing,
      // the ACK will fail with "Job not found". This is expected
      // behavior (Issue #33) - emit error event but don't re-throw.
      const ackError = ackErr instanceof Error ? ackErr : new Error(String(ackErr));
      if (isJobNotFoundError(ackError)) {
        emitter.emit('error', Object.assign(ackError, { context: 'ack-stale', jobId: jobIdStr }));
        return;
      }
      throw ackErr;
    }

    (job as { returnvalue?: unknown }).returnvalue = result;
    config.onOutcome?.(true);
    emitter.emit('completed', job, result);
  } catch (error) {
    if (config.shouldAbandonOutcome?.()) return;
    // An explicit broker disposition also suppresses catch-path auto-FAIL.
    if (await handleManualMove(manualMove, job, config, internalJob)) return;
    await handleJobFailure(internalJob, error, config, { job, jobIdStr, token });
  }
}
