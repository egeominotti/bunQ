/**
 * Job Processor
 * Handles job execution and result reporting
 */

import type { EventEmitter } from 'events';
import type { TcpConnection } from './types';
import type { Processor, Job, FlowJobData } from '../types';
import { createPublicJob } from '../types';
import type { Job as InternalJob } from '../../domain/types/job';
import { getSharedManager } from '../manager';
import { UnrecoverableError } from '../errors';
import { DelayedError } from '../errors';
import type { AckBatcher } from './ackBatcher';
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

/** Processor configuration */
export interface ProcessorConfig<T, R> {
  name: string;
  processor: Processor<T, R>;
  embedded: boolean;
  tcp: TcpConnection | null;
  ackBatcher: AckBatcher;
  emitter: EventEmitter;
  token?: string | null; // Lock token for ownership verification
  onOutcome?: (succeeded: boolean) => void;
}

/**
 * Process a single job
 */
export async function processJob<T, R>(
  internalJob: InternalJob,
  config: ProcessorConfig<T, R>
): Promise<void> {
  const { processor, embedded, tcp, ackBatcher, emitter, token } = config;
  const jobData = internalJob.data as { name?: string } | null;
  const jobName = jobData?.name ?? 'default';
  const jobIdStr = String(internalJob.id);

  // Use a holder to break the circular reference between job and progress handler
  type JobData = T & FlowJobData;
  const jobHolder: { current: Job<JobData> | null } = { current: null };

  // Track whether moveToFailed/moveToCompleted was explicitly called (Issue #82)
  // Use a mutable holder so TS doesn't narrow to `never` after closure mutation
  const manualMove: {
    result: { type: 'completed' | 'failed'; value?: unknown; error?: Error } | null;
  } = { result: null };

  const moveToFailedHandler = createMoveToFailedHandler(
    embedded,
    tcp,
    internalJob,
    token,
    (error: Error) => {
      manualMove.result = { type: 'failed', error };
    }
  );

  const moveToCompletedHandler = createMoveToCompletedHandler(
    embedded,
    ackBatcher,
    internalJob,
    token,
    (value: unknown) => {
      manualMove.result = { type: 'completed', value };
    }
  );

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
    retry: createRetryHandler(embedded, tcp, internalJob),
    updateData: createUpdateDataHandler(embedded, tcp),
    promote: createPromoteHandler(embedded, tcp),
    changeDelay: createChangeDelayHandler(embedded, tcp),
    changePriority: createChangePriorityHandler(embedded, tcp),
    extendLock: createExtendLockHandler(embedded, tcp),
    clearLogs: createClearLogsHandler(embedded, tcp),
    moveToWait: createMoveToWaitHandler(embedded, tcp),
    moveToDelayed: createMoveToDelayedHandler(embedded, tcp),
    moveToWaitingChildren: createMoveToWaitingChildrenHandler(embedded, tcp),
    waitUntilFinished: createWaitUntilFinishedHandler(embedded, tcp),
    discard: createDiscardHandler(embedded, tcp),
    getDependencies: createGetDependenciesHandler(embedded, tcp, internalJob),
    getDependenciesCount: createGetDependenciesCountHandler(embedded, tcp, internalJob),
    removeDeduplicationKey: createRemoveDeduplicationKeyHandler(),
    token: token ?? undefined,
  });

  jobHolder.current = job;

  emitter.emit('active', job);

  try {
    const result = await processor(job);

    // Issue #82: If moveToFailed/moveToCompleted was called, skip auto-ACK
    if (handleManualMove(manualMove, job, config)) return;

    // Normal path: auto-ACK
    try {
      if (embedded) {
        const manager = getSharedManager();
        // Pass token for lock verification
        await manager.ack(internalJob.id, result, token ?? undefined);
      } else {
        // Queue with token for batch ACK
        await ackBatcher.queue(jobIdStr, result, token ?? undefined);
      }
    } catch (ackErr) {
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
    // Issue #82: If moveToFailed was already called, skip normal failure handling
    if (handleManualMove(manualMove, job, config)) return;
    await handleJobFailure(internalJob, error, config, { job, jobIdStr, token });
  }
}

/** Issue #82: Handle explicit moveToFailed/moveToCompleted called inside processor */
function handleManualMove<T extends FlowJobData>(
  manualMove: { result: { type: 'completed' | 'failed'; value?: unknown; error?: Error } | null },
  job: Job<T>,
  config: { onOutcome?: (succeeded: boolean) => void; emitter: EventEmitter }
): boolean {
  if (manualMove.result?.type === 'failed') {
    const err = manualMove.result.error ?? new Error('Job manually moved to failed');
    (job as { failedReason?: string }).failedReason = err.message;
    config.onOutcome?.(false);
    config.emitter.emit('failed', job, err);
    return true;
  }
  if (manualMove.result?.type === 'completed') {
    (job as { returnvalue?: unknown }).returnvalue = manualMove.result.value;
    config.onOutcome?.(true);
    config.emitter.emit('completed', job, manualMove.result.value);
    return true;
  }
  return false;
}

interface FailureContext<T extends FlowJobData> {
  job: Job<T>;
  jobIdStr: string;
  token?: string | null;
}

/** Check if error is a "job not found" error from stale ACK/FAIL (Issue #33) */
function isJobNotFoundError(err: Error): boolean {
  return err.message.includes('not found') || err.message.includes('not in processing');
}

/** Handle DelayedError: move job back to delayed state without counting as failure */
async function handleDelayedError<T, R>(
  internalJob: InternalJob,
  config: ProcessorConfig<T, R>,
  context: { jobIdStr: string; token?: string | null }
): Promise<void> {
  const { embedded, tcp, emitter } = config;
  try {
    if (embedded) {
      const manager = getSharedManager();
      await manager.moveToDelayed(internalJob.id, internalJob.backoff || 1000);
    } else if (tcp) {
      await tcp.send({
        cmd: 'MoveToDelayed',
        id: internalJob.id,
        delay: internalJob.backoff || 1000,
        ...(context.token ? { token: context.token } : {}),
      });
    }
  } catch (delayError) {
    const wrappedError = delayError instanceof Error ? delayError : new Error(String(delayError));
    if (!isJobNotFoundError(wrappedError)) {
      emitter.emit(
        'error',
        Object.assign(wrappedError, { context: 'delay', jobId: context.jobIdStr })
      );
    }
  }
}

async function handleJobFailure<T, R>(
  internalJob: InternalJob,
  error: unknown,
  config: ProcessorConfig<T, R>,
  context: FailureContext<T & FlowJobData>
): Promise<void> {
  const { embedded, tcp, emitter } = config;
  const { job, jobIdStr, token } = context;
  const err = error instanceof Error ? error : new Error(String(error));

  if (err instanceof DelayedError) {
    await handleDelayedError(internalJob, config, { jobIdStr, token });
    return;
  }

  // UnrecoverableError: force skip all retries
  if (err instanceof UnrecoverableError) {
    (internalJob as { maxAttempts: number }).maxAttempts = 1;
    (internalJob as { attempts: number }).attempts = 0;
  }

  // Bug #74: stack lines computed BEFORE the send so the server can persist
  // them. The wire copy is capped at 50 lines as a bandwidth guard; the
  // authoritative cap (job.stackTraceLimit) is applied server-side in failJob.
  const stackLines = err.stack
    ? err.stack
        .split('\n')
        .map((l) => l.trim())
        .filter(Boolean)
    : [];
  const wireStack = stackLines.length > 0 ? stackLines.slice(0, 50) : undefined;

  try {
    if (embedded) {
      const manager = getSharedManager();
      await manager.fail(internalJob.id, err.message, token ?? undefined, undefined, wireStack);
    } else if (tcp) {
      await tcp.send({
        cmd: 'FAIL',
        id: internalJob.id,
        error: err.message,
        ...(wireStack ? { stack: wireStack } : {}),
        ...(token ? { token } : {}),
        ...(err instanceof UnrecoverableError ? { unrecoverable: true } : {}),
      });
    }
  } catch (failError) {
    const wrappedError = failError instanceof Error ? failError : new Error(String(failError));
    if (isJobNotFoundError(wrappedError)) {
      return;
    }
    emitter.emit('error', Object.assign(wrappedError, { context: 'fail', jobId: jobIdStr }));
  }

  (job as { failedReason?: string }).failedReason = err.message;

  // Bug #74: populate stacktrace on the local `failed` event object
  if (err.stack) {
    const limit = internalJob.stackTraceLimit;
    (job as { stacktrace: string[] | null }).stacktrace = stackLines.slice(0, limit);
  }

  config.onOutcome?.(false);
  emitter.emit('failed', job, err);
}
