import type { EventEmitter } from 'events';
import type { Job as InternalJob } from '../../domain/types/job';
import { DelayedError, UnrecoverableError } from '../errors';
import { getSharedManager } from '../manager';
import type { FlowJobData, Job } from '../types';
import { computeStackLines } from './processorHandlers';
import type { FailureContext, ProcessorConfig } from './types';
import { outcomeWasApplied } from './ackOutcome';
import type { ManualMove } from './types';

/** Settle an explicit processor-owned outcome or nonterminal transition. */
export async function handleManualMove<T extends FlowJobData>(
  manualMove: ManualMove,
  job: Job<T>,
  config: {
    onOutcome?: (succeeded: boolean) => void;
    emitter: EventEmitter;
    shouldAbandonOutcome?: () => boolean;
  },
  internalJob: InternalJob
): Promise<boolean> {
  const result = manualMove.result;
  if (result?.type === 'pending-transition') {
    const settlement = await result.pending;
    manualMove.result = { type: 'transitioned' };
    if (settlement.status === 'failed' && !config.shouldAbandonOutcome?.()) {
      config.emitter.emit(
        'error',
        Object.assign(settlement.error, {
          context: result.context,
          jobId: String(internalJob.id),
        })
      );
    }
    return true;
  }
  if (result?.type === 'ignored' || result?.type === 'transitioned') {
    return true;
  }
  if (result?.type === 'failed') {
    const err = result.error ?? new Error('Job manually moved to failed');
    (job as { failedReason?: string }).failedReason = err.message;
    if (err.stack) {
      const { stackLines } = computeStackLines(err);
      (job as { stacktrace: string[] | null }).stacktrace = stackLines.slice(
        0,
        internalJob.stackTraceLimit
      );
    }
    config.onOutcome?.(false);
    config.emitter.emit('failed', job, err);
    return true;
  }
  if (result?.type === 'completed') {
    (job as { returnvalue?: unknown }).returnvalue = result.value;
    config.onOutcome?.(true);
    config.emitter.emit('completed', job, result.value);
    return true;
  }
  return false;
}

/** Identify an outcome that lost its processing generation before completion. */
export function isJobNotFoundError(err: Error): boolean {
  return err.message.includes('not found') || err.message.includes('not in processing');
}

async function handleDelayedError<T, R>(
  internalJob: InternalJob,
  config: ProcessorConfig<T, R>,
  context: { jobIdStr: string; token?: string | null }
): Promise<void> {
  const { embedded, tcp, emitter } = config;
  if (config.shouldAbandonOutcome?.()) return;
  try {
    if (embedded) {
      await getSharedManager().moveToDelayed(
        internalJob.id,
        internalJob.backoff || 1000,
        context.token ?? undefined
      );
    } else if (tcp) {
      await tcp.send({
        cmd: 'MoveToDelayed',
        id: internalJob.id,
        delay: internalJob.backoff || 1000,
        ...(context.token ? { token: context.token } : {}),
      });
    }
  } catch (delayError) {
    if (config.shouldAbandonOutcome?.()) return;
    const wrappedError = delayError instanceof Error ? delayError : new Error(String(delayError));
    if (!isJobNotFoundError(wrappedError)) {
      emitter.emit(
        'error',
        Object.assign(wrappedError, { context: 'delay', jobId: context.jobIdStr })
      );
    }
  }
}

export async function handleJobFailure<T, R>(
  internalJob: InternalJob,
  error: unknown,
  config: ProcessorConfig<T, R>,
  context: FailureContext<T & FlowJobData>
): Promise<void> {
  const { embedded, tcp, emitter } = config;
  const { job, jobIdStr, token } = context;
  const err = error instanceof Error ? error : new Error(String(error));
  if (config.shouldAbandonOutcome?.()) return;

  if (err instanceof DelayedError) {
    await handleDelayedError(internalJob, config, { jobIdStr, token });
    return;
  }
  if (err instanceof UnrecoverableError) {
    (internalJob as { maxAttempts: number }).maxAttempts = 1;
    (internalJob as { attempts: number }).attempts = 0;
  }

  const { stackLines, wireStack } = computeStackLines(err);
  try {
    let applied = true;
    if (embedded) {
      const outcome = await getSharedManager().fail(
        internalJob.id,
        err.message,
        token ?? undefined,
        undefined,
        wireStack,
        config.removeOnFail
      );
      applied = outcome?.applied !== false;
    } else if (tcp) {
      const response = await tcp.send({
        cmd: 'FAIL',
        id: internalJob.id,
        error: err.message,
        ...(wireStack ? { stack: wireStack } : {}),
        ...(token ? { token } : {}),
        ...(err instanceof UnrecoverableError ? { unrecoverable: true } : {}),
        ...(config.removeOnFail ? { removeOnFail: true } : {}),
      });
      if (response.ok !== true) {
        throw new Error(typeof response.error === 'string' ? response.error : 'FAIL failed');
      }
      applied = outcomeWasApplied(response.data);
    }
    if (!applied) return;
  } catch (failError) {
    if (config.shouldAbandonOutcome?.()) return;
    const wrappedError = failError instanceof Error ? failError : new Error(String(failError));
    if (!isJobNotFoundError(wrappedError)) {
      emitter.emit('error', Object.assign(wrappedError, { context: 'fail', jobId: jobIdStr }));
    }
    return;
  }

  (job as { failedReason?: string }).failedReason = err.message;
  if (err.stack) {
    (job as { stacktrace: string[] | null }).stacktrace = stackLines.slice(
      0,
      internalJob.stackTraceLimit
    );
  }
  config.onOutcome?.(false);
  emitter.emit('failed', job, err);
}
