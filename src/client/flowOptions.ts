import type { JobInput, JobId } from '../domain/types/job';
import type { JobOptions } from './types';

function removeFlag(value: JobOptions['removeOnComplete']): boolean {
  return typeof value === 'boolean' ? value : false;
}

/** Options intentionally unsupported by BullMQ-compatible atomic flows. */
export function assertAtomicFlowOptions(opts: JobOptions): void {
  if (opts.repeat) throw new Error('repeat is not supported inside an atomic flow');
  if (opts.deduplication) {
    throw new Error('deduplication is not supported inside an atomic flow');
  }
  if (opts.debounce) throw new Error('debounce is not supported inside an atomic flow');
  if (opts.parent) throw new Error('FlowProducer owns parent links; opts.parent is not allowed');
}

/** Convert public JobOptions into the domain input used by the atomic broker command. */
export function flowJobInput(
  data: unknown,
  opts: JobOptions,
  links: {
    parentId?: JobId;
    dependsOn?: JobId[];
    childrenIds?: JobId[];
  } = {}
): JobInput {
  assertAtomicFlowOptions(opts);
  return {
    data,
    priority: opts.priority,
    delay: opts.delay,
    maxAttempts: opts.attempts,
    backoff: opts.backoff,
    timeout: opts.timeout,
    customId: opts.jobId,
    dependsOn: links.dependsOn,
    childrenIds: links.childrenIds,
    parentId: links.parentId,
    lifo: opts.lifo,
    removeOnComplete: removeFlag(opts.removeOnComplete),
    removeOnFail: removeFlag(opts.removeOnFail),
    durable: opts.durable,
    stallTimeout: opts.stallTimeout,
    stackTraceLimit: opts.stackTraceLimit,
    keepLogs: opts.keepLogs,
    sizeLimit: opts.sizeLimit,
    failParentOnFailure: opts.failParentOnFailure,
    removeDependencyOnFailure: opts.removeDependencyOnFailure,
    continueParentOnFailure: opts.continueParentOnFailure,
    ignoreDependencyOnFailure: opts.ignoreDependencyOnFailure,
    timestamp: opts.timestamp,
  };
}
