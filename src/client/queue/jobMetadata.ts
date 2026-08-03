import { lastFailedReason } from '../../domain/job/terminal';
import type { Job as InternalJob } from '../../domain/types/job';
import type { JobReflectionMeta } from './types/job';
import { buildJobOpts } from '../jobHelpers';

export interface TerminalJobView extends InternalJob {
  returnvalue?: unknown;
  failedReason?: string;
}

/** Build one public metadata shape for embedded and TCP queue reads. */
export function metadataFromJob(job: TerminalJobView, embeddedResult?: unknown): JobReflectionMeta {
  const opts = buildJobOpts(job);
  return {
    attemptsMade: job.attempts,
    attemptsStarted: job.attempts,
    progress: job.progress,
    stalledCounter: job.stallCount,
    priority: job.priority ?? 0,
    delay: opts.delay ?? 0,
    processedOn: job.startedAt ?? undefined,
    finishedOn: job.completedAt ?? undefined,
    opts,
    stacktrace: job.stacktrace ?? null,
    returnvalue: Object.hasOwn(job, 'returnvalue') ? job.returnvalue : embeddedResult,
    failedReason: job.failedReason ?? lastFailedReason(job),
  };
}

/** Encode terminal values for BullMQ-compatible raw JSON output. */
export function serializeReturnvalue(value: unknown): string | undefined {
  return value === undefined ? undefined : JSON.stringify(value);
}
