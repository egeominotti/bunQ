/**
 * Typed server-response shapes, mirroring the server's response builders
 * (src/domain/types/response.ts). `Connection.call<R>()` and `Queue.call<R>()`
 * are generic over these, so call sites read `response.job` / `response.counts`
 * with real types instead of casting `as Record<string, unknown>`.
 *
 * These narrow the loose transport `Response`; the runtime value is whatever
 * the server sent, so only assert the shape a given command actually returns.
 */

import type { JobRaw } from './job.js';
import type { JobCounts, JobStateName } from './types.js';

interface Ok {
  ok: true;
  reqId?: string;
  // Index signature so these satisfy the loose transport `Response`
  // (Record<string, unknown> & { ok }). Named fields still win for reads.
  [key: string]: unknown;
}

/** PUSH → id, or a bare ok ack. */
export interface OkResponse extends Ok {
  id?: string;
}

/** PUSHB → ids. */
export interface BatchResponse extends Ok {
  ids: string[];
}

/** GetJob → job (null when not found is surfaced as a CommandError instead). */
export interface JobResponse extends Ok {
  job: JobRaw | null;
}

/** PULL → job + lock token. */
export interface PulledJobResponse extends Ok {
  job: JobRaw | null;
  token: string | null;
}

/** PULLB → jobs + lock tokens (same order). */
export interface PulledJobsResponse extends Ok {
  jobs: JobRaw[];
  tokens: string[];
}

/** GetJobs → jobs. */
export interface JobsResponse extends Ok {
  jobs: JobRaw[];
}

/** GetState → state. */
export interface StateResponse extends Ok {
  id: string;
  state: JobStateName;
}

/** GetResult → result. */
export interface ResultResponse<R = unknown> extends Ok {
  result: R;
}

/** WaitJob → completed flag + (on completion) result. */
export interface WaitJobResponse<R = unknown> extends Ok {
  completed: boolean;
  result?: R;
}

/** GetJobCounts → counts. */
export interface JobCountsResponse extends Ok {
  counts: JobCounts;
}

/** GetProgress → progress + message. */
export interface ProgressResponse extends Ok {
  progress: number;
  message: string | null;
}

/** IsPaused → paused. */
export interface PausedResponse extends Ok {
  paused: boolean;
}

/** Count / Clean / PromoteJobs / RetryDlq → count (+ optional removed ids). */
export interface CountResponse extends Ok {
  count: number;
  ids?: string[];
}

/** Generic data-wrapped payload (logs, workers, values, webhookId, …). */
export interface DataResponse<T = unknown> extends Ok {
  data: T;
}
