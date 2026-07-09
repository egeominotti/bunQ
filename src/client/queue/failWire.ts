/**
 * Shared FAIL payload builder.
 *
 * Several client entry points move a job to `failed` (the Queue reflection API
 * `queue.getJob(id).moveToFailed()`, the job proxies, and `moveJobToFailed`).
 * They must all persist the stacktrace (#74) and honour `UnrecoverableError`,
 * exactly like the worker failure path — otherwise the stack and the
 * "do not retry" intent are silently dropped (the #111 silent-loss class).
 * Centralising the mapping here keeps every site consistent.
 */

import { UnrecoverableError } from '../errors';
import { computeStackLines } from '../worker/processorHandlers';

/** Build the TCP `FAIL` command for an explicit job failure. */
export function buildFailCommand(
  id: string,
  error: Error,
  token?: string
): Record<string, unknown> {
  const { wireStack } = computeStackLines(error);
  return {
    cmd: 'FAIL',
    id,
    error: error.message,
    ...(wireStack ? { stack: wireStack } : {}),
    ...(token ? { token } : {}),
    ...(error instanceof UnrecoverableError ? { unrecoverable: true } : {}),
  };
}

/**
 * Decompose an error into the trailing args of the embedded
 * `manager.fail(jobId, error?, token?, unrecoverable?, stack?)` signature.
 */
export function failEmbeddedArgs(
  error: Error,
  token?: string
): [string, string | undefined, boolean, string[] | undefined] {
  const { wireStack } = computeStackLines(error);
  return [error.message, token, error instanceof UnrecoverableError, wireStack];
}
