/**
 * BullMQ-compatible error classes
 *
 * UnrecoverableError - Skip all retries, go directly to failed/DLQ
 * DelayedError - Move job back to delayed state instead of failing
 */

/**
 * Throw this error in a processor to skip all retries.
 * The job will go directly to failed/DLQ regardless of the attempts setting.
 */
export class UnrecoverableError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'UnrecoverableError';
  }
}

/**
 * Throw this error in a processor to move the job back to delayed state.
 * The job will not count as failed — it will be re-delayed for later processing.
 */
export class DelayedError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'DelayedError';
  }
}

/** Marker thrown after a processor manually rate-limits and requeues its group. */
export class RateLimitError extends Error {
  constructor(message = 'Group rate limited') {
    super(message);
    this.name = 'RateLimitError';
  }
}
