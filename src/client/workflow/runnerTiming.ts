/**
 * Timing primitives used while running a workflow step.
 *
 * Kept separate from runner.ts so the runner stays focused on state transitions.
 */

import { clock, type TimerHandle } from './clock';
import { describeError } from './identity';

/** Largest delay accepted by Bun/Node timers without wrapping to 1ms. */
const MAX_TIMER_DELAY_MS = 2_147_483_647;

/** Exponential retry backoff with jitter. */
export function retryBackoffDelay(attempt: number, baseMs = 500, maxMs = 30_000): number {
  const delay = Math.min(baseMs * 2 ** (attempt - 1), maxMs);
  return delay + delay * 0.5 * clock().random();
}

/**
 * Bound a value or PromiseLike without passing an overflowing delay to the runtime.
 *
 * Long deadlines are re-armed in platform-sized chunks. Promise.resolve is
 * intentional: it assimilates userland thenables as well as native promises.
 */
export function runWithTimeout<T>(
  value: PromiseLike<T> | T,
  timeoutMs: number,
  controller?: AbortController
): Promise<T> {
  const operation = Promise.resolve(value);
  if (!(timeoutMs > 0) || !Number.isFinite(timeoutMs)) return operation;

  const scheduler = clock();
  const deadline = scheduler.now() + timeoutMs;
  if (!Number.isFinite(deadline)) return operation;

  return new Promise<T>((resolve, reject) => {
    let timer: TimerHandle | undefined;
    let settled = false;

    const clearTimer = () => {
      if (timer) scheduler.clearTimeout(timer);
      timer = undefined;
    };
    const onTimeout = () => {
      if (settled) return;
      const remaining = deadline - scheduler.now();
      if (remaining > 0) {
        timer = scheduler.setTimeout(onTimeout, Math.min(remaining, MAX_TIMER_DELAY_MS));
        return;
      }
      settled = true;
      timer = undefined;
      const error = new Error(`Step timed out after ${timeoutMs}ms`);
      controller?.abort(error);
      reject(error);
    };

    timer = scheduler.setTimeout(onTimeout, Math.min(timeoutMs, MAX_TIMER_DELAY_MS));
    operation.then(
      (result) => {
        if (settled) return;
        settled = true;
        clearTimer();
        resolve(result);
      },
      (reason: unknown) => {
        if (settled) return;
        settled = true;
        clearTimer();
        reject(reason instanceof Error ? reason : new Error(describeError(reason)));
      }
    );
  });
}
