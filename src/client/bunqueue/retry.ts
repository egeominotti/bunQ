/**
 * Bunqueue — Advanced Retry with backoff strategies
 */

import type { RetryConfig, RetryStrategy } from './types';

/** Calculate backoff delay based on strategy */
export function calculateBackoff(
  strategy: RetryStrategy,
  attempt: number,
  baseDelay: number,
  error: Error,
  config: RetryConfig
): number {
  switch (strategy) {
    case 'fixed':
      return baseDelay;

    case 'exponential':
      return baseDelay * Math.pow(2, attempt - 1);

    case 'jitter': {
      const exp = baseDelay * Math.pow(2, attempt - 1);
      return Math.floor(exp * (0.5 + Math.random()));
    }

    case 'fibonacci': {
      let a = 1,
        b = 1;
      for (let i = 0; i < attempt - 1; i++) {
        const next = a + b;
        a = b;
        b = next;
      }
      return baseDelay * b;
    }

    case 'custom':
      if (config.customBackoff) {
        return config.customBackoff(attempt, error);
      }
      return baseDelay;

    default:
      return baseDelay;
  }
}

function cancellationError(): Error {
  return new Error('Job cancelled');
}

function waitForRetry<R>(delay: number, next: () => Promise<R>, signal?: AbortSignal): Promise<R> {
  return new Promise<R>((resolve, reject) => {
    if (signal?.aborted) {
      reject(cancellationError());
      return;
    }

    let timer: ReturnType<typeof setTimeout> | null = null;
    const onAbort = () => {
      if (timer !== null) clearTimeout(timer);
      timer = null;
      signal?.removeEventListener('abort', onAbort);
      reject(cancellationError());
    };
    timer = setTimeout(() => {
      timer = null;
      signal?.removeEventListener('abort', onAbort);
      if (signal?.aborted) {
        reject(cancellationError());
        return;
      }
      resolve(next());
    }, delay);
    signal?.addEventListener('abort', onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });
}

/** Execute a function with retry logic */
export function executeWithRetry<R>(
  fn: () => Promise<R>,
  config: RetryConfig,
  signal?: AbortSignal
): Promise<R> {
  const maxAttempts = config.maxAttempts ?? 3;
  const baseDelay = config.delay ?? 1000;
  const strategy = config.strategy ?? 'exponential';

  const attempt = (n: number): Promise<R> => {
    if (signal?.aborted) return Promise.reject(cancellationError());
    let execution: Promise<R>;
    try {
      execution = fn();
    } catch (error) {
      execution = Promise.reject(error);
    }
    return execution.catch((err: unknown) => {
      const error = err instanceof Error ? err : new Error(String(err));
      if (signal?.aborted) throw cancellationError();
      if (n >= maxAttempts) throw error;
      if (config.retryIf && !config.retryIf(error, n)) throw error;

      const delay = calculateBackoff(strategy, n, baseDelay, error, config);
      return waitForRetry(delay, () => attempt(n + 1), signal);
    });
  };

  return attempt(1);
}
