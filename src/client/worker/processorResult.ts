import type { ObservableLike } from '../types';

function isObservable<T>(value: unknown): value is ObservableLike<T> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { subscribe?: unknown }).subscribe === 'function'
  );
}

function abortError(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  return new DOMException('Job processing aborted', 'AbortError');
}

export async function resolveProcessorResult<T>(
  value: Promise<T> | ObservableLike<T> | T,
  signal: AbortSignal
): Promise<T> {
  if (signal.aborted) {
    if (value instanceof Promise) void value.catch(() => undefined);
    throw abortError(signal);
  }
  if (!isObservable<T>(value)) {
    return await value;
  }

  return await new Promise<T>((resolve, reject) => {
    let settled = false;
    let hasValue = false;
    let lastValue: T;
    let cleanup: (() => void) | undefined;
    let cleaned = false;
    const runCleanup = () => {
      if (cleaned || !cleanup) return;
      cleaned = true;
      try {
        cleanup();
      } catch {
        // Observable teardown cannot change an already-settled processor result.
      }
    };
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener('abort', onAbort);
      callback();
      runCleanup();
    };
    const onAbort = () => finish(() => reject(abortError(signal)));
    signal.addEventListener('abort', onAbort, { once: true });

    try {
      const subscription = value.subscribe({
        next(nextValue) {
          hasValue = true;
          lastValue = nextValue;
        },
        error(error) {
          finish(() => reject(error));
        },
        complete() {
          finish(() =>
            hasValue
              ? resolve(lastValue)
              : reject(new Error('Processor Observable completed without a value'))
          );
        },
      });
      cleanup =
        typeof subscription === 'function'
          ? subscription
          : subscription
            ? () => subscription.unsubscribe()
            : undefined;
      if (settled) runCleanup();
      else if (signal.aborted) onAbort();
    } catch (error) {
      finish(() => reject(error));
    }
  });
}
