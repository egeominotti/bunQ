/**
 * Shared I/O helpers for S3 backup operations.
 */

import type { S3Client } from 'bun';
import { backupLog } from '../../shared/logger';

export const DEFAULT_S3_TIMEOUT_MS = 30_000;

/** Race a promise against a timeout and always release the timeout handle. */
export async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timed out after ${ms}ms`));
    }, ms);
  });

  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

/** Check if an error is transient and worth retrying. */
function isTransientError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  const lower = message.toLowerCase();
  return (
    lower.includes('connection reset') ||
    lower.includes('econnreset') ||
    lower.includes('timeout') ||
    lower.includes('etimedout') ||
    lower.includes('econnrefused') ||
    lower.includes('socket hang up') ||
    lower.includes('network') ||
    lower.includes('503') ||
    lower.includes('500') ||
    lower.includes('502') ||
    lower.includes('504') ||
    lower.includes('service unavailable') ||
    lower.includes('internal server error') ||
    lower.includes('bad gateway') ||
    lower.includes('gateway timeout') ||
    lower.includes('transient')
  );
}

/** Retry an async operation with exponential backoff (500ms, 1000ms, 2000ms). */
export async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  maxRetries = 3,
  baseDelayMs = 500
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt >= maxRetries || !isTransientError(error)) {
        throw error;
      }

      const delay = baseDelayMs * Math.pow(2, attempt);
      backupLog.warn(
        `${label} failed (attempt ${attempt + 1}/${maxRetries + 1}), retrying in ${delay}ms`,
        { error: error instanceof Error ? error.message : String(error) }
      );
      await Bun.sleep(delay);
    }
  }
  throw lastError;
}

/** Give every retry attempt its own configured operation timeout. */
export function retryWithTimeout<T>(
  fn: () => Promise<T>,
  timeoutMs: number,
  label: string
): Promise<T> {
  return withRetry(() => withTimeout(fn(), timeoutMs, label), label);
}

/** Async gzip compression using Web Streams. */
export async function gzipAsync(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([data as unknown as BlobPart])
    .stream()
    .pipeThrough(new CompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** Async gzip decompression using Web Streams. */
export async function gunzipAsync(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([data as unknown as BlobPart])
    .stream()
    .pipeThrough(new DecompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export function sha256(data: Uint8Array): string {
  const hasher = new Bun.CryptoHasher('sha256');
  hasher.update(data);
  return hasher.digest('hex');
}

/**
 * Remove a payload that failed to publish, then its now-orphaned metadata.
 *
 * A locally timed-out PUT may still complete because Bun's S3 write cannot be
 * aborted. In that case metadata must remain so a late payload is still paired.
 */
export async function cleanupFailedPayload(
  client: S3Client,
  key: string,
  timeoutMs: number,
  uploadError: unknown
): Promise<void> {
  if (uploadError instanceof Error && uploadError.message.includes('timed out after')) {
    return;
  }

  try {
    await retryWithTimeout(() => client.delete(key), timeoutMs, 'S3 failed payload cleanup');
    await retryWithTimeout(
      () => client.delete(`${key}.meta.json`),
      timeoutMs,
      'S3 orphaned metadata cleanup'
    );
  } catch (error) {
    backupLog.warn('Failed to clean incomplete backup publication', {
      key,
      error: String(error),
    });
  }
}
