import type { Job } from '../types/jobs/model';
import { DEFAULT_MAX_BACKOFF } from './constants';

export function normalizeStacktrace(
  lines: readonly unknown[] | undefined,
  limit: number
): string[] | null {
  if (!lines || lines.length === 0) return null;
  const out: string[] = [];
  for (const line of lines) {
    if (out.length >= limit) break;
    if (typeof line !== 'string') continue;
    const trimmed = line.trim();
    if (trimmed) out.push(trimmed);
  }
  return out.length > 0 ? out : null;
}

export function isDelayed(job: Job, now: number = Date.now()): boolean {
  return job.runAt > now;
}

export function isReady(job: Job, now: number = Date.now()): boolean {
  return job.runAt <= now;
}

export function isExpired(job: Job, now: number = Date.now()): boolean {
  if (job.ttl === null) return false;
  return now > job.createdAt + job.ttl;
}

export function isTimedOut(job: Job, now: number = Date.now()): boolean {
  if (job.timeout === null || job.startedAt === null) return false;
  return now > job.startedAt + job.timeout;
}

export function calculateBackoff(job: Job): number {
  const maxDelay = job.backoffConfig?.maxDelay ?? DEFAULT_MAX_BACKOFF;

  if (job.backoffConfig) {
    if (job.backoffConfig.type === 'fixed') {
      const base = job.backoffConfig.delay;
      const jittered = base * (0.8 + Math.random() * 0.4);
      return Math.min(jittered, maxDelay);
    }
    const base = job.backoffConfig.delay * Math.pow(2, job.attempts);
    const jittered = base * (0.5 + Math.random());
    return Math.min(jittered, maxDelay);
  }

  const base = job.backoff * Math.pow(2, job.attempts);
  const jittered = base * (0.5 + Math.random());
  return Math.min(jittered, maxDelay);
}

export function canRetry(job: Job): boolean {
  return job.attempts < job.maxAttempts;
}
