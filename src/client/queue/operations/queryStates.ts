/** State-specific aliases over the generic queue query operations. */

import type { Job } from '../../types';
import { getJobs, getJobsAsync, type QueryContext } from './query';

export function getWaiting<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'waiting', start, end });
}

export function getWaitingAsync<T>(ctx: QueryContext, start = 0, end = 100): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'waiting', start, end });
}

export function getDelayed<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'delayed', start, end });
}

export function getDelayedAsync<T>(ctx: QueryContext, start = 0, end = 100): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'delayed', start, end });
}

export function getActive<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'active', start, end });
}

export function getActiveAsync<T>(ctx: QueryContext, start = 0, end = 100): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'active', start, end });
}

export function getCompleted<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'completed', start, end });
}

export function getCompletedAsync<T>(ctx: QueryContext, start = 0, end = 100): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'completed', start, end });
}

export function getFailed<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'failed', start, end });
}

export function getFailedAsync<T>(ctx: QueryContext, start = 0, end = 100): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'failed', start, end });
}
