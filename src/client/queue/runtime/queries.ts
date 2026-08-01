import type { Job, JobOptions, JobStateType } from '../../types';
import * as countsOps from '../operations/counts';
import * as addOps from '../operations/add';
import * as queryOps from '../operations/query';
import * as queryStateOps from '../operations/queryStates';
import { QueueState } from './state';

/** Job creation, lookup, state-listing, and count operations. */
export class QueueQueries<T> extends QueueState<T> {
  add(name: string, data: T, opts?: JobOptions): Promise<Job<T>> {
    if (this.addBatcher && !opts?.durable) {
      return this.addBatcher.enqueue(name, data, opts) as Promise<Job<T>>;
    }
    return addOps.add(this.addCtx, name, data, opts);
  }

  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]> {
    return addOps.addBulk(this.addCtx, jobs);
  }

  getJob(id: string): Promise<Job<T> | null> {
    return queryOps.getJob(this.queryCtx, id);
  }

  getJobState(id: string): Promise<JobStateType> {
    return queryOps.getJobState(this.queryCtx, id);
  }

  getChildrenValues(id: string): Promise<Record<string, unknown>> {
    return queryOps.getChildrenValues(this.queryCtx, id);
  }

  getJobs(opts?: {
    state?: string | string[];
    start?: number;
    end?: number;
    asc?: boolean;
  }): Job<T>[] {
    return queryOps.getJobs(this.queryCtx, opts);
  }

  getJobsAsync(opts?: { state?: string | string[]; start?: number; end?: number; asc?: boolean }) {
    return queryOps.getJobsAsync(this.queryCtx, opts);
  }

  getWaiting(start?: number, end?: number) {
    return queryStateOps.getWaiting<T>(this.queryCtx, start, end);
  }

  getWaitingAsync(start?: number, end?: number) {
    return queryStateOps.getWaitingAsync<T>(this.queryCtx, start, end);
  }

  getDelayed(start?: number, end?: number) {
    return queryStateOps.getDelayed<T>(this.queryCtx, start, end);
  }

  getDelayedAsync(start?: number, end?: number) {
    return queryStateOps.getDelayedAsync<T>(this.queryCtx, start, end);
  }

  getActive(start?: number, end?: number) {
    return queryStateOps.getActive<T>(this.queryCtx, start, end);
  }

  getActiveAsync(start?: number, end?: number) {
    return queryStateOps.getActiveAsync<T>(this.queryCtx, start, end);
  }

  getCompleted(start?: number, end?: number) {
    return queryStateOps.getCompleted<T>(this.queryCtx, start, end);
  }

  getCompletedAsync(start?: number, end?: number) {
    return queryStateOps.getCompletedAsync<T>(this.queryCtx, start, end);
  }

  getFailed(start?: number, end?: number) {
    return queryStateOps.getFailed<T>(this.queryCtx, start, end);
  }

  getFailedAsync(start?: number, end?: number) {
    return queryStateOps.getFailedAsync<T>(this.queryCtx, start, end);
  }

  getJobCounts() {
    return countsOps.getJobCounts(this.ctx);
  }

  getJobCountsAsync() {
    return countsOps.getJobCountsAsync(this.ctx);
  }

  getWaitingCount() {
    return countsOps.getWaitingCount(this.ctx);
  }

  getActiveCount() {
    return countsOps.getActiveCount(this.ctx);
  }

  getCompletedCount() {
    return countsOps.getCompletedCount(this.ctx);
  }

  getFailedCount() {
    return countsOps.getFailedCount(this.ctx);
  }

  getDelayedCount() {
    return countsOps.getDelayedCount(this.ctx);
  }

  count() {
    return countsOps.count(this.ctx);
  }

  countAsync() {
    return countsOps.countAsync(this.ctx);
  }

  getCountsPerPriority() {
    return countsOps.getCountsPerPriority(this.ctx);
  }

  getCountsPerPriorityAsync() {
    return countsOps.getCountsPerPriorityAsync(this.ctx);
  }
}
