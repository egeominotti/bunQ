import type { Job, JobOptions, JobStateType } from '../../types';
import * as countsOps from '../operations/counts';
import * as addOps from '../operations/add';
import * as queryOps from '../operations/query';
import * as queryStateOps from '../operations/queryStates';
import * as groupOps from '../operations/groups';
import { normalizeGroupId } from '../../groupId';
import { QueueState } from './state';

/** Job creation, lookup, state-listing, and count operations. */
export class QueueQueries<T> extends QueueState<T> {
  getGroupJobsCount(groupId: string): Promise<number> {
    return groupOps.getGroupJobsCount(this.ctx, groupId);
  }

  getGroupsJobsCount(maxCount?: number): Promise<number> {
    return groupOps.getGroupsJobsCount(this.ctx, maxCount);
  }

  getGroupActiveCount(groupId: string): Promise<number> {
    return groupOps.getGroupActiveCount(this.ctx, groupId);
  }

  setGroupRateLimit(groupId: string, max: number, duration: number): Promise<void> {
    return groupOps.setGroupRateLimit(this.ctx, groupId, max, duration);
  }

  getGroupRateLimit(groupId: string): Promise<{ max: number; duration: number } | null> {
    return groupOps.getGroupRateLimit(this.ctx, groupId);
  }

  removeGroupRateLimit(groupId: string): Promise<number> {
    return groupOps.removeGroupRateLimit(this.ctx, groupId);
  }

  getGroupRateLimitTtl(groupId: string, maxJobs?: number): Promise<number> {
    return groupOps.getGroupRateLimitTtl(this.ctx, groupId, maxJobs);
  }

  setGroupConcurrency(groupId: string, concurrency: number): Promise<void> {
    return groupOps.setGroupConcurrency(this.ctx, groupId, concurrency);
  }

  getGroupConcurrency(groupId: string): Promise<number | null> {
    return groupOps.getGroupConcurrency(this.ctx, groupId);
  }

  removeGroupConcurrency(groupId: string): Promise<number> {
    return groupOps.removeGroupConcurrency(this.ctx, groupId);
  }

  pauseGroup(groupId: string): Promise<boolean> {
    return groupOps.pauseGroup(this.ctx, groupId);
  }

  resumeGroup(groupId: string): Promise<boolean> {
    return groupOps.resumeGroup(this.ctx, groupId);
  }

  isGroupPaused(groupId: string): Promise<boolean> {
    return groupOps.isGroupPaused(this.ctx, groupId);
  }

  async getGroupJobs(groupId: string, start = 0, end = -1): Promise<Job<T>[]> {
    const normalized = normalizeGroupId(groupId);
    const jobs = (await this.getJobsAsync({
      state: ['waiting', 'prioritized', 'delayed'],
      start: 0,
      end: -1,
      asc: true,
    })) as Job<T>[];
    const grouped = jobs.filter((job) => String(job.opts.group?.id) === normalized);
    return grouped.slice(start, end < 0 ? undefined : end + 1);
  }

  async getCountsPerPriorityForGroup(groupId: string): Promise<Record<number, number>> {
    const counts: Record<number, number> = {};
    for (const job of await this.getGroupJobs(groupId)) {
      counts[job.priority] = (counts[job.priority] ?? 0) + 1;
    }
    return counts;
  }

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
