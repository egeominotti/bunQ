import type { GetDependenciesOpts, JobDependencies, JobDependenciesCount } from '../../types';
import * as bullmqCompatOps from '../bullmqCompat';
import * as deduplicationOps from '../deduplication';
import * as jobMoveOps from '../jobMove';
import * as schedulerOps from '../scheduler';
import type { JobTemplate, RepeatOpts, SchedulerInfo } from '../scheduler';
import { QueueConfiguration } from './configuration';

/** Scheduler, deduplication, job-transition, and dependency operations. */
export class QueueScheduling<T> extends QueueConfiguration<T> {
  upsertJobScheduler(
    schedulerId: string,
    repeatOpts: RepeatOpts,
    jobTemplate?: JobTemplate<T>
  ): Promise<SchedulerInfo | null> {
    return schedulerOps.upsertJobScheduler(
      { ...this.ctx, defaultJobOptions: this.opts.defaultJobOptions },
      schedulerId,
      repeatOpts,
      jobTemplate
    );
  }

  removeJobScheduler(schedulerId: string) {
    return schedulerOps.removeJobScheduler(this.ctx, schedulerId);
  }

  getJobScheduler(schedulerId: string) {
    return schedulerOps.getJobScheduler(this.ctx, schedulerId);
  }

  getJobSchedulers(start?: number, end?: number, asc?: boolean) {
    return schedulerOps.getJobSchedulers(this.ctx, start, end, asc);
  }

  getJobSchedulersCount() {
    return schedulerOps.getJobSchedulersCount(this.ctx);
  }

  getDeduplicationJobId(deduplicationId: string) {
    return deduplicationOps.getDeduplicationJobId(this.ctx, deduplicationId);
  }

  removeDeduplicationKey(deduplicationId: string) {
    return deduplicationOps.removeDeduplicationKey(this.ctx, deduplicationId);
  }

  moveJobToCompleted(id: string, returnValue: unknown, token?: string) {
    return jobMoveOps.moveJobToCompleted(this.moveCtx, id, returnValue, token);
  }

  moveJobToFailed(id: string, error: Error, token?: string) {
    return jobMoveOps.moveJobToFailed(this.moveCtx, id, error, token);
  }

  moveJobToWait(id: string, token?: string) {
    return jobMoveOps.moveJobToWait(this.moveCtx, id, token);
  }

  moveJobToDelayed(id: string, timestamp: number, token?: string) {
    return jobMoveOps.moveJobToDelayed(this.moveCtx, id, timestamp, token);
  }

  moveJobToWaitingChildren(
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) {
    return jobMoveOps.moveJobToWaitingChildren(this.moveCtx, id, token, opts);
  }

  waitJobUntilFinished(id: string, queueEvents: unknown, ttl?: number) {
    return jobMoveOps.waitJobUntilFinished(this.moveCtx, id, queueEvents, ttl);
  }

  getJobDependencies(id: string, opts?: GetDependenciesOpts): Promise<JobDependencies> {
    return bullmqCompatOps.getJobDependencies(this.addCtx as never, id, opts);
  }

  getJobDependenciesCount(id: string, opts?: GetDependenciesOpts): Promise<JobDependenciesCount> {
    return bullmqCompatOps.getJobDependenciesCount(this.addCtx as never, id, opts);
  }

  getDependencies(
    parentId: string,
    type?: 'processed' | 'unprocessed',
    start?: number,
    end?: number
  ) {
    return bullmqCompatOps.getDependencies(this.addCtx as never, parentId, type, start, end);
  }
}
