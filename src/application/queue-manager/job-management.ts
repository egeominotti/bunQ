import type { JobId } from '../../domain/types/job';
import * as lockMgr from '../lockManager';
import * as jobMgmt from '../operations/jobManagement';
import * as jobPromotion from '../operations/jobPromotion';
import * as jobTransitions from '../operations/jobStateTransitions';
import { QueueManagerConfiguration } from './configuration';

export class QueueManagerJobManagement extends QueueManagerConfiguration {
  async cancel(jobId: JobId): Promise<boolean> {
    return jobMgmt.cancelJob(jobId, this.contextFactory.getJobMgmtContext());
  }

  async updateProgress(jobId: JobId, progress: number, message?: string): Promise<boolean> {
    return jobMgmt.updateJobProgress(
      jobId,
      progress,
      this.contextFactory.getJobMgmtContext(),
      message
    );
  }

  async updateJobData(jobId: JobId, data: unknown): Promise<boolean> {
    return jobMgmt.updateJobData(jobId, data, this.contextFactory.getJobMgmtContext());
  }

  async changePriority(jobId: JobId, priority: number, lifo?: boolean): Promise<boolean> {
    return jobMgmt.changeJobPriority(
      jobId,
      priority,
      this.contextFactory.getJobMgmtContext(),
      lifo
    );
  }

  async promote(jobId: JobId): Promise<boolean> {
    return jobPromotion.promoteJob(jobId, this.contextFactory.getJobMgmtContext());
  }

  async promoteJobs(queue: string, count?: number): Promise<number> {
    return jobPromotion.promoteJobs(queue, count, this.contextFactory.getJobMgmtContext());
  }

  async moveToDelayed(jobId: JobId, delay: number): Promise<boolean> {
    return this.changeDelay(jobId, delay);
  }

  async changeDelay(jobId: JobId, delay: number): Promise<boolean> {
    const context = this.contextFactory.getJobMgmtContext();
    const location = context.jobIndex.get(jobId);
    if (location?.type === 'queue') {
      return jobTransitions.changeWaitingDelay(jobId, delay, context);
    }
    return jobMgmt.moveJobToDelayed(jobId, delay, context);
  }

  async moveActiveToWait(jobId: JobId): Promise<boolean> {
    return jobTransitions.moveActiveToWait(jobId, this.contextFactory.getJobMgmtContext());
  }

  async changeWaitingDelay(jobId: JobId, delay: number): Promise<boolean> {
    return jobTransitions.changeWaitingDelay(jobId, delay, this.contextFactory.getJobMgmtContext());
  }

  async moveToWaitingChildren(jobId: JobId): Promise<boolean> {
    return jobTransitions.moveToWaitingChildren(jobId, this.contextFactory.getJobMgmtContext());
  }

  async extendLock(
    jobId: JobId | string,
    token: string | null,
    duration: number
  ): Promise<boolean> {
    const targetId = typeof jobId === 'string' ? (jobId as JobId) : jobId;
    const context = this.contextFactory.getLockContext();
    if (token) return lockMgr.renewJobLock(targetId, token, context, duration);
    const lock = lockMgr.getLockInfo(targetId, context);
    return lock ? lockMgr.renewJobLock(targetId, lock.token, context, duration) : false;
  }

  async discard(jobId: JobId): Promise<boolean> {
    return jobMgmt.discardJob(jobId, this.contextFactory.getJobMgmtContext());
  }
}
