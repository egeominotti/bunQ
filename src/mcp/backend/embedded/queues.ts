import { jobId as toJobId } from '../../../domain/types/job';
import type { JobCounts } from '../../types/adapter';
import { serializeMcpJob } from '../serializers';
import { EmbeddedJobBackend } from './jobs';

export class EmbeddedQueueBackend extends EmbeddedJobBackend {
  getJobs(queue: string, opts?: { state?: string; start?: number; end?: number }) {
    const jobs = this.manager.getJobs(queue, {
      state: opts?.state as 'waiting' | 'delayed' | 'active' | 'completed' | 'failed',
      start: opts?.start,
      end: opts?.end,
    });
    return Promise.resolve(jobs.map(serializeMcpJob));
  }

  getJobCounts(queue: string): Promise<JobCounts> {
    const counts = this.manager.getQueueJobCounts(queue);
    const isPaused = this.manager.isPaused(queue);
    return Promise.resolve({
      waiting: counts.waiting,
      prioritized: counts.prioritized,
      delayed: counts.delayed,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      paused: isPaused ? counts.waiting : 0,
    });
  }

  pauseQueue(queue: string) {
    this.manager.pause(queue);
    return Promise.resolve();
  }

  resumeQueue(queue: string) {
    this.manager.resume(queue);
    return Promise.resolve();
  }

  drainQueue(queue: string) {
    return Promise.resolve(this.manager.drain(queue));
  }

  obliterateQueue(queue: string) {
    this.manager.obliterate(queue);
    return Promise.resolve();
  }

  listQueues() {
    return Promise.resolve(this.manager.listQueues());
  }

  countJobs(queue: string) {
    return Promise.resolve(this.manager.count(queue));
  }

  cleanQueue(queue: string, graceMs: number, state?: string, limit?: number) {
    return Promise.resolve(this.manager.clean(queue, graceMs, state, limit));
  }

  isPaused(queue: string) {
    return Promise.resolve(this.manager.isPaused(queue));
  }

  getCountsPerPriority(queue: string) {
    return Promise.resolve(this.manager.getCountsPerPriority(queue));
  }

  getDlq(queue: string, limit?: number) {
    return Promise.resolve(this.manager.getDlq(queue, limit).map(serializeMcpJob));
  }

  retryDlq(queue: string, id?: string) {
    return Promise.resolve(this.manager.retryDlq(queue, id ? toJobId(id) : undefined));
  }

  purgeDlq(queue: string) {
    return Promise.resolve(this.manager.purgeDlq(queue));
  }

  retryCompleted(queue: string, id?: string) {
    return Promise.resolve(this.manager.retryCompleted(queue, id ? toJobId(id) : undefined));
  }

  setRateLimit(queue: string, limit: number) {
    this.manager.setRateLimit(queue, limit);
    return Promise.resolve();
  }

  clearRateLimit(queue: string) {
    this.manager.clearRateLimit(queue);
    return Promise.resolve();
  }

  setConcurrency(queue: string, limit: number) {
    this.manager.setConcurrency(queue, limit);
    return Promise.resolve();
  }

  clearConcurrency(queue: string) {
    this.manager.clearConcurrency(queue);
    return Promise.resolve();
  }
}
