import type { ChangePriorityOpts } from '../../types';
import * as controlOps from '../operations/control';
import * as managementOps from '../operations/management';
import { QueueQueries } from './queries';

/** Queue lifecycle controls and individual job management operations. */
export class QueueControl<T> extends QueueQueries<T> {
  pause() {
    controlOps.pause(this.ctx);
  }

  pauseAsync() {
    return controlOps.pauseAsync(this.ctx);
  }

  resume() {
    controlOps.resume(this.ctx);
  }

  resumeAsync() {
    return controlOps.resumeAsync(this.ctx);
  }

  drain() {
    controlOps.drain(this.ctx);
  }

  drainAsync() {
    return controlOps.drainAsync(this.ctx);
  }

  obliterate() {
    controlOps.obliterate(this.ctx);
  }

  obliterateAsync() {
    return controlOps.obliterateAsync(this.ctx);
  }

  isPaused() {
    return controlOps.isPaused(this.ctx);
  }

  isPausedAsync() {
    return controlOps.isPausedAsync(this.ctx);
  }

  waitUntilReady() {
    return controlOps.waitUntilReady(this.ctx);
  }

  remove(id: string) {
    managementOps.remove(this.ctx, id);
  }

  removeAsync(id: string) {
    return managementOps.removeAsync(this.ctx, id);
  }

  retryJob(id: string) {
    return managementOps.retryJob(this.ctx, id);
  }

  retryJobs(opts?: { state?: 'failed' | 'completed'; count?: number; timestamp?: number }) {
    return managementOps.retryJobs(this.ctx, opts);
  }

  clean(grace: number, limit: number, type?: string) {
    return managementOps.clean(
      this.ctx,
      grace,
      limit,
      type as 'completed' | 'wait' | 'active' | 'paused' | 'delayed' | 'failed'
    );
  }

  cleanAsync(grace: number, limit: number, type?: string) {
    return managementOps.cleanAsync(
      this.ctx,
      grace,
      limit,
      type as 'completed' | 'wait' | 'active' | 'paused' | 'delayed' | 'failed'
    );
  }

  promoteJobs(opts?: { count?: number }) {
    return managementOps.promoteJobs(this.ctx, opts);
  }

  promoteJob(id: string) {
    return managementOps.promoteJob(this.ctx, id);
  }

  updateJobProgress(id: string, progress: number | object) {
    return managementOps.updateJobProgress(this.ctx, id, progress);
  }

  getJobLogs(id: string, start?: number, end?: number, _asc?: boolean) {
    return managementOps.getJobLogs(this.ctx, id, start, end);
  }

  addJobLog(id: string, logRow: string, _keepLogs?: number) {
    return managementOps.addJobLog(this.ctx, id, logRow);
  }

  clearJobLogs(id: string, keepLogs?: number) {
    return managementOps.clearJobLogs(this.ctx, id, keepLogs);
  }

  updateJobData(id: string, data: unknown) {
    return managementOps.updateJobData(this.ctx, id, data);
  }

  changeJobDelay(id: string, delay: number) {
    return managementOps.changeJobDelay(this.ctx, id, delay);
  }

  changeJobPriority(id: string, opts: ChangePriorityOpts) {
    return managementOps.changeJobPriority(this.ctx, id, opts);
  }

  extendJobLock(id: string, token: string, duration: number) {
    return managementOps.extendJobLock(this.ctx, id, token, duration);
  }
}
