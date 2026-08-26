import type { QueueManager } from '../../../application/queueManager';
import type { CronJobInput } from '../../../domain/types/cron';
import type { JobId } from '../../../domain/types/job';
import type { CloudQueueAdapter, CloudSnapshotSource } from './types';

const SNAPSHOT_STATES = [
  'waiting',
  'active',
  'delayed',
  'failed',
  'completed',
  'prioritized',
  'paused',
  'waiting-children',
] as const;

/** SQLite/local Cloud strategy; delegates to the existing synchronous semantics. */
export class LocalCloudQueueAdapter implements CloudQueueAdapter {
  constructor(readonly manager: QueueManager) {}

  pause(queue: string, paused: boolean): Promise<void> {
    if (paused) this.manager.pause(queue);
    else this.manager.resume(queue);
    return Promise.resolve();
  }

  drain(queue: string): Promise<number> {
    return Promise.resolve(this.manager.drain(queue));
  }

  clean(queue: string, graceMs: number, state?: string, limit?: number): Promise<JobId[]> {
    return Promise.resolve(this.manager.clean(queue, graceMs, state, limit));
  }

  obliterate(queue: string): Promise<void> {
    this.manager.obliterate(queue);
    return Promise.resolve();
  }

  async promoteAll(queue: string, limit: number): Promise<number> {
    const jobs = this.manager.getJobs(queue, { state: ['delayed'], start: 0, end: limit });
    let promoted = 0;
    for (const job of jobs) {
      try {
        if (await this.manager.promote(job.id)) promoted++;
      } catch {
        // The job may have moved since the command selected it.
      }
    }
    return promoted;
  }

  retryCompleted(queue: string): Promise<number> {
    return Promise.resolve(this.manager.retryCompleted(queue));
  }

  setRateLimit(queue: string, limit: number | null): Promise<void> {
    if (limit === null) this.manager.clearRateLimit(queue);
    else this.manager.setRateLimit(queue, limit);
    return Promise.resolve();
  }

  setConcurrency(queue: string, limit: number | null): Promise<void> {
    if (limit === null) this.manager.clearConcurrency(queue);
    else this.manager.setConcurrency(queue, limit);
    return Promise.resolve();
  }

  setStallConfig(queue: string, patch: Record<string, unknown>): Promise<void> {
    this.manager.setStallConfig(queue, patch);
    return Promise.resolve();
  }

  setDlqConfig(queue: string, patch: Record<string, unknown>): Promise<void> {
    this.manager.setDlqConfig(queue, patch);
    return Promise.resolve();
  }

  listJobs(
    queue: string,
    options: { state?: string | string[]; start?: number; end?: number; asc?: boolean } = {}
  ) {
    return Promise.resolve(this.manager.getJobs(queue, options));
  }

  async getJob(id: JobId) {
    return await this.manager.getJob(id);
  }

  getLogs(id: JobId) {
    return Promise.resolve(this.manager.getLogs(id));
  }

  getResult(id: JobId): Promise<unknown> {
    return Promise.resolve(this.manager.getResult(id) ?? null);
  }

  clearLogs(id: JobId, keepLogs?: number): Promise<void> {
    this.manager.clearLogs(id, keepLogs);
    return Promise.resolve();
  }

  retryDlq(queue: string, id?: JobId): Promise<number> {
    return Promise.resolve(this.manager.retryDlq(queue, id));
  }

  purgeDlq(queue: string): Promise<number> {
    return Promise.resolve(this.manager.purgeDlq(queue));
  }

  async push(queue: string, input: Parameters<QueueManager['push']>[1]) {
    return await this.manager.push(queue, input);
  }

  async cancel(id: JobId): Promise<boolean> {
    return await this.manager.cancel(id);
  }

  async promote(id: JobId): Promise<boolean> {
    return await this.manager.promote(id);
  }

  async changePriority(id: JobId, priority: number): Promise<boolean> {
    return await this.manager.changePriority(id, priority);
  }

  async discard(id: JobId): Promise<boolean> {
    return await this.manager.discard(id);
  }

  async changeDelay(id: JobId, delay: number): Promise<boolean> {
    return await this.manager.changeDelay(id, delay);
  }

  async updateData(id: JobId, data: unknown): Promise<boolean> {
    return await this.manager.updateJobData(id, data);
  }

  addCron(input: CronJobInput) {
    return Promise.resolve(this.manager.addCron(input));
  }

  removeCron(name: string): Promise<boolean> {
    return Promise.resolve(this.manager.removeCron(name));
  }

  readSnapshotSource(requestedQueues: readonly string[] = []): Promise<CloudSnapshotSource> {
    const queueNames = new Set([...this.manager.listQueues(), ...requestedQueues]);
    const counts = this.manager.getQueueJobCountsBatch(queueNames);
    const queues: CloudSnapshotSource['queues'][number][] = [];
    const jobs: CloudSnapshotSource['jobs'][number][] = [];
    const dlqEntries: CloudSnapshotSource['dlqEntries'][number][] = [];
    for (const name of queueNames) {
      try {
        const queueCounts = counts.get(name);
        if (!queueCounts) continue;
        const limits = this.manager.getQueueLimits(name);
        queues.push({
          name,
          counts: queueCounts,
          paused: this.manager.isPaused(name),
          rateLimit: limits.rateLimit,
          concurrencyLimit: limits.concurrencyLimit,
          stallConfig: this.manager.getStallConfig(name),
          dlqConfig: this.manager.getDlqConfig(name),
        });
      } catch {
        // Preserve the local collector's per-queue best-effort behavior.
      }
      for (const state of SNAPSHOT_STATES) {
        try {
          for (const job of this.manager.getJobs(name, { state: [state], start: 0, end: 1000 })) {
            jobs.push({ job, state });
          }
        } catch {
          // Preserve the local collector's per-state best-effort behavior.
        }
      }
      try {
        dlqEntries.push(...this.manager.getDlqEntries(name));
      } catch {
        // Preserve the local collector's per-queue best-effort behavior.
      }
    }
    const jobResults = new Map<string, unknown>();
    for (const [id, result] of this.manager.getAllJobResults()) {
      jobResults.set(String(id), result);
    }
    const jobLogs = new Map<string, ReturnType<QueueManager['getLogs']>>();
    for (const [id, logs] of this.manager.getAllJobLogs()) jobLogs.set(String(id), logs);
    return Promise.resolve({
      stats: this.manager.getStats(),
      queues,
      jobs,
      dlqEntries,
      workers: this.manager.workerManager.list(),
      workerStats: this.manager.workerManager.getStats(),
      crons: this.manager.listCrons(),
      jobResults,
      jobLogs,
      activeLocks: [...this.manager.getAllJobLocks().values()].map((lock) => ({
        ...lock,
        jobId: String(lock.jobId),
      })),
    });
  }
}
