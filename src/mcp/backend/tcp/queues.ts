import type { JobCounts } from '../../types/adapter';
import { TcpJobBackend } from './jobs';

export class TcpQueueBackend extends TcpJobBackend {
  async getJobs(queue: string, opts?: { state?: string; start?: number; end?: number }) {
    const start = opts?.start ?? 0;
    const hasEnd = opts?.end !== undefined && opts.end >= 0;
    const response = await this.send({
      cmd: 'GetJobs',
      queue,
      state: opts?.state,
      offset: start,
      limit: hasEnd ? Math.max(0, (opts?.end as number) - start) : undefined,
    });
    return ((response.jobs as Array<Record<string, unknown>>) ?? []).map((job) =>
      this.parseJob(job)
    );
  }

  async getJobCounts(queue: string): Promise<JobCounts> {
    const response = await this.send({ cmd: 'GetJobCounts', queue });
    const counts = (response.counts as Record<string, number> | undefined) ?? {};
    return {
      waiting: counts.waiting ?? 0,
      prioritized: counts.prioritized ?? 0,
      delayed: counts.delayed ?? 0,
      active: counts.active ?? 0,
      completed: counts.completed ?? 0,
      failed: counts.failed ?? 0,
      paused: counts.paused ?? 0,
    };
  }

  async pauseQueue(queue: string) {
    await this.send({ cmd: 'Pause', queue });
  }

  async resumeQueue(queue: string) {
    await this.send({ cmd: 'Resume', queue });
  }

  async drainQueue(queue: string) {
    const response = await this.send({ cmd: 'Drain', queue });
    return (response.removed as number) ?? 0;
  }

  async obliterateQueue(queue: string) {
    await this.send({ cmd: 'Obliterate', queue });
  }

  async listQueues() {
    const response = await this.send({ cmd: 'ListQueues' });
    return (response.queues as string[]) ?? [];
  }

  async countJobs(queue: string) {
    const response = await this.send({ cmd: 'Count', queue });
    return (response.count as number) ?? 0;
  }

  async cleanQueue(queue: string, graceMs: number, state?: string, limit?: number) {
    const response = await this.send({ cmd: 'Clean', queue, grace: graceMs, state, limit });
    return (response.ids as string[]) ?? [];
  }

  async isPaused(queue: string) {
    const response = await this.send({ cmd: 'IsPaused', queue });
    return (response.paused as boolean) ?? false;
  }

  async getCountsPerPriority(queue: string) {
    const response = await this.send({ cmd: 'GetCountsPerPriority', queue });
    return (response.counts as Record<number, number>) ?? {};
  }

  async getDlq(queue: string, limit?: number) {
    const response = await this.send({ cmd: 'Dlq', queue, count: limit });
    return ((response.jobs as Array<Record<string, unknown>>) ?? []).map((job) =>
      this.parseJob(job)
    );
  }

  async retryDlq(queue: string, id?: string) {
    const response = await this.send({ cmd: 'RetryDlq', queue, id });
    return (response.retried as number) ?? 0;
  }

  async purgeDlq(queue: string) {
    const response = await this.send({ cmd: 'PurgeDlq', queue });
    return (response.purged as number) ?? 0;
  }

  async retryCompleted(queue: string, id?: string) {
    const response = await this.send({ cmd: 'RetryCompleted', queue, id });
    return (response.retried as number) ?? 0;
  }

  async setRateLimit(queue: string, limit: number) {
    await this.send({ cmd: 'RateLimit', queue, limit });
  }

  async clearRateLimit(queue: string) {
    await this.send({ cmd: 'RateLimitClear', queue });
  }

  async setConcurrency(queue: string, limit: number) {
    await this.send({ cmd: 'SetConcurrency', queue, limit });
  }

  async clearConcurrency(queue: string) {
    await this.send({ cmd: 'ClearConcurrency', queue });
  }
}
