import type { CronJob, CronJobInput } from '../../domain/types/cron';
import type { JobId } from '../../domain/types/job';
import type { JobEvent } from '../../domain/types/queue';
import type { CreateWorkerOptions } from '../../domain/types/worker';
import { shardIndex } from '../../shared/hash';
import { QueueManagerObservability } from './observability';

export class QueueManagerServices extends QueueManagerObservability {
  addCron(input: CronJobInput): CronJob {
    if (input.preventOverlap) {
      this.removeOrphanedCronJob(input.queue, input.uniqueKey ?? `cron:${input.name}`);
    }
    const cron = this.cronScheduler.add(input);
    this.storage?.saveCron(cron);
    return cron;
  }

  protected removeOrphanedCronJob(queue: string, uniqueKey: string): void {
    const index = shardIndex(queue);
    const shard = this.shards[index];
    const entry = shard.getUniqueKeyEntry(queue, uniqueKey);
    if (!entry) return;
    const targetId = entry.jobId;
    if (this.jobIndex.get(targetId)?.type !== 'queue') return;
    const job = shard.getQueue(queue).remove(targetId);
    if (!job) return;
    shard.decrementQueued(targetId);
    shard.releaseUniqueKey(queue, uniqueKey);
    this.jobIndex.delete(targetId);
    this.storage?.deleteJob(targetId);
  }

  removeCron(name: string): boolean {
    const removed = this.cronScheduler.remove(name);
    if (removed) this.storage?.deleteCron(name);
    return removed;
  }

  getCron(name: string): CronJob | undefined {
    return this.cronScheduler.get(name);
  }

  listCrons(): CronJob[] {
    return this.cronScheduler.list();
  }

  setDashboardEmit(fn: (event: string, data: Record<string, unknown>) => void): void {
    this.dashboardEmit = fn;
    this.cronScheduler.setDashboardEmit(fn);
    this.webhookManager.setDashboardEmit(fn);
    this.workerManager.setDashboardEmit(fn);
    if (this.recoveryStats) {
      fn('server:recovered', this.recoveryStats);
      this.recoveryStats = null;
    }
  }

  emitDashboardEvent(event: string, data: Record<string, unknown>): void {
    this.dashboardEmit?.(event, data);
  }

  subscribe(callback: (event: JobEvent) => void): () => void {
    return this.eventsManager.subscribe(callback);
  }

  waitForJobCompletion(jobId: JobId, timeoutMs: number, signal?: AbortSignal): Promise<boolean> {
    return this.eventsManager.waitForJobCompletion(jobId, timeoutMs, signal);
  }

  registerWorker(
    name: string,
    queues: string[],
    concurrency?: number,
    options?: CreateWorkerOptions
  ) {
    const worker = this.workerManager.register(name, queues, concurrency, options);
    this.dashboardEmit?.('worker:connected', {
      workerId: worker.id,
      name: worker.name,
      queues: worker.queues,
      hostname: worker.hostname,
      pid: worker.pid,
    });
    return worker;
  }

  unregisterWorker(workerId: string): boolean {
    const removed = this.workerManager.unregister(workerId);
    if (removed) this.dashboardEmit?.('worker:disconnected', { workerId });
    return removed;
  }

  unregisterWorkersByClientId(clientId: string): number {
    return this.workerManager.unregisterByClientId(clientId);
  }
}
