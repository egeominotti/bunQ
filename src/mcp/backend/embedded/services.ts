import type { CronJobInput } from '../../../domain/types/cron';
import { jobId as toJobId } from '../../../domain/types/job';
import type { SerializedCron, WebhookInfo, WorkerInfo } from '../../types/adapter';
import { serializeMcpCron } from '../serializers';
import { EmbeddedQueueBackend } from './queues';

export class EmbeddedServiceBackend extends EmbeddedQueueBackend {
  addCron(input: CronJobInput): Promise<SerializedCron> {
    return Promise.resolve(serializeMcpCron(this.manager.addCron(input)));
  }

  listCrons(): Promise<SerializedCron[]> {
    return Promise.resolve(this.manager.listCrons().map(serializeMcpCron));
  }

  getCron(name: string): Promise<SerializedCron | null> {
    const cron = this.manager.getCron(name);
    return Promise.resolve(cron ? serializeMcpCron(cron) : null);
  }

  deleteCron(name: string) {
    return Promise.resolve(this.manager.removeCron(name));
  }

  addWebhook(url: string, events: string[], queue?: string): Promise<WebhookInfo> {
    const webhook = this.manager.webhookManager.add(url, events, queue);
    return Promise.resolve({
      id: String(webhook.id),
      url: webhook.url,
      events: webhook.events as string[],
      queue: webhook.queue ?? undefined,
      enabled: webhook.enabled,
    });
  }

  removeWebhook(id: string) {
    return Promise.resolve(this.manager.webhookManager.remove(id as never));
  }

  listWebhooks(): Promise<WebhookInfo[]> {
    return Promise.resolve(
      this.manager.webhookManager.list().map((webhook) => ({
        id: String(webhook.id),
        url: webhook.url,
        events: webhook.events as string[],
        queue: webhook.queue ?? undefined,
        enabled: webhook.enabled,
      }))
    );
  }

  setWebhookEnabled(id: string, enabled: boolean) {
    return Promise.resolve(this.manager.webhookManager.setEnabled(id as never, enabled));
  }

  registerWorker(name: string, queues: string[]): Promise<WorkerInfo> {
    const worker = this.manager.workerManager.register(name, queues);
    return Promise.resolve({
      id: String(worker.id),
      name: worker.name,
      queues: worker.queues,
      active: worker.activeJobs,
      processed: worker.processedJobs,
      failed: worker.failedJobs,
      lastHeartbeat: worker.lastSeen,
    });
  }

  unregisterWorker(id: string) {
    return Promise.resolve(this.manager.workerManager.unregister(id as never));
  }

  workerHeartbeat(id: string) {
    return Promise.resolve(this.manager.workerManager.heartbeat(id as never));
  }

  listWorkers(): Promise<WorkerInfo[]> {
    return Promise.resolve(
      this.manager.workerManager.list().map((worker) => ({
        id: String(worker.id),
        name: worker.name,
        queues: worker.queues,
        active: worker.activeJobs,
        processed: worker.processedJobs,
        failed: worker.failedJobs,
        lastHeartbeat: worker.lastSeen,
      }))
    );
  }

  getStats(): Promise<Record<string, unknown>> {
    const stats = this.manager.getStats();
    return Promise.resolve(
      JSON.parse(
        JSON.stringify(stats, (_key, value: unknown) =>
          typeof value === 'bigint' ? Number(value) : value
        )
      ) as Record<string, unknown>
    );
  }

  getJobLogs(id: string) {
    return Promise.resolve(this.manager.getLogs(toJobId(id)));
  }

  addJobLog(id: string, message: string, level?: 'info' | 'warn' | 'error') {
    return Promise.resolve(this.manager.addLog(toJobId(id), message, level));
  }

  getPerQueueStats(): Promise<Record<string, unknown>> {
    const stats = this.manager.getPerQueueStats();
    const result: Record<string, unknown> = {};
    for (const [key, value] of stats) result[key] = value;
    return Promise.resolve(result);
  }

  getMemoryStats(): Promise<Record<string, unknown>> {
    return Promise.resolve(this.manager.getMemoryStats() as unknown as Record<string, unknown>);
  }

  getPrometheusMetrics() {
    return Promise.resolve(this.manager.getPrometheusMetrics());
  }

  getStorageStatus() {
    const status = this.manager.getStorageStatus();
    return Promise.resolve({ diskFull: status.diskFull, error: status.error });
  }

  clearJobLogs(id: string, keepLogs?: number) {
    this.manager.clearLogs(toJobId(id), keepLogs);
    return Promise.resolve();
  }

  compactMemory() {
    this.manager.compactMemory();
    return Promise.resolve();
  }
}
