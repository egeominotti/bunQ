import type { CronJobInput } from '../../../domain/types/cron';
import type { JobLogEntry } from '../../../domain/types/worker';
import type { SerializedCron, WebhookInfo, WorkerInfo } from '../../types/adapter';
import { TcpQueueBackend } from './queues';

function serializeTcpCron(value: unknown): SerializedCron {
  if (!value || typeof value !== 'object') throw new Error('Invalid cron response from broker');
  const cron = value as Record<string, unknown>;
  const schedule = cron.schedule;
  const repeatEvery = cron.repeatEvery;
  if (
    typeof cron.name !== 'string' ||
    typeof cron.queue !== 'string' ||
    typeof cron.nextRun !== 'number' ||
    !Number.isFinite(cron.nextRun) ||
    (schedule != null && typeof schedule !== 'string') ||
    (repeatEvery != null && (typeof repeatEvery !== 'number' || !Number.isFinite(repeatEvery)))
  ) {
    throw new Error('Invalid cron response from broker');
  }
  const nextRun = new Date(cron.nextRun);
  if (Number.isNaN(nextRun.getTime())) throw new Error('Invalid cron response from broker');
  return {
    name: cron.name,
    queue: cron.queue,
    schedule: schedule ?? undefined,
    repeatEvery: repeatEvery ?? undefined,
    nextRun: nextRun.toISOString(),
    executions: typeof cron.executions === 'number' ? cron.executions : 0,
  };
}

export class TcpServiceBackend extends TcpQueueBackend {
  async addCron(input: CronJobInput): Promise<SerializedCron> {
    const response = await this.send({ cmd: 'Cron', ...input });
    if (response.ok !== true) {
      throw new Error(typeof response.error === 'string' ? response.error : 'Failed to add cron');
    }
    return serializeTcpCron(response.cron);
  }

  async listCrons(): Promise<SerializedCron[]> {
    const response = await this.send({ cmd: 'CronList' });
    return ((response.crons as unknown[]) ?? []).map(serializeTcpCron);
  }

  async getCron(name: string): Promise<SerializedCron | null> {
    const response = await this.send({ cmd: 'CronGet', name });
    if (!response.ok) return null;
    return serializeTcpCron(response.cron);
  }

  async deleteCron(name: string) {
    const response = await this.send({ cmd: 'CronDelete', name });
    return (response.ok as boolean) ?? true;
  }

  async addWebhook(url: string, events: string[], queue?: string): Promise<WebhookInfo> {
    const response = await this.send({ cmd: 'AddWebhook', url, events, queue });
    return { id: String(response.id ?? '0'), url, events, queue, enabled: true };
  }

  async removeWebhook(id: string) {
    const response = await this.send({ cmd: 'RemoveWebhook', id });
    return (response.ok as boolean) ?? true;
  }

  async listWebhooks(): Promise<WebhookInfo[]> {
    const response = await this.send({ cmd: 'ListWebhooks' });
    return ((response.webhooks as Array<Record<string, unknown>>) ?? []).map((webhook) => ({
      id: String(webhook.id),
      url: webhook.url as string,
      events: (webhook.events as string[]) ?? [],
      queue: webhook.queue as string | undefined,
      enabled: (webhook.enabled as boolean) ?? true,
    }));
  }

  async setWebhookEnabled(id: string, enabled: boolean) {
    const response = await this.send({ cmd: 'SetWebhookEnabled', id, enabled });
    return (response.ok as boolean) ?? true;
  }

  async registerWorker(name: string, queues: string[]): Promise<WorkerInfo> {
    const response = await this.send({ cmd: 'RegisterWorker', name, queues });
    return {
      id: String(response.id ?? '0'),
      name,
      queues,
      active: 0,
      processed: 0,
      failed: 0,
      lastHeartbeat: Date.now(),
    };
  }

  async unregisterWorker(id: string) {
    const response = await this.send({ cmd: 'UnregisterWorker', workerId: id });
    return (response.ok as boolean) ?? true;
  }

  async workerHeartbeat(id: string) {
    const response = await this.send({ cmd: 'Heartbeat', id });
    return (response.ok as boolean) ?? true;
  }

  async listWorkers(): Promise<WorkerInfo[]> {
    const response = await this.send({ cmd: 'ListWorkers' });
    const data = response.data as { workers?: Array<Record<string, unknown>> } | undefined;
    const workers = data?.workers ?? (response.workers as Array<Record<string, unknown>>) ?? [];
    return workers.map((worker) => ({
      id: String(worker.id),
      name: worker.name as string,
      queues: (worker.queues as string[]) ?? [],
      active: (worker.activeJobs as number) ?? 0,
      processed: (worker.processedJobs as number) ?? (worker.processed as number) ?? 0,
      failed: (worker.failedJobs as number) ?? (worker.failed as number) ?? 0,
      lastHeartbeat: (worker.lastSeen as number) ?? (worker.lastHeartbeat as number) ?? 0,
    }));
  }

  async getStats(): Promise<Record<string, unknown>> {
    return this.send({ cmd: 'Stats' });
  }

  async getJobLogs(id: string): Promise<JobLogEntry[]> {
    const response = await this.send({ cmd: 'GetLogs', id });
    return (response.logs as JobLogEntry[]) ?? [];
  }

  async addJobLog(id: string, message: string, level?: 'info' | 'warn' | 'error') {
    const response = await this.send({ cmd: 'AddLog', id, message, level });
    return (response.ok as boolean) ?? true;
  }

  async getPerQueueStats(): Promise<Record<string, unknown>> {
    const response = await this.send({ cmd: 'DashboardQueues' });
    const data = response.data as { queues?: Array<Record<string, unknown>> } | undefined;
    const result: Record<string, unknown> = {};
    for (const queue of data?.queues ?? []) {
      const name = queue.name;
      if (typeof name !== 'string') continue;
      result[name] = {
        waiting: (queue.waiting as number) ?? 0,
        prioritized: (queue.prioritized as number) ?? 0,
        delayed: (queue.delayed as number) ?? 0,
        active: (queue.active as number) ?? 0,
        dlq: (queue.dlq as number) ?? 0,
      };
    }
    return result;
  }

  async getMemoryStats(): Promise<Record<string, unknown>> {
    const response = await this.send({ cmd: 'Stats' });
    return (response.memory as Record<string, unknown>) ?? {};
  }

  async getPrometheusMetrics() {
    const response = await this.send({ cmd: 'Prometheus' });
    return (response.metrics as string) ?? '';
  }

  async clearJobLogs(id: string, _keepLogs?: number) {
    await this.send({ cmd: 'ClearLogs', id });
  }

  async compactMemory() {
    await this.send({ cmd: 'CompactMemory' });
  }

  async getStorageStatus() {
    const response = await this.send({ cmd: 'StorageStatus' });
    const data = response.data as Record<string, unknown> | undefined;
    return {
      diskFull: (data?.diskFull as boolean) ?? false,
      error: (data?.error as string | null) ?? null,
    };
  }
}
