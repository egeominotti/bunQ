import type { CronJob, CronJobInput } from '../../domain/types/cron';
import type { JobId } from '../../domain/types/job';
import type { QueueMetrics, QueueMetricType } from '../../domain/types/metrics';
import type { CreateWorkerOptions, Worker } from '../../domain/types/worker';
import { PostgresQueueManagerControl } from './control';

/** Durable PostgreSQL-backed broker services shared across every broker instance. */
export class PostgresQueueManagerServices extends PostgresQueueManagerControl {
  override registerWorker(
    name: string,
    queues: string[],
    concurrency?: number,
    options?: CreateWorkerOptions
  ): Worker {
    return this.operations.runSync(() => {
      const worker = super.registerWorker(name, queues, concurrency, options);
      this.enqueueWrite(() => this.postgresStore.saveWorker(worker));
      return worker;
    });
  }

  async registerWorkerDurable(
    name: string,
    queues: string[],
    concurrency?: number,
    options?: CreateWorkerOptions
  ): Promise<Worker> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const worker = super.registerWorker(name, queues, concurrency, options);
      try {
        await this.postgresStore.saveWorker(worker);
        return worker;
      } catch (error) {
        super.unregisterWorker(worker.id);
        throw error;
      }
    });
  }

  override unregisterWorker(id: string): boolean {
    const admitted = this.operations.tryRunSync(() => {
      const removed = super.unregisterWorker(id);
      this.enqueueWrite(() => this.postgresStore.removeWorker(id).then(() => undefined));
      return removed;
    });
    return admitted.accepted ? admitted.value : super.unregisterWorker(id);
  }

  async unregisterWorkerDurable(id: string, clientId?: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.removeWorker(id, clientId);
      super.unregisterWorker(id);
      return removed;
    });
  }

  override unregisterWorkersByClientId(clientId: string): number {
    const admitted = this.operations.tryRunSync(() => {
      const removed = super.unregisterWorkersByClientId(clientId);
      this.enqueueWrite(() =>
        this.postgresStore.removeClientWorkers(clientId).then(() => undefined)
      );
      return removed;
    });
    return admitted.accepted ? admitted.value : super.unregisterWorkersByClientId(clientId);
  }

  async heartbeatWorkerDurable(
    id: string,
    stats?: { activeJobs?: number; processed?: number; failed?: number },
    clientId?: string
  ): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const updated = await this.postgresStore.heartbeatWorker(id, stats, clientId);
      if (updated) this.workerManager.heartbeat(id, stats);
      return updated;
    });
  }

  async listWorkersDurable(): Promise<Worker[]> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.listWorkers();
    });
  }

  async findMissingDependenciesDurable(ids: readonly JobId[]): Promise<JobId[]> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.findMissingDependencies(ids);
    });
  }

  override addCron(input: CronJobInput): CronJob {
    return this.operations.runSync(() => {
      const cron = this.cronScheduler.add(input);
      this.enqueueWrite(async () => {
        const stored = await this.postgresStore.addCron(input);
        cron.executions = stored.executions;
        cron.nextRun = stored.nextRun;
      });
      return cron;
    });
  }

  async addCronDurable(input: CronJobInput): Promise<CronJob> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const stored = await this.postgresStore.addCron(input);
      const local = this.cronScheduler.add(input);
      local.executions = stored.executions;
      local.nextRun = stored.nextRun;
      return stored;
    });
  }

  override removeCron(name: string): boolean {
    return this.operations.runSync(() => {
      const removed = this.cronScheduler.remove(name);
      this.enqueueWrite(() => this.postgresStore.removeCron(name).then(() => undefined));
      return removed;
    });
  }

  async removeCronDurable(name: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const removed = await this.postgresStore.removeCron(name);
      if (removed) this.cronScheduler.remove(name);
      return removed;
    });
  }

  async getCronDurable(name: string): Promise<CronJob | undefined> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getCron(name);
    });
  }

  async listCronsDurable(): Promise<CronJob[]> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.listCrons();
    });
  }

  async addLogDurable(
    id: JobId,
    message: string,
    level: 'info' | 'warn' | 'error' = 'info'
  ): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.addLog(id, message, level);
    });
  }

  async getLogsDurable(id: JobId) {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getLogs(id);
    });
  }

  async clearLogsDurable(id: JobId, keepLogs?: number): Promise<void> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      await this.postgresStore.clearLogs(id, keepLogs);
    });
  }

  async getQueueMetricsDurable(
    queue: string,
    type: QueueMetricType,
    start = 0,
    end = -1
  ): Promise<QueueMetrics> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getQueueMetrics(queue, type, start, end);
    });
  }

  async trimQueueEventsDurable(queue: string, maxLength: number): Promise<number> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.trimQueueEvents(queue, maxLength);
    });
  }
}
