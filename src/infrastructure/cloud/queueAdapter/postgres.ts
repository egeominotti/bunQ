import { PostgresQueueManager } from '../../../application/postgresQueueManager';
import type { CronJobInput } from '../../../domain/types/cron';
import { DEFAULT_DLQ_CONFIG } from '../../../domain/types/dlq';
import type { JobId } from '../../../domain/types/job';
import { DEFAULT_STALL_CONFIG } from '../../../domain/types/stall';
import type { PostgresCounts, PostgresQueueState } from '../../persistence/postgres';
import type { PostgresCloudReadModel } from '../../persistence/postgres/cloudReadModel';
import type { CloudQueueAdapter, CloudQueueCounts, CloudSnapshotSource } from './types';

const EMPTY_COUNTS: PostgresCounts = {
  waiting: 0,
  prioritized: 0,
  delayed: 0,
  active: 0,
  completed: 0,
  failed: 0,
  waitingChildren: 0,
};

type MutableCounts = { -readonly [Key in keyof PostgresCounts]: PostgresCounts[Key] };

function queueCounts(model: PostgresCloudReadModel): Map<string, MutableCounts> {
  const result = new Map<string, MutableCounts>();
  for (const row of model.counts) {
    const counts = result.get(row.queue) ?? { ...EMPTY_COUNTS };
    if (row.state === 'waiting-children') counts.waitingChildren = row.count;
    else counts[row.state] = row.count;
    result.set(row.queue, counts);
  }
  return result;
}

function queueTotals(
  model: PostgresCloudReadModel
): Map<string, { completed: bigint; failed: bigint }> {
  const result = new Map<string, { completed: bigint; failed: bigint }>();
  for (const row of model.totals) {
    const totals = result.get(row.queue) ?? { completed: 0n, failed: 0n };
    totals[row.metricType] = row.count;
    result.set(row.queue, totals);
  }
  return result;
}

function cloudCounts(
  counts: PostgresCounts,
  totals: { completed: bigint; failed: bigint }
): CloudQueueCounts {
  return {
    waiting: counts.waiting,
    prioritized: counts.prioritized,
    delayed: counts.delayed,
    active: counts.active,
    completed: counts.completed,
    failed: counts.failed,
    'waiting-children': counts.waitingChildren,
    totalCompleted: Number(totals.completed),
    totalFailed: Number(totals.failed),
  };
}

function defaultQueueState(queue: string): PostgresQueueState {
  return {
    queue,
    paused: false,
    rateLimit: null,
    rateDurationMs: null,
    rateWindowStartedAt: null,
    rateExpiresAt: null,
    rateCount: 0,
    concurrencyLimit: null,
    stallConfig: DEFAULT_STALL_CONFIG,
    dlqConfig: DEFAULT_DLQ_CONFIG,
  };
}

/** PostgreSQL Cloud strategy; every shared surface goes through durable APIs. */
export class PostgresCloudQueueAdapter implements CloudQueueAdapter {
  constructor(readonly manager: PostgresQueueManager) {}

  pause = (queue: string, paused: boolean) => this.manager.pauseDurable(queue, paused);
  drain = (queue: string) => this.manager.drainDurable(queue);
  clean = (queue: string, graceMs: number, state?: string, limit?: number) =>
    this.manager.cleanDurable(queue, graceMs, state, limit);
  obliterate = (queue: string) => this.manager.obliterateDurable(queue);
  promoteAll = (queue: string, limit: number) => this.manager.promoteJobs(queue, limit);
  retryCompleted = (queue: string) => this.manager.retryCompletedDurable(queue);
  setRateLimit = (queue: string, limit: number | null) =>
    this.manager.setRateLimitDurable(queue, limit, null);
  setConcurrency = (queue: string, limit: number | null) =>
    this.manager.setConcurrencyDurable(queue, limit);
  setStallConfig = (queue: string, patch: Record<string, unknown>) =>
    this.manager.setStallConfigDurable(queue, patch);
  setDlqConfig = (queue: string, patch: Record<string, unknown>) =>
    this.manager.setDlqConfigDurable(queue, patch);
  listJobs = (
    queue: string,
    options?: { state?: string | string[]; start?: number; end?: number; asc?: boolean }
  ) => this.manager.listCloudJobsDurable(queue, options);
  getJob = (id: JobId) => this.manager.getJob(id);
  getLogs = (id: JobId) => this.manager.getLogsDurable(id);
  getResult = (id: JobId) => this.manager.getCloudResultDurable(id);
  clearLogs = (id: JobId, keepLogs?: number) => this.manager.clearLogsDurable(id, keepLogs);
  retryDlq = (queue: string, id?: JobId) => this.manager.retryDlqDurable(queue, id);
  purgeDlq = (queue: string) => this.manager.purgeDlqDurable(queue);
  push = (queue: string, input: Parameters<PostgresQueueManager['push']>[1]) =>
    this.manager.push(queue, input);
  cancel = (id: JobId) => this.manager.cancel(id);
  promote = (id: JobId) => this.manager.promote(id);
  changePriority = (id: JobId, priority: number) => this.manager.changePriority(id, priority);
  discard = (id: JobId) => this.manager.discard(id);
  changeDelay = (id: JobId, delay: number) => this.manager.changeDelay(id, delay);
  updateData = (id: JobId, data: unknown) => this.manager.updateJobData(id, data);
  addCron = (input: CronJobInput) => this.manager.addCronDurable(input);
  removeCron = (name: string) => this.manager.removeCronDurable(name);

  async readSnapshotSource(requestedQueues: readonly string[] = []): Promise<CloudSnapshotSource> {
    const model = await this.manager.readCloudSnapshotDurable();
    const counts = queueCounts(model);
    const totals = queueTotals(model);
    const names = new Set([
      ...model.queueStates.map(({ queue }) => queue),
      ...counts.keys(),
      ...totals.keys(),
      ...model.jobs.map(({ job }) => job.queue),
      ...requestedQueues,
    ]);
    const states = new Map(model.queueStates.map((state) => [state.queue, state]));
    const queues = [...names].sort().map((name) => {
      const state = states.get(name) ?? defaultQueueState(name);
      const rateExpired = state.rateExpiresAt !== null && state.rateExpiresAt <= Date.now();
      return {
        name,
        counts: cloudCounts(
          counts.get(name) ?? EMPTY_COUNTS,
          totals.get(name) ?? { completed: 0n, failed: 0n }
        ),
        paused: state.paused,
        rateLimit: rateExpired ? null : state.rateLimit,
        concurrencyLimit: state.concurrencyLimit,
        stallConfig: state.stallConfig,
        dlqConfig: state.dlqConfig,
      };
    });
    return this.toSnapshotSource(model, queues);
  }

  private toSnapshotSource(
    model: PostgresCloudReadModel,
    queues: CloudSnapshotSource['queues']
  ): CloudSnapshotSource {
    const aggregate = [...queueCounts(model).values()].reduce(
      (sum, counts) => ({
        waiting: sum.waiting + counts.waiting,
        prioritized: sum.prioritized + counts.prioritized,
        delayed: sum.delayed + counts.delayed,
        active: sum.active + counts.active,
        completed: sum.completed + counts.completed,
        failed: sum.failed + counts.failed,
        waitingChildren: sum.waitingChildren + counts.waitingChildren,
      }),
      { ...EMPTY_COUNTS }
    );
    const durableTotals = [...queueTotals(model).values()].reduce(
      (sum, item) => ({
        completed: sum.completed + item.completed,
        failed: sum.failed + item.failed,
      }),
      { completed: 0n, failed: 0n }
    );
    const localStats = this.manager.getCloudProcessStats();
    const now = Date.now();
    const activeWorkers = model.workers.filter((worker) => now - worker.lastSeen < 30_000);
    const workerStats = {
      total: model.workers.length,
      active: activeWorkers.length,
      totalProcessed: model.workers.reduce((sum, worker) => sum + worker.processedJobs, 0),
      totalFailed: model.workers.reduce((sum, worker) => sum + worker.failedJobs, 0),
      activeJobs: model.workers.reduce((sum, worker) => sum + worker.activeJobs, 0),
      concurrencySlots: activeWorkers.reduce((sum, worker) => sum + worker.concurrency, 0),
    };
    const jobResults = new Map(
      model.results.map(({ jobId: id, result }) => [String(id), result] as const)
    );
    return {
      stats: {
        ...localStats,
        waiting: aggregate.waiting,
        prioritized: aggregate.prioritized,
        delayed: aggregate.delayed,
        active: aggregate.active,
        completed: aggregate.completed,
        dlq: aggregate.failed,
        'waiting-children': aggregate.waitingChildren,
        totalCompleted: durableTotals.completed,
        totalFailed: durableTotals.failed,
        cronJobs: model.crons.length,
        cronPending: model.crons.filter((cron) => cron.nextRun <= now).length,
      },
      queues,
      jobs: model.jobs.map((row) => ({ job: row.job, state: row.state })),
      dlqEntries: model.jobs.flatMap((row) => (row.dlqEntry ? [row.dlqEntry] : [])),
      workers: model.workers,
      workerStats,
      crons: model.crons,
      jobResults,
      jobLogs: model.logs,
      activeLocks: model.jobs.flatMap((row) => {
        if (row.state !== 'active' || !row.token || !row.leaseOwner || row.leaseUntil === null) {
          return [];
        }
        const lastRenewalAt = row.job.lastHeartbeat || row.job.startedAt || row.job.createdAt;
        return [
          {
            jobId: String(row.job.id),
            owner: row.leaseOwner,
            token: row.token,
            createdAt: row.job.startedAt ?? row.job.createdAt,
            expiresAt: row.leaseUntil,
            lastRenewalAt,
            renewalCount: row.leaseRenewals,
            ttl: Math.max(0, row.leaseUntil - lastRenewalAt),
          },
        ];
      }),
    };
  }
}
