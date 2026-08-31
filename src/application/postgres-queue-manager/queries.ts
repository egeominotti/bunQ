import {
  FailureReason,
  type DlqEntry,
  type DlqFilter,
  type DlqStats,
} from '../../domain/types/dlq';
import type { Job, JobId } from '../../domain/types/job';
import type { PostgresCounts } from '../../infrastructure/persistence/postgres';
import { PostgresQueueManagerGroupQueries } from './groupQueries';

function jobCounts(
  counts: PostgresCounts,
  totals?: { totalCompleted: bigint; totalFailed: bigint }
) {
  return {
    waiting: counts.waiting,
    prioritized: counts.prioritized,
    delayed: counts.delayed,
    active: counts.active,
    completed: counts.completed,
    failed: counts.failed,
    'waiting-children': counts.waitingChildren,
    totalCompleted: Number(totals?.totalCompleted ?? 0n),
    totalFailed: Number(totals?.totalFailed ?? 0n),
  };
}

export class PostgresQueueManagerQueries extends PostgresQueueManagerGroupQueries {
  override getJobIndex() {
    return this.postgresSnapshot.locations();
  }

  override getCompletedJobs() {
    return this.postgresSnapshot.completedIds();
  }

  override getDepCompletions() {
    return this.postgresSnapshot.completionIds();
  }

  override async getJob(jobId: JobId): Promise<Job | null> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const row = await this.fetchPostgresJob(jobId);
      return row?.job ?? null;
    });
  }

  override async getJobState(jobId: JobId): Promise<string> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      const row = await this.fetchPostgresJob(jobId);
      return row?.state ?? 'unknown';
    });
  }

  override getResult(jobId: JobId): unknown {
    return this.postgresSnapshot.getResult(jobId);
  }

  override async getCompletionAsync(jobId: JobId): Promise<{ found: boolean; result: unknown }> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getResult(jobId);
    });
  }

  override async getResultAsync(jobId: JobId): Promise<unknown> {
    return (await this.getCompletionAsync(jobId)).result;
  }

  override async waitForJobCompletion(jobId: JobId, timeoutMs: number): Promise<boolean> {
    const controller = new AbortController();
    const waiting = super.waitForJobCompletion(jobId, timeoutMs, controller.signal);
    try {
      const completed = (await this.getCompletionAsync(jobId)).found;
      return completed || (await waiting);
    } finally {
      controller.abort();
    }
  }

  override async getChildrenValues(parentJobId: JobId): Promise<Record<string, unknown>> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return await this.postgresStore.getChildrenValues(parentJobId);
    });
  }

  override getJobByCustomId(customId: string): Job | null {
    return this.postgresSnapshot.getByCustomId(customId);
  }

  override getProgress(jobId: JobId): { progress: number; message: string | null } | null {
    const row = this.postgresSnapshot.get(jobId);
    if (row?.state !== 'active') return null;
    return { progress: row.job.progress, message: row.job.progressMessage };
  }

  override getJobs(
    queue: string,
    options: { state?: string | string[]; start?: number; end?: number; asc?: boolean } = {}
  ): Job[] {
    return this.postgresSnapshot.list(queue, options);
  }

  override count(queue: string): number {
    const counts = this.postgresSnapshot.counts(queue);
    return counts.waiting + counts.prioritized + counts.delayed;
  }

  override listQueues(): string[] {
    return this.postgresSnapshot.queueNames();
  }

  override getCountsPerPriority(queue: string): Record<number, number> {
    return this.postgresSnapshot.priorities(queue);
  }

  override getDlq(queue: string, count?: number): Job[] {
    return this.postgresSnapshot
      .dlq(queue, count === undefined ? undefined : { limit: count })
      .map((entry) => entry.job);
  }

  override getDlqEntries(queue: string, filter?: DlqFilter): DlqEntry[] {
    return this.postgresSnapshot.dlq(queue, filter);
  }

  override getDlqCount(queue: string): number {
    return this.postgresSnapshot.counts(queue).failed;
  }

  override getDlqStats(queue: string): DlqStats {
    const entries = this.postgresSnapshot.dlq(queue);
    const now = Date.now();
    const byReason: Record<FailureReason, number> = {
      [FailureReason.ExplicitFail]: 0,
      [FailureReason.MaxAttemptsExceeded]: 0,
      [FailureReason.Timeout]: 0,
      [FailureReason.Stalled]: 0,
      [FailureReason.TtlExpired]: 0,
      [FailureReason.WorkerLost]: 0,
      [FailureReason.Unknown]: 0,
    };
    for (const entry of entries) byReason[entry.reason]++;
    return {
      total: entries.length,
      byReason,
      byQueue: { [queue]: entries.length },
      pendingRetry: entries.filter(
        (entry) => entry.nextRetryAt !== null && entry.nextRetryAt <= now
      ).length,
      expired: entries.filter((entry) => entry.expiresAt !== null && entry.expiresAt <= now).length,
      oldestEntry:
        entries.length === 0 ? null : Math.min(...entries.map((entry) => entry.enteredAt)),
      newestEntry:
        entries.length === 0 ? null : Math.max(...entries.map((entry) => entry.enteredAt)),
    };
  }

  override isPaused(queue: string): boolean {
    return this.postgresSnapshot.queueState(queue)?.paused ?? false;
  }

  async isPausedDurable(queue: string): Promise<boolean> {
    return await this.runPostgresOperation(async () => {
      await this.postgresReady;
      return (await this.postgresStore.getQueueState(queue)).paused;
    });
  }

  override getQueueLimits(queue: string): {
    rateLimit: number | null;
    concurrencyLimit: number | null;
  } {
    const state = this.postgresSnapshot.queueState(queue);
    const rateExpired = state?.rateExpiresAt !== null && (state?.rateExpiresAt ?? 0) <= Date.now();
    return {
      rateLimit: rateExpired ? null : (state?.rateLimit ?? null),
      concurrencyLimit: state?.concurrencyLimit ?? null,
    };
  }

  override getQueueLimitStatus(queue: string, _maxJobs?: number) {
    const state = this.postgresSnapshot.queueState(queue);
    const active = this.postgresSnapshot.counts(queue).active;
    const concurrencyLimit = state?.concurrencyLimit ?? null;
    const duration = state?.rateDurationMs ?? 1000;
    const rateExpired = state?.rateExpiresAt !== null && (state?.rateExpiresAt ?? 0) <= Date.now();
    const rateLimitTtl = state?.rateWindowStartedAt
      ? Math.max(0, state.rateWindowStartedAt + duration - Date.now())
      : 0;
    return {
      rateLimit:
        rateExpired || state?.rateLimit === null || state?.rateLimit === undefined
          ? null
          : { max: state.rateLimit, duration },
      rateLimitTtl,
      concurrencyLimit,
      maxed: concurrencyLimit !== null && active >= concurrencyLimit,
    };
  }

  override getQueueJobCounts(queueName: string) {
    return jobCounts(this.postgresSnapshot.counts(queueName), this.perQueueMetrics.get(queueName));
  }

  override getQueueJobCountsBatch(queueNames: Iterable<string>) {
    return new Map([...queueNames].map((queue) => [queue, this.getQueueJobCounts(queue)]));
  }

  override getAllQueueJobCounts() {
    return this.getQueueJobCountsBatch(this.listQueues());
  }

  override getPerQueueStats() {
    return new Map(
      this.listQueues().map((queue) => {
        const counts = this.postgresSnapshot.counts(queue);
        return [
          queue,
          {
            waiting: counts.waiting,
            prioritized: counts.prioritized,
            delayed: counts.delayed,
            active: counts.active,
            dlq: counts.failed,
          },
        ];
      })
    );
  }

  override getStats() {
    const counts = this.postgresSnapshot.counts();
    const cron = this.cronScheduler.getStats();
    return {
      waiting: counts.waiting,
      prioritized: counts.prioritized,
      delayed: counts.delayed,
      active: counts.active,
      dlq: counts.failed,
      completed: counts.completed,
      'waiting-children': counts.waitingChildren,
      totalPushed: this.metrics.totalPushed.value,
      totalPulled: this.metrics.totalPulled.value,
      totalCompleted: this.metrics.totalCompleted.value,
      totalFailed: this.metrics.totalFailed.value,
      uptime: Date.now() - this.startTime,
      cronJobs: cron.total,
      cronPending: cron.pending,
    };
  }

  override getQueuesSummary() {
    return this.listQueues().map((name) => {
      const counts = this.postgresSnapshot.counts(name);
      return {
        name,
        paused: this.isPaused(name),
        counts: {
          waiting: counts.waiting,
          prioritized: counts.prioritized,
          active: counts.active,
          completed: counts.completed,
          failed: counts.failed,
          delayed: counts.delayed,
        },
      };
    });
  }

  override getStorageStatus(): { diskFull: boolean; error: string | null; since: number | null } {
    const health = this.postgresStore.health();
    return { diskFull: false, error: health.error, since: health.since };
  }

  override getMemoryStats() {
    return { ...super.getMemoryStats(), jobIndex: this.postgresSnapshot.size() };
  }

  override flushPersistence(): number {
    return 0;
  }
}
