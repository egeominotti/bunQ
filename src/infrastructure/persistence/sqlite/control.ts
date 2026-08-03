import type { CronJob } from '../../../domain/types/cron';
import type { DlqConfig } from '../../../domain/types/dlq';
import type { StallConfig } from '../../../domain/types/stall';
import { decodeStoredNamedPayload, pack, unpack } from '../sqliteSerializer';
import type { DbCron, DbQueueState } from '../statements';
import { SqliteQueries } from './queries';

export interface PersistedQueueState {
  name: string;
  paused: boolean;
  rateLimit: number | null;
  concurrencyLimit: number | null;
  rateLimitDuration: number | null;
  rateLimitExpiresAt: number | null;
  stallConfig: StallConfig | null;
  dlqConfig: DlqConfig | null;
}

/** Cron schedules and durable queue control-state. */
export abstract class SqliteControl extends SqliteQueries {
  saveCron(cron: CronJob): void {
    this.safeWrite(() => {
      this.statements
        .get('insertCron')!
        .run(
          cron.name,
          cron.queue,
          cron.jobName,
          pack(cron.data),
          cron.schedule,
          cron.repeatEvery,
          cron.priority,
          cron.nextRun,
          cron.executions,
          cron.maxLimit,
          cron.timezone,
          cron.uniqueKey,
          cron.dedup ? pack(cron.dedup) : null,
          cron.skipMissedOnRestart ? 1 : 0,
          cron.skipIfNoWorker ? 1 : 0,
          cron.preventOverlap ? 1 : 0,
          cron.jobOptions ? pack(cron.jobOptions) : null
        );
    });
  }

  loadCronJobs(): CronJob[] {
    const rows = this.db.query<DbCron, []>('SELECT * FROM cron_jobs').all();
    return rows.map((row) => {
      const payload = decodeStoredNamedPayload(row.job_name, row.data, `loadCronJobs:${row.name}`);
      return {
        name: row.name,
        queue: row.queue,
        jobName: payload.name,
        data: payload.data,
        schedule: row.schedule,
        repeatEvery: row.repeat_every,
        priority: row.priority,
        timezone: row.timezone,
        nextRun: row.next_run,
        executions: row.executions,
        maxLimit: row.max_limit,
        uniqueKey: row.unique_key ?? null,
        dedup: row.dedup
          ? (unpack(row.dedup, null, `loadCronDedup:${row.name}`) as CronJob['dedup'])
          : null,
        skipMissedOnRestart: row.skip_missed_on_restart === 1,
        skipIfNoWorker: row.skip_if_no_worker === 1,
        preventOverlap: row.prevent_overlap === 1,
        jobOptions: row.job_options
          ? (unpack(
              row.job_options,
              null,
              `loadCronJobOptions:${row.name}`
            ) as CronJob['jobOptions'])
          : null,
      };
    });
  }

  deleteCron(name: string): void {
    this.safeWrite(() => {
      this.db.prepare('DELETE FROM cron_jobs WHERE name = ?').run(name);
    });
  }

  updateCron(name: string, executions: number, nextRun: number): void {
    this.safeWrite(() => {
      this.statements.get('updateCron')!.run(executions, nextRun, name);
    });
  }

  saveQueueState(
    name: string,
    state: {
      paused: boolean;
      rateLimit: number | null;
      concurrencyLimit: number | null;
      rateLimitDuration?: number | null;
      rateLimitExpiresAt?: number | null;
      stallConfig?: StallConfig | null;
      dlqConfig?: DlqConfig | null;
    }
  ): void {
    this.safeWrite(() => {
      this.statements
        .get('upsertQueueState')!
        .run(
          name,
          state.paused ? 1 : 0,
          state.rateLimit,
          state.concurrencyLimit,
          state.rateLimitDuration ?? null,
          state.rateLimitExpiresAt ?? null,
          state.stallConfig === null || state.stallConfig === undefined
            ? null
            : state.stallConfig.enabled
              ? 1
              : 0,
          state.stallConfig?.stallInterval ?? null,
          state.stallConfig?.maxStalls ?? null,
          state.stallConfig?.gracePeriod ?? null,
          state.dlqConfig ? pack(state.dlqConfig) : null
        );
    });
  }

  loadQueueState(): PersistedQueueState[] {
    const rows = this.statements.get('loadQueueState')!.all() as DbQueueState[];
    return rows.map((row) => ({
      name: row.name,
      paused: row.paused === 1,
      rateLimit: row.rate_limit,
      concurrencyLimit: row.concurrency_limit,
      rateLimitDuration: row.rate_limit_duration ?? null,
      rateLimitExpiresAt: row.rate_limit_expires_at ?? null,
      stallConfig:
        row.stall_enabled === null ||
        row.stall_interval === null ||
        row.max_stalls === null ||
        row.stall_grace_period === null
          ? null
          : {
              enabled: row.stall_enabled === 1,
              stallInterval: row.stall_interval,
              maxStalls: row.max_stalls,
              gracePeriod: row.stall_grace_period,
            },
      dlqConfig: unpack<DlqConfig | null>(row.dlq_config, null, `loadQueueDlqConfig:${row.name}`),
    }));
  }

  deleteQueueState(name: string): void {
    this.safeWrite(() => {
      this.statements.get('deleteQueueState')!.run(name);
    });
  }
}
