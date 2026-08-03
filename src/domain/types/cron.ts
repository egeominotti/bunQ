/**
 * Cron job domain types
 */

import { normalizeJobPayload } from '../job/payload';

/** Deduplication config for cron-spawned jobs */
export interface CronDedup {
  readonly ttl?: number;
  readonly extend?: boolean;
  readonly replace?: boolean;
}

/**
 * Job options applied to every job spawned by a cron (issue #86).
 * Mirrors the relevant subset of JobInput so retry/cleanup policy can be
 * configured per-scheduler instead of always falling back to JOB_DEFAULTS.
 */
export interface CronJobOptions {
  readonly maxAttempts?: number;
  readonly backoff?: number | { type: 'fixed' | 'exponential'; delay: number };
  readonly timeout?: number;
  readonly delay?: number;
  readonly stallTimeout?: number;
  readonly removeOnComplete?: boolean;
  readonly removeOnFail?: boolean;
}

/** Cron job definition */
export interface CronJob {
  readonly name: string;
  readonly jobName: string;
  readonly queue: string;
  readonly data: unknown;
  readonly schedule: string | null;
  readonly repeatEvery: number | null;
  readonly priority: number;
  /** IANA timezone for cron schedule (e.g., "Europe/Rome", "America/New_York") */
  readonly timezone: string | null;
  nextRun: number;
  executions: number;
  readonly maxLimit: number | null;
  /** Unique key for deduplication of cron-spawned jobs */
  readonly uniqueKey: string | null;
  /** Deduplication options for cron-spawned jobs */
  readonly dedup: CronDedup | null;
  /** Skip missed runs on restart instead of executing them (default: true) */
  readonly skipMissedOnRestart: boolean;
  /** Skip job push if no worker is registered for the queue (default: false) */
  readonly skipIfNoWorker: boolean;
  /** Prevent overlapping cron jobs via automatic dedup (default: true) */
  readonly preventOverlap: boolean;
  /** Job options applied to each spawned job (retry/cleanup policy, etc.) */
  readonly jobOptions: CronJobOptions | null;
}

/** Input for creating a cron job */
export interface CronJobInput {
  name: string;
  jobName?: string;
  queue: string;
  data: unknown;
  schedule?: string;
  repeatEvery?: number;
  priority?: number;
  maxLimit?: number;
  /** IANA timezone for cron schedule (e.g., "Europe/Rome", "America/New_York") */
  timezone?: string;
  /** Unique key for deduplication of cron-spawned jobs */
  uniqueKey?: string;
  /** Deduplication options for cron-spawned jobs */
  dedup?: CronDedup;
  /** Skip missed runs on restart instead of executing them (default: true) */
  skipMissedOnRestart?: boolean;
  /** Fire immediately on creation, then continue on schedule */
  immediately?: boolean;
  /** Skip job push if no worker is registered for the queue (default: false) */
  skipIfNoWorker?: boolean;
  /** Prevent overlapping cron jobs via automatic dedup (default: true) */
  preventOverlap?: boolean;
  /** Job options applied to each spawned job (retry/cleanup policy, etc.) */
  jobOptions?: CronJobOptions;
}

/** Create a new cron job */
export function createCronJob(input: CronJobInput, nextRun: number): CronJob {
  if (!input.schedule && !input.repeatEvery) {
    throw new Error('Cron job must have either schedule or repeatEvery');
  }

  const payload = normalizeJobPayload({ name: input.jobName, data: input.data });

  return {
    name: input.name,
    jobName: payload.name,
    queue: input.queue,
    data: input.data,
    schedule: input.schedule ?? null,
    repeatEvery: input.repeatEvery ?? null,
    priority: input.priority ?? 0,
    timezone: input.timezone ?? null,
    nextRun,
    executions: 0,
    // 0/negative mean "no limit" on every surface (CLI/HTTP/TCP/MCP): storing
    // 0 would make isAtLimit treat the cron as already exhausted (0 >= 0).
    maxLimit: input.maxLimit !== undefined && input.maxLimit > 0 ? input.maxLimit : null,
    uniqueKey: input.uniqueKey ?? null,
    dedup: input.dedup ?? null,
    skipMissedOnRestart: input.skipMissedOnRestart ?? true,
    skipIfNoWorker: input.skipIfNoWorker ?? false,
    preventOverlap: input.preventOverlap ?? true,
    jobOptions: input.jobOptions ?? null,
  };
}

/** Check if cron job has reached its execution limit */
export function isAtLimit(cron: CronJob): boolean {
  if (cron.maxLimit === null) return false;
  return cron.executions >= cron.maxLimit;
}

/** Check if cron job is due to run */
export function isDue(cron: CronJob, now: number = Date.now()): boolean {
  return cron.nextRun <= now && !isAtLimit(cron);
}
