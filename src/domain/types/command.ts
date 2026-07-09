/**
 * Protocol commands
 * All supported commands for TCP/HTTP protocol
 */

import type { JobInput, JobState } from './job';
import type { CronJobOptions } from './cron';

/** Base command interface */
interface BaseCommand {
  readonly cmd: string;
  readonly reqId?: string;
}

// ============ Core Commands ============

export interface PushCommand extends BaseCommand {
  readonly cmd: 'PUSH';
  readonly queue: string;
  readonly data: unknown;
  readonly priority?: number;
  readonly delay?: number;
  readonly maxAttempts?: number;
  readonly backoff?: number | { type: 'fixed' | 'exponential'; delay: number };
  readonly ttl?: number;
  readonly timeout?: number;
  readonly uniqueKey?: string;
  readonly jobId?: string; // customId
  readonly dependsOn?: string[];
  readonly childrenIds?: string[];
  readonly parentId?: string;
  readonly tags?: string[];
  readonly groupId?: string;
  readonly lifo?: boolean;
  readonly removeOnComplete?: boolean;
  readonly removeOnFail?: boolean;
  /** Force immediate persistence to disk (bypass write buffer) */
  readonly durable?: boolean;
  /**
   * Repeat configuration for recurring jobs. The wire object is preserved
   * verbatim by `unpack` and consumed by the domain `parseRepeatConfig`, so
   * the type mirrors `JobInput['repeat']` — under-declaring it (e.g. only
   * `every`/`limit`/`count`) silently hid `pattern`/`tz`/dates from callers.
   */
  readonly repeat?: {
    every?: number;
    limit?: number;
    pattern?: string;
    count?: number;
    startDate?: number;
    endDate?: number;
    tz?: string;
    immediately?: boolean;
    prevMillis?: number;
    offset?: number;
    jobId?: string;
  };
  /** BullMQ v5 flow failure propagation options */
  readonly failParentOnFailure?: boolean;
  readonly removeDependencyOnFailure?: boolean;
  readonly ignoreDependencyOnFailure?: boolean;
  readonly continueParentOnFailure?: boolean;
  /** Stall detection timeout (ms) */
  readonly stallTimeout?: number;
  /** BullMQ v5 options not in the core set */
  readonly stackTraceLimit?: number;
  readonly keepLogs?: number;
  readonly sizeLimit?: number;
  /** Deduplication options (uniqueKey carries the id) */
  readonly dedup?: { ttl?: number; extend?: boolean; replace?: boolean };
  /** Debounce options */
  readonly debounceId?: string;
  readonly debounceTtl?: number;
  /** Explicit creation timestamp */
  readonly timestamp?: number;
}

export interface PushBatchCommand extends BaseCommand {
  readonly cmd: 'PUSHB';
  readonly queue: string;
  readonly jobs: JobInput[];
}

export interface PullCommand extends BaseCommand {
  readonly cmd: 'PULL';
  readonly queue: string;
  readonly timeout?: number;
  readonly owner?: string; // Client identifier for lock ownership
  readonly lockTtl?: number; // Lock TTL in ms (default: 30000)
  readonly detach?: boolean; // Don't auto-release on disconnect (for CLI usage)
}

export interface PullBatchCommand extends BaseCommand {
  readonly cmd: 'PULLB';
  readonly queue: string;
  readonly count: number;
  readonly timeout?: number;
  readonly owner?: string; // Client identifier for lock ownership
  readonly lockTtl?: number; // Lock TTL in ms (default: 30000)
}

export interface AckCommand extends BaseCommand {
  readonly cmd: 'ACK';
  readonly id: string;
  readonly result?: unknown;
  readonly token?: string; // Lock token for ownership verification
}

export interface AckBatchCommand extends BaseCommand {
  readonly cmd: 'ACKB';
  readonly ids: string[];
  readonly results?: unknown[]; // Optional results for each job (same order as ids)
  readonly tokens?: string[]; // Lock tokens for each job (same order as ids)
}

export interface FailCommand extends BaseCommand {
  readonly cmd: 'FAIL';
  readonly id: string;
  readonly error?: string;
  readonly token?: string; // Lock token for ownership verification
  /** Skip all remaining retries and fail terminally (UnrecoverableError over TCP). */
  readonly unrecoverable?: boolean;
  /** Stack trace lines of the failure — persisted server-side, capped at job.stackTraceLimit (#74). */
  readonly stack?: string[];
}

// ============ Query Commands ============

export interface GetJobCommand extends BaseCommand {
  readonly cmd: 'GetJob';
  readonly id: string;
}

export interface GetStateCommand extends BaseCommand {
  readonly cmd: 'GetState';
  readonly id: string;
}

export interface GetResultCommand extends BaseCommand {
  readonly cmd: 'GetResult';
  readonly id: string;
}

export interface GetJobsCommand extends BaseCommand {
  readonly cmd: 'GetJobs';
  readonly queue: string;
  readonly state?: JobState | JobState[];
  readonly limit?: number;
  readonly offset?: number;
}

export interface GetJobCountsCommand extends BaseCommand {
  readonly cmd: 'GetJobCounts';
  readonly queue: string;
}

export interface GetCountsPerPriorityCommand extends BaseCommand {
  readonly cmd: 'GetCountsPerPriority';
  readonly queue: string;
}

export interface GetJobByCustomIdCommand extends BaseCommand {
  readonly cmd: 'GetJobByCustomId';
  readonly customId: string;
}

export interface CountCommand extends BaseCommand {
  readonly cmd: 'Count';
  readonly queue: string;
}

export interface GetProgressCommand extends BaseCommand {
  readonly cmd: 'GetProgress';
  readonly id: string;
}

// ============ Management Commands ============

export interface CancelCommand extends BaseCommand {
  readonly cmd: 'Cancel';
  readonly id: string;
}

export interface ProgressCommand extends BaseCommand {
  readonly cmd: 'Progress';
  readonly id: string;
  readonly progress: number;
  readonly message?: string;
}

export interface UpdateCommand extends BaseCommand {
  readonly cmd: 'Update';
  readonly id: string;
  readonly data: unknown;
}

export interface ChangePriorityCommand extends BaseCommand {
  readonly cmd: 'ChangePriority';
  readonly id: string;
  readonly priority: number;
  /** Tie-break ordering among same-priority jobs (BullMQ v5). See #90. */
  readonly lifo?: boolean;
}

export interface PromoteCommand extends BaseCommand {
  readonly cmd: 'Promote';
  readonly id: string;
}

export interface WaitJobCommand extends BaseCommand {
  readonly cmd: 'WaitJob';
  readonly id: string;
  readonly timeout?: number;
}

export interface MoveToDelayedCommand extends BaseCommand {
  readonly cmd: 'MoveToDelayed';
  readonly id: string;
  readonly delay: number;
}

export interface DiscardCommand extends BaseCommand {
  readonly cmd: 'Discard';
  readonly id: string;
}

// ============ Queue Control Commands ============

export interface PauseCommand extends BaseCommand {
  readonly cmd: 'Pause';
  readonly queue: string;
}

export interface ResumeCommand extends BaseCommand {
  readonly cmd: 'Resume';
  readonly queue: string;
}

export interface IsPausedCommand extends BaseCommand {
  readonly cmd: 'IsPaused';
  readonly queue: string;
}

export interface DrainCommand extends BaseCommand {
  readonly cmd: 'Drain';
  readonly queue: string;
}

export interface ObliterateCommand extends BaseCommand {
  readonly cmd: 'Obliterate';
  readonly queue: string;
}

export interface ListQueuesCommand extends BaseCommand {
  readonly cmd: 'ListQueues';
}

export interface CleanCommand extends BaseCommand {
  readonly cmd: 'Clean';
  readonly queue: string;
  readonly grace: number; // ms - jobs older than this
  readonly state?: JobState;
  readonly limit?: number;
}

// ============ DLQ Commands ============

export interface DlqCommand extends BaseCommand {
  readonly cmd: 'Dlq';
  readonly queue: string;
  readonly count?: number;
}

export interface RetryDlqCommand extends BaseCommand {
  readonly cmd: 'RetryDlq';
  readonly queue: string;
  readonly jobId?: string;
  /** Cap the number of DLQ entries retried (omit = retry all). #111-class. */
  readonly count?: number;
}

export interface PurgeDlqCommand extends BaseCommand {
  readonly cmd: 'PurgeDlq';
  readonly queue: string;
}

export interface RetryCompletedCommand extends BaseCommand {
  readonly cmd: 'RetryCompleted';
  readonly queue: string;
  readonly id?: string;
}

// ============ Rate Limiting Commands ============

export interface RateLimitCommand extends BaseCommand {
  readonly cmd: 'RateLimit';
  readonly queue: string;
  readonly limit: number;
}

export interface SetConcurrencyCommand extends BaseCommand {
  readonly cmd: 'SetConcurrency';
  readonly queue: string;
  readonly limit: number;
}

export interface RateLimitClearCommand extends BaseCommand {
  readonly cmd: 'RateLimitClear';
  readonly queue: string;
}

export interface ClearConcurrencyCommand extends BaseCommand {
  readonly cmd: 'ClearConcurrency';
  readonly queue: string;
}

// ============ Config Commands ============

export interface SetStallConfigCommand extends BaseCommand {
  readonly cmd: 'SetStallConfig';
  readonly queue: string;
  readonly config: Record<string, unknown>;
}

export interface GetStallConfigCommand extends BaseCommand {
  readonly cmd: 'GetStallConfig';
  readonly queue: string;
}

export interface SetDlqConfigCommand extends BaseCommand {
  readonly cmd: 'SetDlqConfig';
  readonly queue: string;
  readonly config: Record<string, unknown>;
}

export interface GetDlqConfigCommand extends BaseCommand {
  readonly cmd: 'GetDlqConfig';
  readonly queue: string;
}

// ============ Cron Commands ============

export interface CronCommand extends BaseCommand {
  readonly cmd: 'Cron';
  readonly name: string;
  readonly queue: string;
  readonly data: unknown;
  readonly schedule?: string;
  readonly repeatEvery?: number;
  readonly priority?: number;
  readonly maxLimit?: number;
  /** IANA timezone for cron schedule (e.g., "Europe/Rome", "America/New_York") */
  readonly timezone?: string;
  /** Unique key for deduplication of cron-spawned jobs */
  readonly uniqueKey?: string;
  /** Deduplication options for cron-spawned jobs */
  readonly dedup?: { ttl?: number; extend?: boolean; replace?: boolean };
  /** Skip missed runs on restart instead of executing them */
  readonly skipMissedOnRestart?: boolean;
  /** Fire immediately on creation, then continue on schedule */
  readonly immediately?: boolean;
  /** Skip job push if no worker is registered for the queue */
  readonly skipIfNoWorker?: boolean;
  /** Prevent overlapping cron jobs via automatic dedup */
  readonly preventOverlap?: boolean;
  /** Job options applied to each spawned job (retry/cleanup policy, etc.) */
  readonly jobOptions?: CronJobOptions;
}

export interface CronDeleteCommand extends BaseCommand {
  readonly cmd: 'CronDelete';
  readonly name: string;
}

export interface CronListCommand extends BaseCommand {
  readonly cmd: 'CronList';
}

// ============ Job Logs Commands ============

export interface AddLogCommand extends BaseCommand {
  readonly cmd: 'AddLog';
  readonly id: string;
  readonly message: string;
  readonly level?: 'info' | 'warn' | 'error';
}

export interface GetLogsCommand extends BaseCommand {
  readonly cmd: 'GetLogs';
  readonly id: string;
  readonly start?: number;
  readonly end?: number;
}

// ============ Heartbeat & Workers ============

export interface HeartbeatCommand extends BaseCommand {
  readonly cmd: 'Heartbeat';
  readonly id: string; // Worker ID
  readonly activeJobs?: number;
  readonly processed?: number;
  readonly failed?: number;
}

/** Job-level heartbeat for stall detection */
export interface JobHeartbeatCommand extends BaseCommand {
  readonly cmd: 'JobHeartbeat';
  readonly id: string; // Job ID
  readonly token?: string; // Lock token for ownership verification
  readonly duration?: number; // Lock renewal duration in ms
}

/** Batch job heartbeat for multiple jobs */
export interface JobHeartbeatBatchCommand extends BaseCommand {
  readonly cmd: 'JobHeartbeatB';
  readonly ids: string[]; // Job IDs
  readonly tokens?: string[]; // Lock tokens for each job (same order as ids)
}

/** Ping for connection health check */
export interface PingCommand extends BaseCommand {
  readonly cmd: 'Ping';
}

export interface RegisterWorkerCommand extends BaseCommand {
  readonly cmd: 'RegisterWorker';
  readonly name: string;
  readonly queues: string[];
  readonly concurrency?: number;
  readonly workerId?: string;
  readonly hostname?: string;
  readonly pid?: number;
  readonly startedAt?: number;
}

export interface UnregisterWorkerCommand extends BaseCommand {
  readonly cmd: 'UnregisterWorker';
  readonly workerId: string;
}

export interface ListWorkersCommand extends BaseCommand {
  readonly cmd: 'ListWorkers';
}

// ============ Webhooks ============

export interface AddWebhookCommand extends BaseCommand {
  readonly cmd: 'AddWebhook';
  readonly url: string;
  readonly events: string[];
  readonly queue?: string;
  readonly secret?: string;
}

export interface RemoveWebhookCommand extends BaseCommand {
  readonly cmd: 'RemoveWebhook';
  readonly webhookId: string;
}

export interface ListWebhooksCommand extends BaseCommand {
  readonly cmd: 'ListWebhooks';
}

// ============ Monitoring Commands ============

export interface StatsCommand extends BaseCommand {
  readonly cmd: 'Stats';
}

export interface MetricsCommand extends BaseCommand {
  readonly cmd: 'Metrics';
}

export interface PrometheusCommand extends BaseCommand {
  readonly cmd: 'Prometheus';
}

// ============ New Query/Monitoring Commands ============

/** Get a single cron job by name */
export interface CronGetCommand extends BaseCommand {
  readonly cmd: 'CronGet';
  readonly name: string;
}

/** Batch get children values for a parent job */
export interface GetChildrenValuesCommand extends BaseCommand {
  readonly cmd: 'GetChildrenValues';
  readonly id: string;
}

/** Get real storage/disk health status */
export interface StorageStatusCommand extends BaseCommand {
  readonly cmd: 'StorageStatus';
}

// ============ Extended Commands ============

export interface ClearLogsCommand extends BaseCommand {
  readonly cmd: 'ClearLogs';
  readonly id: string;
  readonly keepLogs?: number;
}

export interface ExtendLockCommand extends BaseCommand {
  readonly cmd: 'ExtendLock';
  readonly id: string;
  readonly token?: string;
  readonly duration: number;
}

export interface ExtendLocksCommand extends BaseCommand {
  readonly cmd: 'ExtendLocks';
  readonly ids: string[];
  readonly tokens: string[];
  readonly durations: number[];
}

export interface ChangeDelayCommand extends BaseCommand {
  readonly cmd: 'ChangeDelay';
  readonly id: string;
  readonly delay: number;
}

export interface SetWebhookEnabledCommand extends BaseCommand {
  readonly cmd: 'SetWebhookEnabled';
  readonly id: string;
  readonly enabled: boolean;
}

export interface CompactMemoryCommand extends BaseCommand {
  readonly cmd: 'CompactMemory';
}

export interface UpdateParentCommand extends BaseCommand {
  readonly cmd: 'UpdateParent';
  readonly childId: string;
  readonly parentId: string;
}

/** Get failed children values for a parent job */
export interface GetFailedChildrenValuesCommand extends BaseCommand {
  readonly cmd: 'GetFailedChildrenValues';
  readonly id: string;
}

/** Get ignored children failures for a parent job */
export interface GetIgnoredChildrenFailuresCommand extends BaseCommand {
  readonly cmd: 'GetIgnoredChildrenFailures';
  readonly id: string;
}

/** Remove a child's dependency from its parent */
export interface RemoveChildDependencyCommand extends BaseCommand {
  readonly cmd: 'RemoveChildDependency';
  readonly id: string;
}

/** Remove all unprocessed children of a parent job */
export interface RemoveUnprocessedChildrenCommand extends BaseCommand {
  readonly cmd: 'RemoveUnprocessedChildren';
  readonly id: string;
}

export interface MoveToWaitCommand extends BaseCommand {
  readonly cmd: 'MoveToWait';
  readonly id: string;
}

export interface PromoteJobsCommand extends BaseCommand {
  readonly cmd: 'PromoteJobs';
  readonly queue: string;
  readonly count?: number;
}

// ============ Dashboard Commands ============

/** Dashboard overview - aggregates stats, metrics, workers, crons in a single call */
export interface DashboardOverviewCommand extends BaseCommand {
  readonly cmd: 'DashboardOverview';
}

/** Dashboard queues - all queues with full stats per queue */
export interface DashboardQueuesCommand extends BaseCommand {
  readonly cmd: 'DashboardQueues';
}

/** Dashboard single queue detail */
export interface DashboardQueueCommand extends BaseCommand {
  readonly cmd: 'DashboardQueue';
  readonly queue: string;
  /** Include recent jobs in response (default: false) */
  readonly includeJobs?: boolean;
  /** Max recent jobs to include (default: 10) */
  readonly jobsLimit?: number;
}

// ============ Auth Commands ============

export interface AuthCommand extends BaseCommand {
  readonly cmd: 'Auth';
  readonly token: string;
}

// ============ Protocol Negotiation ============

/** Hello command for protocol version negotiation */
export interface HelloCommand extends BaseCommand {
  readonly cmd: 'Hello';
  /** Client protocol version */
  readonly protocolVersion: number;
  /** Supported capabilities */
  readonly capabilities?: 'pipelining'[];
}

/** Union of all commands */
export type Command =
  | PushCommand
  | PushBatchCommand
  | PullCommand
  | PullBatchCommand
  | AckCommand
  | AckBatchCommand
  | FailCommand
  | GetJobCommand
  | GetStateCommand
  | GetResultCommand
  | GetJobsCommand
  | GetJobCountsCommand
  | GetCountsPerPriorityCommand
  | GetJobByCustomIdCommand
  | CountCommand
  | GetProgressCommand
  | CancelCommand
  | ProgressCommand
  | UpdateCommand
  | ChangePriorityCommand
  | PromoteCommand
  | WaitJobCommand
  | MoveToDelayedCommand
  | DiscardCommand
  | PauseCommand
  | ResumeCommand
  | IsPausedCommand
  | DrainCommand
  | ObliterateCommand
  | ListQueuesCommand
  | CleanCommand
  | DlqCommand
  | RetryDlqCommand
  | PurgeDlqCommand
  | RetryCompletedCommand
  | RateLimitCommand
  | SetConcurrencyCommand
  | RateLimitClearCommand
  | ClearConcurrencyCommand
  | SetStallConfigCommand
  | GetStallConfigCommand
  | SetDlqConfigCommand
  | GetDlqConfigCommand
  | CronCommand
  | CronDeleteCommand
  | CronListCommand
  | AddLogCommand
  | GetLogsCommand
  | HeartbeatCommand
  | JobHeartbeatCommand
  | JobHeartbeatBatchCommand
  | PingCommand
  | RegisterWorkerCommand
  | UnregisterWorkerCommand
  | ListWorkersCommand
  | AddWebhookCommand
  | RemoveWebhookCommand
  | ListWebhooksCommand
  | StatsCommand
  | MetricsCommand
  | PrometheusCommand
  | CronGetCommand
  | GetChildrenValuesCommand
  | StorageStatusCommand
  | ClearLogsCommand
  | ExtendLockCommand
  | ExtendLocksCommand
  | ChangeDelayCommand
  | SetWebhookEnabledCommand
  | CompactMemoryCommand
  | UpdateParentCommand
  | MoveToWaitCommand
  | PromoteJobsCommand
  | DashboardOverviewCommand
  | DashboardQueuesCommand
  | DashboardQueueCommand
  | AuthCommand
  | GetFailedChildrenValuesCommand
  | GetIgnoredChildrenFailuresCommand
  | RemoveChildDependencyCommand
  | RemoveUnprocessedChildrenCommand
  | HelloCommand;

/** Extract command type */
export type CommandType = Command['cmd'];
