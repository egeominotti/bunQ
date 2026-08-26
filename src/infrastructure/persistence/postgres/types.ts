import type { DlqConfig, DlqEntry, DlqRetryState } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import type { StallConfig } from '../../../domain/types/stall';

export type PostgresJobState =
  | 'waiting'
  | 'prioritized'
  | 'delayed'
  | 'waiting-children'
  | 'active'
  | 'completed'
  | 'failed';

export interface PostgresStorageConfig {
  readonly url: string;
  readonly namespace?: string;
  readonly brokerId?: string;
  readonly poolSize?: number;
  readonly leaseDurationMs?: number;
  readonly pollIntervalMs?: number;
  readonly maxQueueEvents?: number;
  readonly maxMetricDataPoints?: number;
  readonly maxCompletedJobs?: number;
  readonly maxJobResults?: number;
}

export interface ResolvedPostgresStorageConfig {
  readonly url: string;
  readonly namespace: string;
  readonly brokerId: string;
  readonly poolSize: number;
  readonly leaseDurationMs: number;
  readonly pollIntervalMs: number;
  readonly maxQueueEvents: number;
  readonly maxMetricDataPoints: number;
  readonly maxCompletedJobs: number;
  readonly maxJobResults: number;
}

export interface PostgresJobRow {
  namespace: string;
  id: string;
  queue: string;
  payload: Uint8Array;
  state: PostgresJobState;
  priority: number;
  lifo: boolean;
  run_at: number | string | bigint;
  created_at: number | string | bigint;
  started_at: number | string | bigint | null;
  completed_at: number | string | bigint | null;
  attempts: number;
  max_attempts: number;
  ttl: number | string | bigint | null;
  timeout: number | string | bigint | null;
  unique_key: string | null;
  unique_expires_at: number | string | bigint | null;
  custom_id: string | null;
  group_id: string | null;
  parent_id: string | null;
  lease_owner: string | null;
  lease_broker_id: string | null;
  lease_token: string | null;
  lease_until: number | string | bigint | null;
  lease_renewals: number;
  result: Uint8Array | null;
  dlq_entry: Uint8Array | null;
  dlq_retry_state: Uint8Array | null;
  error: string | null;
  failure_reason: string | null;
  version: number | string | bigint;
}

export interface ClaimedPostgresJob {
  readonly job: Job;
  readonly token: string;
  readonly owner: string;
  readonly brokerId: string;
  readonly leaseUntil: number;
}

export interface PostgresStoredJob {
  readonly job: Job;
  readonly state: PostgresJobState;
  readonly result: unknown;
  readonly error: string | null;
  readonly dlqEntry: DlqEntry | null;
  readonly dlqRetryState: DlqRetryState | null;
  readonly token: string | null;
  readonly leaseOwner: string | null;
  readonly leaseBrokerId: string | null;
  readonly leaseUntil: number | null;
  readonly leaseRenewals: number;
  readonly version: number;
}

export interface PostgresCompletionResult {
  readonly jobId: JobId;
  readonly queue: string;
  readonly result: unknown;
  readonly pinned: boolean;
}

export interface PostgresQueueState {
  readonly queue: string;
  readonly paused: boolean;
  readonly rateLimit: number | null;
  readonly rateDurationMs: number | null;
  readonly rateWindowStartedAt: number | null;
  readonly rateExpiresAt: number | null;
  readonly rateCount: number;
  readonly concurrencyLimit: number | null;
  readonly stallConfig: StallConfig;
  readonly dlqConfig: DlqConfig;
}

export interface PostgresCounts {
  readonly waiting: number;
  readonly active: number;
  readonly completed: number;
  readonly failed: number;
  readonly delayed: number;
  readonly prioritized: number;
  readonly waitingChildren: number;
}

export interface PostgresTransitionResult {
  readonly applied: boolean;
  readonly alreadyFinalized: boolean;
  readonly job: Job | null;
  readonly state: PostgresJobState | null;
}

export interface PostgresCompletionInput {
  readonly id: JobId;
  readonly token: string;
  readonly result?: unknown;
  readonly removeOnComplete?: boolean;
}

export interface PostgresBatchCompletionResult extends PostgresTransitionResult {
  readonly removed: boolean;
}

export interface PostgresFailureInput {
  readonly id: JobId;
  readonly token: string;
  readonly error?: string;
  readonly stack?: string[];
  readonly unrecoverable?: boolean;
  readonly removeOnFail?: boolean;
  readonly failureReason?: string;
}

export interface PostgresStoreEvent {
  readonly id: number;
  readonly queue: string;
  readonly type: string;
  readonly jobId: JobId;
  readonly occurredAt: number;
  readonly job: Job | null;
  readonly state: PostgresJobState | null;
  readonly result?: unknown;
  readonly error?: string;
  readonly removed?: boolean;
  readonly dlqEntry?: DlqEntry | null;
  readonly dlqRetryState?: DlqRetryState | null;
}

export interface PostgresStorageHealth {
  readonly ok: boolean;
  readonly error: string | null;
  readonly since: number | null;
  readonly backend: 'postgres';
  readonly brokerId: string;
  readonly namespace: string;
}
