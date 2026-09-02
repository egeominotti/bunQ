/** Queue Manager configuration. */
export interface QueueManagerConfig {
  dataPath?: string;
  maxCompletedJobs?: number;
  /** Delete retained completed jobs after this age; null/undefined disables automatic retention. */
  completedRetentionMs?: number | null;
  maxJobResults?: number;
  maxJobLogs?: number;
  maxCustomIds?: number;
  maxWaitingDeps?: number;
  /** Maximum queue label values emitted in one Prometheus scrape; zero disables them. */
  maxPrometheusQueues?: number;
  /** Maximum retained lifecycle events per queue. */
  maxQueueEvents?: number;
  /** Maximum retained one-minute metric buckets per queue and terminal state. */
  maxMetricDataPoints?: number;
  cleanupIntervalMs?: number;
  /** Retry delay after a processing-timeout transition fails. */
  jobTimeoutCheckMs?: number;
  dependencyCheckMs?: number;
  stallCheckMs?: number;
  dlqMaintenanceMs?: number;
  validateWebhookUrls?: boolean;
}

export const DEFAULT_CONFIG = {
  maxCompletedJobs: 50_000,
  completedRetentionMs: null as number | null,
  maxJobResults: 10_000,
  maxJobLogs: 10_000,
  maxCustomIds: 50_000,
  maxWaitingDeps: 10_000,
  maxPrometheusQueues: 100,
  maxQueueEvents: 10_000,
  maxMetricDataPoints: 20_160,
  cleanupIntervalMs: 10_000,
  jobTimeoutCheckMs: 5_000,
  dependencyCheckMs: 30_000,
  stallCheckMs: 5_000,
  dlqMaintenanceMs: 60_000,
};

export function normalizeCompletedRetentionMs(value: number | null | undefined): number | null {
  if (value === null || value === undefined || !Number.isFinite(value) || value < 0) return null;
  const normalized = Math.floor(value);
  return Number.isSafeInteger(normalized) ? normalized : null;
}
