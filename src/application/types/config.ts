/** Queue Manager configuration. */
export interface QueueManagerConfig {
  dataPath?: string;
  maxCompletedJobs?: number;
  maxJobResults?: number;
  maxJobLogs?: number;
  maxCustomIds?: number;
  maxWaitingDeps?: number;
  /** Maximum queue label values emitted in one Prometheus scrape; zero disables them. */
  maxPrometheusQueues?: number;
  cleanupIntervalMs?: number;
  jobTimeoutCheckMs?: number;
  dependencyCheckMs?: number;
  stallCheckMs?: number;
  dlqMaintenanceMs?: number;
  validateWebhookUrls?: boolean;
}

export const DEFAULT_CONFIG = {
  maxCompletedJobs: 50_000,
  maxJobResults: 10_000,
  maxJobLogs: 10_000,
  maxCustomIds: 50_000,
  maxWaitingDeps: 10_000,
  maxPrometheusQueues: 100,
  cleanupIntervalMs: 10_000,
  jobTimeoutCheckMs: 5_000,
  dependencyCheckMs: 30_000,
  stallCheckMs: 5_000,
  dlqMaintenanceMs: 60_000,
};
