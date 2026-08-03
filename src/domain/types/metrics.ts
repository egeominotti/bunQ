export type QueueMetricType = 'completed' | 'failed';

export interface QueueMetricsMeta {
  /** Cumulative number of terminal jobs since the queue metrics were created. */
  readonly count: number;
  /** Timestamp of the most recently recorded terminal job. */
  readonly prevTS: number;
  /** Count in the most recently updated one-minute bucket. */
  readonly prevCount: number;
}

export interface QueueMetrics {
  readonly meta: QueueMetricsMeta;
  /** One-minute job-count buckets, newest first. */
  readonly data: number[];
  /** Total number of available buckets before start/end pagination. */
  readonly count: number;
}
