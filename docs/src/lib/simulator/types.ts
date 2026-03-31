/**
 * bunqueue Simulator Types
 * Browser-compatible type definitions
 */

export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'dlq';

export interface Job {
  id: string;
  queue: string;
  name: string;
  data: unknown;
  priority: number;
  delay: number;
  attempts: number;
  maxAttempts: number;
  backoff: number;
  state: JobState;
  progress: number;
  createdAt: number;
  processedAt?: number;
  completedAt?: number;
  failedAt?: number;
  runAt: number;
  result?: unknown;
  error?: string;
  workerId?: string;
}

export interface JobOptions {
  priority?: number;
  delay?: number;
  attempts?: number;
  backoff?: number;
  jobId?: string;
}

export interface DlqEntry {
  id: string;
  job: Job;
  reason: 'max_attempts' | 'timeout' | 'explicit_fail' | 'stalled';
  error: string;
  enteredAt: number;
  retryCount: number;
}

export interface ShardStats {
  queuedJobs: number;
  delayedJobs: number;
  activeJobs: number;
  completedJobs: number;
  failedJobs: number;
  dlqJobs: number;
}

export interface QueueStats {
  name: string;
  shardIndex: number;
  waiting: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  dlq: number;
  paused: boolean;
}

export interface WorkerInfo {
  id: string;
  queue: string;
  concurrency: number;
  activeJobs: number;
  processedCount: number;
  failedCount: number;
  startedAt: number;
  status: 'running' | 'paused' | 'stopped';
}

export interface SimulatorConfig {
  shardCount: number;
  defaultMaxAttempts: number;
  processingTimeMin: number;
  processingTimeMax: number;
  failureRate: number;
}

export interface SimulatorEvent {
  type: 'job:pushed' | 'job:pulled' | 'job:completed' | 'job:failed' | 'job:dlq' |
        'job:progress' | 'worker:started' | 'worker:stopped' | 'queue:paused' | 'queue:resumed';
  timestamp: number;
  data: unknown;
}

export interface CronJob {
  id: string;
  name: string;
  queue: string;
  schedule: string;
  data: unknown;
  nextRun: number;
  lastRun?: number;
  enabled: boolean;
}
