import type { CronJobInput } from '../../domain/types/cron';
import type { JobLogEntry } from '../../domain/types/worker';

export interface JobCounts {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  paused: number;
}

export interface SerializedJob {
  id: string;
  name: string;
  queue: string;
  data: unknown;
  priority: number;
  state?: string;
  progress: number;
  attempts: number;
  maxAttempts: number;
  createdAt: string;
  startedAt?: string;
}

export interface SerializedCron {
  name: string;
  queue: string;
  schedule?: string;
  repeatEvery?: number;
  nextRun: string | null;
  executions: number;
}

export interface WebhookInfo {
  id: string;
  url: string;
  events: string[];
  queue?: string;
  enabled: boolean;
}

export interface WorkerInfo {
  id: string;
  name: string;
  queues: string[];
  active: number;
  processed: number;
  failed: number;
  lastHeartbeat: number;
}

export interface FlowJobInput {
  name: string;
  queueName: string;
  data?: Record<string, unknown>;
  opts?: { priority?: number; delay?: number; attempts?: number };
  children?: FlowJobInput[];
}

export interface FlowStepInput {
  name: string;
  queueName: string;
  data: Record<string, unknown>;
  opts?: { priority?: number; delay?: number; attempts?: number };
}

export interface FlowNodeResult {
  jobId: string;
  name: string;
  queueName: string;
  children?: FlowNodeResult[];
}

/** Common backend contract; all methods are async for TCP compatibility. */
export interface McpBackend {
  addJob(
    queue: string,
    name: string,
    data: unknown,
    opts?: { priority?: number; delay?: number; attempts?: number }
  ): Promise<{ jobId: string }>;
  addJobsBulk(
    queue: string,
    jobs: Array<{ name: string; data: unknown; priority?: number; delay?: number }>
  ): Promise<{ jobIds: string[] }>;
  getJob(jobId: string): Promise<SerializedJob | null>;
  getJobState(jobId: string): Promise<string>;
  getJobResult(jobId: string): Promise<unknown>;
  getProgress(jobId: string): Promise<{ progress: number; message: string | null } | null>;
  cancelJob(jobId: string): Promise<boolean>;
  promoteJob(jobId: string): Promise<boolean>;
  updateProgress(jobId: string, progress: number, message?: string): Promise<boolean>;
  updateJobData(jobId: string, data: unknown): Promise<boolean>;
  changeJobPriority(jobId: string, priority: number): Promise<boolean>;
  moveToDelayed(jobId: string, delay: number): Promise<boolean>;
  changeDelay(jobId: string, delay: number): Promise<boolean>;
  discardJob(jobId: string): Promise<boolean>;
  getChildrenValues(parentJobId: string): Promise<Record<string, unknown>>;
  getJobByCustomId(customId: string): Promise<SerializedJob | null>;
  waitForJobCompletion(jobId: string, timeoutMs: number): Promise<boolean>;

  pullJob(queue: string, timeoutMs?: number): Promise<SerializedJob | null>;
  pullJobBatch(queue: string, count: number, timeoutMs?: number): Promise<SerializedJob[]>;
  ackJob(jobId: string, result?: unknown): Promise<void>;
  ackJobBatch(jobIds: string[]): Promise<void>;
  failJob(jobId: string, error?: string): Promise<void>;
  jobHeartbeat(jobId: string): Promise<boolean>;
  jobHeartbeatBatch(jobIds: string[]): Promise<number>;
  extendLock(jobId: string, token: string, duration: number): Promise<boolean>;

  getJobs(
    queue: string,
    opts?: { state?: string; start?: number; end?: number }
  ): Promise<SerializedJob[]>;
  getJobCounts(queue: string): Promise<JobCounts>;
  pauseQueue(queue: string): Promise<void>;
  resumeQueue(queue: string): Promise<void>;
  drainQueue(queue: string): Promise<number>;
  obliterateQueue(queue: string): Promise<void>;
  listQueues(): Promise<string[]>;
  countJobs(queue: string): Promise<number>;
  cleanQueue(queue: string, graceMs: number, state?: string, limit?: number): Promise<string[]>;
  isPaused(queue: string): Promise<boolean>;
  getCountsPerPriority(queue: string): Promise<Record<number, number>>;

  getDlq(queue: string, limit?: number): Promise<SerializedJob[]>;
  retryDlq(queue: string, jobId?: string): Promise<number>;
  purgeDlq(queue: string): Promise<number>;
  retryCompleted(queue: string, jobId?: string): Promise<number>;
  setRateLimit(queue: string, limit: number): Promise<void>;
  clearRateLimit(queue: string): Promise<void>;
  setConcurrency(queue: string, limit: number): Promise<void>;
  clearConcurrency(queue: string): Promise<void>;

  addCron(input: CronJobInput): Promise<SerializedCron>;
  getCron(name: string): Promise<SerializedCron | null>;
  listCrons(): Promise<SerializedCron[]>;
  deleteCron(name: string): Promise<boolean>;
  addWebhook(url: string, events: string[], queue?: string): Promise<WebhookInfo>;
  removeWebhook(id: string): Promise<boolean>;
  listWebhooks(): Promise<WebhookInfo[]>;
  setWebhookEnabled(id: string, enabled: boolean): Promise<boolean>;
  registerWorker(name: string, queues: string[]): Promise<WorkerInfo>;
  unregisterWorker(id: string): Promise<boolean>;
  workerHeartbeat(id: string): Promise<boolean>;
  listWorkers(): Promise<WorkerInfo[]>;

  getStats(): Promise<Record<string, unknown>>;
  getPerQueueStats(): Promise<Record<string, unknown>>;
  getMemoryStats(): Promise<Record<string, unknown>>;
  getPrometheusMetrics(): Promise<string>;
  getStorageStatus(): Promise<{ diskFull: boolean; error: string | null }>;
  getJobLogs(jobId: string): Promise<JobLogEntry[]>;
  addJobLog(jobId: string, message: string, level?: 'info' | 'warn' | 'error'): Promise<boolean>;
  clearJobLogs(jobId: string, keepLogs?: number): Promise<void>;
  compactMemory(): Promise<void>;

  addFlow(flow: FlowJobInput): Promise<FlowNodeResult>;
  addFlowChain(steps: FlowStepInput[]): Promise<{ jobIds: string[] }>;
  addFlowBulkThen(
    parallel: FlowStepInput[],
    final: FlowStepInput
  ): Promise<{ parallelIds: string[]; finalId: string }>;
  getFlow(
    jobId: string,
    queueName: string,
    depth?: number,
    maxChildren?: number
  ): Promise<FlowNodeResult | null>;
  shutdown(): void;
}
