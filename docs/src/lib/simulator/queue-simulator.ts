/**
 * bunqueue Simulator
 * Complete browser-compatible simulation of bunqueue functionality
 */

import type {
  Job,
  JobOptions,
  QueueStats,
  WorkerInfo,
  SimulatorConfig,
  SimulatorEvent,
  CronJob,
  DlqEntry,
} from './types';
import { Shard } from './shard';
import { shardIndex, generateId, calculateShardCount } from './hash';

type EventCallback = (event: SimulatorEvent) => void;

const DEFAULT_CONFIG: SimulatorConfig = {
  shardCount: 8,
  defaultMaxAttempts: 3,
  processingTimeMin: 500,
  processingTimeMax: 2000,
  failureRate: 0.1,
};

export class QueueSimulator {
  private config: SimulatorConfig;
  private shards: Shard[];
  private shardMask: number;
  private workers: Map<string, WorkerInfo> = new Map();
  private cronJobs: Map<string, CronJob> = new Map();
  private listeners: Set<EventCallback> = new Set();
  private cronInterval?: ReturnType<typeof setInterval>;
  private totalPushed = 0;
  private totalProcessed = 0;
  private totalFailed = 0;

  constructor(config: Partial<SimulatorConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };

    // Ensure power of 2
    const shardCount = calculateShardCount(this.config.shardCount);
    this.config.shardCount = shardCount;
    this.shardMask = shardCount - 1;

    // Initialize shards
    this.shards = Array.from({ length: shardCount }, (_, i) => new Shard(i));

    // Start cron scheduler
    this.startCronScheduler();
  }

  // ===== QUEUE OPERATIONS =====

  push(queue: string, name: string, data: unknown, options: JobOptions = {}): Job {
    const now = Date.now();
    const delay = options.delay || 0;

    const job: Job = {
      id: options.jobId || generateId(),
      queue,
      name,
      data,
      priority: options.priority || 0,
      delay,
      attempts: 0,
      maxAttempts: options.attempts || this.config.defaultMaxAttempts,
      backoff: options.backoff || 1000, // Default 1 second backoff
      state: delay > 0 ? 'delayed' : 'waiting',
      progress: 0,
      createdAt: now,
      runAt: now + delay,
    };

    const idx = shardIndex(queue, this.shardMask);
    this.shards[idx].push(job);
    this.totalPushed++;

    this.emit({
      type: 'job:pushed',
      timestamp: now,
      data: { job, shardIndex: idx },
    });

    return job;
  }

  pushBulk(queue: string, jobs: Array<{ name: string; data: unknown; options?: JobOptions }>): Job[] {
    return jobs.map(j => this.push(queue, j.name, j.data, j.options));
  }

  pull(queue: string): Job | undefined {
    const idx = shardIndex(queue, this.shardMask);
    const job = this.shards[idx].pull(queue);

    if (job) {
      this.emit({
        type: 'job:pulled',
        timestamp: Date.now(),
        data: { job, shardIndex: idx },
      });
    }

    return job;
  }

  ack(jobId: string, queue: string, result?: unknown): Job | undefined {
    const idx = shardIndex(queue, this.shardMask);
    const job = this.shards[idx].complete(jobId, result);

    if (job) {
      this.totalProcessed++;
      this.emit({
        type: 'job:completed',
        timestamp: Date.now(),
        data: { job, result },
      });
    }

    return job;
  }

  fail(jobId: string, queue: string, error: string): { job: Job; toDlq: boolean } | undefined {
    const idx = shardIndex(queue, this.shardMask);
    const result = this.shards[idx].fail(jobId, error, this.config.defaultMaxAttempts);

    if (result) {
      this.totalFailed++;
      this.emit({
        type: result.toDlq ? 'job:dlq' : 'job:failed',
        timestamp: Date.now(),
        data: { job: result.job, error, toDlq: result.toDlq },
      });
    }

    return result;
  }

  updateProgress(jobId: string, queue: string, progress: number): boolean {
    const idx = shardIndex(queue, this.shardMask);
    const updated = this.shards[idx].updateProgress(jobId, progress);

    if (updated) {
      this.emit({
        type: 'job:progress',
        timestamp: Date.now(),
        data: { jobId, queue, progress },
      });
    }

    return updated;
  }

  // ===== QUEUE CONTROL =====

  pause(queue: string): void {
    const idx = shardIndex(queue, this.shardMask);
    this.shards[idx].pause(queue);
    this.emit({
      type: 'queue:paused',
      timestamp: Date.now(),
      data: { queue },
    });
  }

  resume(queue: string): void {
    const idx = shardIndex(queue, this.shardMask);
    this.shards[idx].resume(queue);
    this.emit({
      type: 'queue:resumed',
      timestamp: Date.now(),
      data: { queue },
    });
  }

  drain(queue: string): number {
    const idx = shardIndex(queue, this.shardMask);
    return this.shards[idx].drain(queue);
  }

  isPaused(queue: string): boolean {
    const idx = shardIndex(queue, this.shardMask);
    return this.shards[idx].isPaused(queue);
  }

  // ===== DLQ OPERATIONS =====

  getDlq(queue: string): DlqEntry[] {
    const idx = shardIndex(queue, this.shardMask);
    return this.shards[idx].getDlq(queue);
  }

  retryDlq(queue: string): number {
    const idx = shardIndex(queue, this.shardMask);
    return this.shards[idx].retryDlq(queue);
  }

  purgeDlq(queue: string): number {
    const idx = shardIndex(queue, this.shardMask);
    return this.shards[idx].purgeDlq(queue);
  }

  // ===== WORKER SIMULATION =====

  createWorker(
    queue: string,
    processor: (job: Job) => Promise<unknown>,
    options: { concurrency?: number; id?: string } = {}
  ): string {
    const workerId = options.id || `worker-${generateId()}`;
    const concurrency = options.concurrency || 1;

    const workerInfo: WorkerInfo = {
      id: workerId,
      queue,
      concurrency,
      activeJobs: 0,
      processedCount: 0,
      failedCount: 0,
      startedAt: Date.now(),
      status: 'running',
    };

    this.workers.set(workerId, workerInfo);

    this.emit({
      type: 'worker:started',
      timestamp: Date.now(),
      data: { worker: workerInfo },
    });

    // Start processing loop
    this.runWorkerLoop(workerId, queue, processor, concurrency);

    return workerId;
  }

  private async runWorkerLoop(
    workerId: string,
    queue: string,
    processor: (job: Job) => Promise<unknown>,
    concurrency: number
  ): Promise<void> {
    while (true) {
      // Re-fetch worker each iteration to check current state
      const worker = this.workers.get(workerId);
      if (!worker || worker.status !== 'running') {
        break;
      }

      // Check if we can take more jobs
      if (worker.activeJobs >= concurrency) {
        await this.sleep(100);
        continue;
      }

      // Reserve slot BEFORE pulling to prevent over-concurrency
      worker.activeJobs++;

      const job = this.pull(queue);
      if (!job) {
        // No job available, release the reserved slot
        worker.activeJobs--;
        await this.sleep(200);
        continue;
      }

      // Process job asynchronously (slot already reserved)
      this.processJob(workerId, job, processor).catch(() => {
        // Ensure slot is released even on unexpected errors
        const w = this.workers.get(workerId);
        if (w && w.activeJobs > 0) {
          w.activeJobs--;
        }
      });
    }
  }

  private async processJob(
    workerId: string,
    job: Job,
    processor: (job: Job) => Promise<unknown>
  ): Promise<void> {
    try {
      // Simulate processing with progress updates
      const steps = 5;
      for (let i = 1; i <= steps; i++) {
        await this.sleep(this.config.processingTimeMin / steps);
        this.updateProgress(job.id, job.queue, (i / steps) * 100);
      }

      // Simulate random failure
      if (Math.random() < this.config.failureRate) {
        throw new Error('Simulated random failure');
      }

      const result = await processor(job);
      this.ack(job.id, job.queue, result);

      // Update worker stats (check worker still exists)
      const worker = this.workers.get(workerId);
      if (worker) {
        worker.processedCount++;
      }
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      this.fail(job.id, job.queue, errorMsg);

      // Update worker stats (check worker still exists)
      const worker = this.workers.get(workerId);
      if (worker) {
        worker.failedCount++;
      }
    } finally {
      // Release slot (check worker still exists)
      const worker = this.workers.get(workerId);
      if (worker && worker.activeJobs > 0) {
        worker.activeJobs--;
      }
    }
  }

  stopWorker(workerId: string): void {
    const worker = this.workers.get(workerId);
    if (worker) {
      worker.status = 'stopped';
      this.emit({
        type: 'worker:stopped',
        timestamp: Date.now(),
        data: { worker },
      });
    }
  }

  getWorkers(): WorkerInfo[] {
    return Array.from(this.workers.values());
  }

  // ===== CRON JOBS =====

  addCron(name: string, queue: string, schedule: string, data: unknown): CronJob {
    const cronJob: CronJob = {
      id: generateId(),
      name,
      queue,
      schedule,
      data,
      nextRun: this.parseNextRun(schedule),
      enabled: true,
    };

    this.cronJobs.set(cronJob.id, cronJob);
    return cronJob;
  }

  removeCron(cronId: string): boolean {
    return this.cronJobs.delete(cronId);
  }

  getCronJobs(): CronJob[] {
    return Array.from(this.cronJobs.values());
  }

  private parseNextRun(schedule: string): number {
    // Simple interval parsing (e.g., "5s", "1m", "1h")
    const match = schedule.match(/^(\d+)(s|m|h)$/);
    if (!match) return Date.now() + 60000; // Default 1 minute

    const value = parseInt(match[1]);
    const unit = match[2];
    const multiplier = unit === 's' ? 1000 : unit === 'm' ? 60000 : 3600000;

    return Date.now() + value * multiplier;
  }

  private startCronScheduler(): void {
    this.cronInterval = setInterval(() => {
      const now = Date.now();
      for (const cron of this.cronJobs.values()) {
        if (cron.enabled && cron.nextRun <= now) {
          this.push(cron.queue, cron.name, cron.data);
          cron.lastRun = now;
          cron.nextRun = this.parseNextRun(cron.schedule);
        }
      }
    }, 1000);
  }

  // ===== STATS & QUERIES =====

  getJob(jobId: string): Job | undefined {
    for (const shard of this.shards) {
      const job = shard.getJob(jobId);
      if (job) return job;
    }
    return undefined;
  }

  getQueueStats(queue: string): QueueStats {
    const idx = shardIndex(queue, this.shardMask);
    const shard = this.shards[idx];
    const stats = shard.getStats(queue);

    return {
      name: queue,
      shardIndex: idx,
      waiting: stats.queuedJobs,
      delayed: stats.delayedJobs,
      active: stats.activeJobs,
      completed: stats.completedJobs,
      failed: stats.failedJobs,
      dlq: stats.dlqJobs,
      paused: shard.isPaused(queue),
    };
  }

  getAllQueues(): string[] {
    const queues = new Set<string>();
    for (const shard of this.shards) {
      for (const name of shard.getQueueNames()) {
        queues.add(name);
      }
    }
    return Array.from(queues);
  }

  getShardStats(): Array<{ index: number; stats: ReturnType<Shard['getStats']>; jobs: Job[] }> {
    return this.shards.map((shard, index) => ({
      index,
      stats: shard.getStats(),
      jobs: shard.getAllJobs(),
    }));
  }

  getGlobalStats(): {
    shardCount: number;
    totalPushed: number;
    totalProcessed: number;
    totalFailed: number;
    activeWorkers: number;
    queues: number;
  } {
    return {
      shardCount: this.config.shardCount,
      totalPushed: this.totalPushed,
      totalProcessed: this.totalProcessed,
      totalFailed: this.totalFailed,
      activeWorkers: Array.from(this.workers.values()).filter(w => w.status === 'running').length,
      queues: this.getAllQueues().length,
    };
  }

  // ===== EVENTS =====

  on(callback: EventCallback): () => void {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }

  private emit(event: SimulatorEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (e) {
        console.error('Event listener error:', e);
      }
    }
  }

  // ===== UTILITIES =====

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  destroy(): void {
    if (this.cronInterval) {
      clearInterval(this.cronInterval);
    }
    for (const worker of this.workers.values()) {
      worker.status = 'stopped';
    }
    this.workers.clear();
    this.listeners.clear();
  }

  reset(): void {
    this.destroy();
    this.shards = Array.from({ length: this.config.shardCount }, (_, i) => new Shard(i));
    this.totalPushed = 0;
    this.totalProcessed = 0;
    this.totalFailed = 0;
    this.cronJobs.clear();
    this.startCronScheduler();
  }
}

// Export singleton for simple usage
let defaultSimulator: QueueSimulator | null = null;

export function getSimulator(config?: Partial<SimulatorConfig>): QueueSimulator {
  if (!defaultSimulator) {
    defaultSimulator = new QueueSimulator(config);
  }
  return defaultSimulator;
}

export function resetSimulator(): void {
  if (defaultSimulator) {
    defaultSimulator.destroy();
    defaultSimulator = null;
  }
}
