/**
 * Worker Manager
 * Tracks connected workers and their status
 */

import {
  type Worker,
  type WorkerId,
  type CreateWorkerOptions,
  createWorker,
} from '../domain/types/worker';

/** Worker timeout - consider dead after this many ms without heartbeat */
const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);

/** Cleanup interval for stale workers */
const WORKER_CLEANUP_INTERVAL_MS = parseInt(Bun.env.WORKER_CLEANUP_INTERVAL_MS ?? '60000', 10);

/**
 * Worker Manager
 */
export class WorkerManager {
  private readonly workers = new Map<WorkerId, Worker>();
  private cleanupInterval: ReturnType<typeof setInterval> | null = null;
  private dashboardEmit: ((event: string, data: Record<string, unknown>) => void) | null = null;

  /** Running counters for O(1) stats - avoids O(n) reduce operations */
  private totalProcessedCounter = 0;
  private totalFailedCounter = 0;
  private totalActiveJobsCounter = 0;

  constructor() {
    this.startCleanup();
  }

  /** Set the dashboard event emitter callback */
  setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void {
    this.dashboardEmit = callback;
  }

  /** Register a new worker */
  register(
    name: string,
    queues: string[],
    concurrency: number = 1,
    opts?: CreateWorkerOptions
  ): Worker {
    // If client provides a workerId and it's already registered, update it
    if (opts?.workerId) {
      const existing = this.workers.get(opts.workerId);
      if (existing) {
        existing.queues = queues;
        existing.concurrency = concurrency;
        existing.lastSeen = Date.now();
        if (opts.hostname) existing.hostname = opts.hostname;
        if (opts.pid) existing.pid = opts.pid;
        return existing;
      }
    }
    const worker = createWorker(name, queues, concurrency, opts);
    this.workers.set(worker.id, worker);
    return worker;
  }

  /** Unregister a worker */
  unregister(id: WorkerId): boolean {
    const worker = this.workers.get(id);
    if (worker) {
      // Adjust running counters before removal
      this.totalActiveJobsCounter -= worker.activeJobs;
    }
    return this.workers.delete(id);
  }

  /** Unregister all workers associated with a TCP client ID */
  unregisterByClientId(clientId: string): number {
    let removed = 0;
    for (const [id, worker] of this.workers) {
      if (worker.clientId === clientId) {
        this.totalActiveJobsCounter -= worker.activeJobs;
        this.workers.delete(id);
        this.dashboardEmit?.('worker:disconnected', { workerId: id, name: worker.name, clientId });
        removed++;
      }
    }
    return removed;
  }

  /** Get worker by ID */
  get(id: WorkerId): Worker | undefined {
    return this.workers.get(id);
  }

  /** Update worker last seen time and optionally client-reported stats */
  heartbeat(
    id: WorkerId,
    stats?: { activeJobs?: number; processed?: number; failed?: number }
  ): boolean {
    const worker = this.workers.get(id);
    if (!worker) return false;
    worker.lastSeen = Date.now();
    if (stats) {
      if (stats.activeJobs !== undefined) {
        this.totalActiveJobsCounter -= worker.activeJobs;
        worker.activeJobs = stats.activeJobs;
        this.totalActiveJobsCounter += stats.activeJobs;
      }
      if (stats.processed !== undefined) {
        this.totalProcessedCounter -= worker.processedJobs;
        worker.processedJobs = stats.processed;
        this.totalProcessedCounter += stats.processed;
      }
      if (stats.failed !== undefined) {
        this.totalFailedCounter -= worker.failedJobs;
        worker.failedJobs = stats.failed;
        this.totalFailedCounter += stats.failed;
      }
    }
    return true;
  }

  /** Increment active jobs count */
  incrementActive(id: WorkerId, jobId?: string): void {
    const worker = this.workers.get(id);
    if (worker) {
      worker.activeJobs++;
      this.totalActiveJobsCounter++;
      worker.lastSeen = Date.now();
      if (jobId) {
        worker.currentJob = jobId;
      }
    }
  }

  /** Decrement active jobs and increment processed */
  jobCompleted(id: WorkerId): void {
    const worker = this.workers.get(id);
    if (worker) {
      if (worker.activeJobs > 0) {
        worker.activeJobs--;
        this.totalActiveJobsCounter--;
      }
      worker.processedJobs++;
      this.totalProcessedCounter++;
      worker.lastSeen = Date.now();
      if (worker.activeJobs === 0) {
        worker.currentJob = null;
        this.dashboardEmit?.('worker:idle', { workerId: id, processedJobs: worker.processedJobs });
      }
    }
  }

  /** Decrement active jobs and increment failed */
  jobFailed(id: WorkerId): void {
    const worker = this.workers.get(id);
    if (worker) {
      if (worker.activeJobs > 0) {
        worker.activeJobs--;
        this.totalActiveJobsCounter--;
      }
      worker.failedJobs++;
      this.totalFailedCounter++;
      worker.lastSeen = Date.now();
      if (worker.activeJobs === 0) {
        worker.currentJob = null;
        this.dashboardEmit?.('worker:idle', { workerId: id, processedJobs: worker.processedJobs });
      }
      // Emit worker:error at failure thresholds (5, 10, 25, 50, 100)
      const total = worker.processedJobs + worker.failedJobs;
      if (
        total >= 5 &&
        (worker.failedJobs === 5 ||
          worker.failedJobs === 10 ||
          worker.failedJobs === 25 ||
          worker.failedJobs === 50 ||
          worker.failedJobs === 100)
      ) {
        this.dashboardEmit?.('worker:error', {
          workerId: id,
          name: worker.name,
          failedJobs: worker.failedJobs,
          processedJobs: worker.processedJobs,
          failureRate: +(worker.failedJobs / total).toFixed(3),
        });
      }
    }
  }

  /** List all workers */
  list(): Worker[] {
    return Array.from(this.workers.values());
  }

  /** List active workers (seen recently) */
  listActive(): Worker[] {
    const now = Date.now();
    return Array.from(this.workers.values()).filter((w) => now - w.lastSeen < WORKER_TIMEOUT_MS);
  }

  /** Get workers for a specific queue (only active workers with recent heartbeat) */
  getForQueue(queue: string): Worker[] {
    const now = Date.now();
    return Array.from(this.workers.values()).filter(
      (w) => w.queues?.includes(queue) && now - w.lastSeen < WORKER_TIMEOUT_MS
    );
  }

  /** Start cleanup interval */
  private startCleanup(): void {
    this.cleanupInterval = setInterval(() => {
      this.cleanupStale();
    }, WORKER_CLEANUP_INTERVAL_MS);
  }

  /** Remove stale workers */
  private cleanupStale(): void {
    const now = Date.now();
    const staleTimeout = WORKER_TIMEOUT_MS * 3; // Give extra time before removal

    for (const [id, worker] of this.workers) {
      if (now - worker.lastSeen > staleTimeout) {
        // Adjust running counters before removal
        this.totalActiveJobsCounter -= worker.activeJobs;
        this.workers.delete(id);
        this.dashboardEmit?.('worker:removed-stale', { workerId: id, name: worker.name });
      }
    }
  }

  /** Stop cleanup */
  stop(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }

  /** Get stats - O(n) only for active count, O(1) for other metrics */
  getStats() {
    const now = Date.now();
    let activeWorkers = 0;
    let concurrencySlots = 0;

    // Only iterate once for time-based active count and configured capacity.
    for (const worker of this.workers.values()) {
      if (now - worker.lastSeen < WORKER_TIMEOUT_MS) {
        activeWorkers++;
        concurrencySlots += worker.concurrency;
      }
    }

    return {
      total: this.workers.size,
      active: activeWorkers,
      totalProcessed: this.totalProcessedCounter,
      totalFailed: this.totalFailedCounter,
      activeJobs: this.totalActiveJobsCounter,
      concurrencySlots,
    };
  }
}
