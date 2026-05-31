/**
 * Cron Scheduler
 * Manages scheduled jobs and executes them on time
 * Uses min-heap for O(k log n) tick instead of O(n) full scan
 * Event-driven: uses precise setTimeout to wake exactly when the next cron is due
 */

import { type CronJob, type CronJobInput, createCronJob, isAtLimit } from '../../domain/types/cron';
import type { JobInput } from '../../domain/types/job';
import {
  validateCronExpression,
  getNextCronRun,
  getNextIntervalRun,
  expandCronShortcut,
} from './cronParser';
import { cronLog } from '../../shared/logger';
import { MinHeap } from '../../shared/minHeap';

/** Cron scheduler configuration (kept for backward compatibility) */
export interface CronSchedulerConfig {
  /** @deprecated No longer used - scheduler now uses precise setTimeout */
  checkIntervalMs?: number;
}

/** Safety fallback interval (60s) - catches edge cases like timer drift */
const SAFETY_FALLBACK_MS = 60_000;

/** Push job callback type */
export type PushJobCallback = (queue: string, input: JobInput) => Promise<void>;

/** Persist cron state callback type */
export type PersistCronCallback = (name: string, executions: number, nextRun: number) => void;

/** Heap entry with generation for lazy deletion */
interface CronHeapEntry {
  cron: CronJob;
  generation: number;
}

/**
 * Cron Scheduler
 * Event-driven: wakes exactly when the next cron is due via setTimeout
 * Safety fallback: setInterval(60s) catches missed events
 * Optimized with min-heap for O(k log n) tick where k = due crons
 * Uses lazy deletion with generation numbers for O(1) remove
 */
export class CronScheduler {
  /** Map for O(1) lookup by name with generation tracking */
  private readonly cronJobs = new Map<string, { cron: CronJob; generation: number }>();
  /** Min-heap ordered by nextRun for O(k log n) tick */
  private readonly cronHeap = new MinHeap<CronHeapEntry>((a, b) => a.cron.nextRun - b.cron.nextRun);
  /** Current generation counter */
  private generation = 0;
  /** Precise timer targeting the next cron's nextRun */
  private nextTimer: ReturnType<typeof setTimeout> | null = null;
  /** Safety fallback interval (60s) */
  private safetyInterval: ReturnType<typeof setInterval> | null = null;
  /** Whether the scheduler is running */
  private started = false;
  private pushJob: PushJobCallback | null = null;
  private persistCron: PersistCronCallback | null = null;
  private hasWorkers: ((queue: string) => boolean) | null = null;
  private dashboardEmit: ((event: string, data: Record<string, unknown>) => void) | null = null;
  /** Track last fire time per cron for overlap detection */
  private readonly lastFiredAt = new Map<string, number>();

  // Accept config for backward compatibility (no longer used internally)
  constructor(_config?: CronSchedulerConfig) {
    // noop - scheduler now uses precise setTimeout instead of configurable interval
    void _config;
  }

  /**
   * Set the push job callback
   */
  setPushCallback(callback: PushJobCallback): void {
    this.pushJob = callback;
  }

  /**
   * Set the persist cron callback for saving state after execution
   */
  setPersistCallback(callback: PersistCronCallback): void {
    this.persistCron = callback;
  }

  /**
   * Set the worker check callback for skipIfNoWorker
   */
  setWorkerCheckCallback(callback: (queue: string) => boolean): void {
    this.hasWorkers = callback;
  }

  /**
   * Set the dashboard event emitter callback
   */
  setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void {
    this.dashboardEmit = callback;
  }

  /**
   * Start the scheduler
   * Uses precise setTimeout for the fast path + 60s safety fallback
   */
  start(): void {
    if (this.started) return;
    this.started = true;

    // Safety fallback: tick every 60s as last-resort for missed events
    this.safetyInterval = setInterval(() => {
      void this.tick();
    }, SAFETY_FALLBACK_MS);

    // Schedule precise timer for the next due cron
    this.scheduleNext();
  }

  /**
   * Stop the scheduler
   */
  stop(): void {
    this.started = false;
    if (this.nextTimer !== null) {
      clearTimeout(this.nextTimer);
      this.nextTimer = null;
    }
    if (this.safetyInterval !== null) {
      clearInterval(this.safetyInterval);
      this.safetyInterval = null;
    }
  }

  /**
   * Add a cron job
   */
  add(input: CronJobInput): CronJob {
    // Validate
    if (!input.schedule && !input.repeatEvery) {
      throw new Error('Cron job must have either schedule or repeatEvery');
    }

    if (input.schedule) {
      const expanded = expandCronShortcut(input.schedule);
      const error = validateCronExpression(expanded, input.timezone);
      if (error) {
        throw new Error(`Invalid cron expression: ${error}`);
      }
    }

    // Calculate next run
    const now = Date.now();
    let nextRun: number;

    if (input.schedule) {
      const expanded = expandCronShortcut(input.schedule);
      nextRun = getNextCronRun(expanded, now, input.timezone);
    } else {
      nextRun = getNextIntervalRun(input.repeatEvery!, now);
    }

    // Create cron job with generation tracking
    const cron = createCronJob(input, nextRun);

    // Preserve existing executions count when upserting
    const existing = this.cronJobs.get(cron.name);
    if (existing) {
      cron.executions = existing.cron.executions;
      // Reset per-name overlap tracking on upsert: the redefined cron starts
      // fresh and must not be blocked by the previous definition's last fire (H10)
      this.lastFiredAt.delete(cron.name);
    }

    // Handle immediately option: set nextRun to now so it fires on next tick
    // Only on first creation, not on upsert (otherwise it overrides skipMissedOnRestart)
    if (input.immediately && !existing) {
      cron.nextRun = Date.now();
    }

    const gen = this.generation++;
    this.cronJobs.set(cron.name, { cron, generation: gen });
    this.cronHeap.push({ cron, generation: gen });

    // Reschedule if this cron might be sooner than current timer
    if (this.started) {
      this.scheduleNext();
    }

    return cron;
  }

  /**
   * Remove a cron job - O(1) with lazy deletion
   * The heap entry becomes stale and will be skipped in tick()
   */
  remove(name: string): boolean {
    const entry = this.cronJobs.get(name);
    if (!entry) return false;

    // Just remove from map - heap entry becomes stale (lazy deletion)
    this.cronJobs.delete(name);
    // Clear per-name overlap tracking so a future cron re-added with the same
    // name isn't blocked by this cron's stale last-fire timestamp (fixes H10)
    this.lastFiredAt.delete(name);

    // Reschedule in case removed cron was the next timer target
    if (this.started) {
      this.scheduleNext();
    }

    return true;
  }

  /**
   * Get a cron job by name
   */
  get(name: string): CronJob | undefined {
    return this.cronJobs.get(name)?.cron;
  }

  /**
   * List all cron jobs
   */
  list(): CronJob[] {
    return Array.from(this.cronJobs.values()).map((e) => e.cron);
  }

  /**
   * Load cron jobs from storage
   */
  load(crons: CronJob[]): void {
    const now = Date.now();
    const entries: CronHeapEntry[] = [];
    for (const cron of crons) {
      // Recalculate past nextRun to the future on restart (fixes #73)
      // Default: always skip missed crons unless explicitly opted out
      if ((cron.skipMissedOnRestart || cron.skipIfNoWorker) && cron.nextRun < now) {
        if (cron.schedule) {
          cron.nextRun = getNextCronRun(cron.schedule, now, cron.timezone ?? undefined);
        } else if (cron.repeatEvery) {
          cron.nextRun = getNextIntervalRun(cron.repeatEvery, now);
        }
        // Persist the recalculated nextRun to the DB
        this.persistCron?.(cron.name, cron.executions, cron.nextRun);
      }
      const gen = this.generation++;
      this.cronJobs.set(cron.name, { cron, generation: gen });
      entries.push({ cron, generation: gen });
    }
    // Rebuild heap from loaded crons - O(n)
    this.cronHeap.buildFrom(entries);

    // Reschedule if running
    if (this.started) {
      this.scheduleNext();
    }
  }

  /**
   * Schedule a precise setTimeout for the next due cron
   * Called after every mutation (add, remove, load, tick)
   */
  private scheduleNext(): void {
    if (!this.started) return;

    // Cancel existing precise timer
    if (this.nextTimer !== null) {
      clearTimeout(this.nextTimer);
      this.nextTimer = null;
    }

    // Find next non-stale entry, popping stale ones from the heap
    while (!this.cronHeap.isEmpty) {
      const entry = this.cronHeap.peek();
      if (!entry) return;

      // Check if stale (cron was removed or updated since this heap entry was created)
      if (this.cronJobs.get(entry.cron.name)?.generation !== entry.generation) {
        this.cronHeap.pop(); // Remove stale entry and continue looking
        continue;
      }

      const delay = Math.max(0, entry.cron.nextRun - Date.now());
      this.nextTimer = setTimeout(() => {
        this.nextTimer = null;
        void this.tick();
      }, delay);
      return;
    }
  }

  /**
   * Check and execute due cron jobs
   * O(k log n) where k = number of due crons, instead of O(n) full scan
   * Skips stale entries (lazy deletion) automatically
   */
  private async tick(): Promise<void> {
    if (!this.pushJob) return;

    const now = Date.now();
    const toReinsert: CronHeapEntry[] = [];
    const toRemove: string[] = [];

    // Process only due crons from heap - O(k log n)
    while (!this.cronHeap.isEmpty) {
      const entry = this.cronHeap.peek();
      if (!entry || entry.cron.nextRun > now) break;

      // Remove from heap
      this.cronHeap.pop();

      // Check if stale (cron was removed or updated)
      const current = this.cronJobs.get(entry.cron.name);
      if (current?.generation !== entry.generation) {
        // Stale entry, skip
        continue;
      }

      const cron = entry.cron;

      // Check if at limit
      if (isAtLimit(cron)) {
        toRemove.push(cron.name);
        continue;
      }

      try {
        // Calculate new state BEFORE pushing job
        const newExecutions = cron.executions + 1;
        const executionTime = Date.now();
        // The slot this fire was scheduled for (anchor for fixed-rate intervals)
        const scheduledRun = cron.nextRun;
        let newNextRun: number;
        if (cron.schedule) {
          const expanded = expandCronShortcut(cron.schedule);
          newNextRun = getNextCronRun(expanded, executionTime, cron.timezone ?? undefined);
        } else if (cron.repeatEvery) {
          // Fixed-rate: anchor to the scheduled slot, not wall-clock now, so a
          // slow/late job does not cumulatively drift the schedule forward (M4)
          newNextRun = getNextIntervalRun(cron.repeatEvery, scheduledRun);
        } else {
          newNextRun = executionTime;
        }

        // PERSIST STATE FIRST (before pushing job to prevent duplicates)
        if (this.persistCron) {
          try {
            this.persistCron(cron.name, newExecutions, newNextRun);
          } catch (persistErr) {
            // Persist failed - do NOT push job, retry on next tick
            cronLog.error('Failed to persist cron state, skipping job push', {
              name: cron.name,
              error: String(persistErr),
            });
            this.dashboardEmit?.('cron:missed', {
              name: cron.name,
              queue: cron.queue,
              error: String(persistErr),
            });
            toReinsert.push(entry);
            continue;
          }
        }

        // Update in-memory state AFTER successful persist
        cron.executions = newExecutions;
        cron.nextRun = newNextRun;

        // NOW push the job (state already persisted, safe from duplicates)
        await this.fireCronJob(cron, now);

        // Re-insert with same generation (not stale)
        toReinsert.push(entry);
      } catch (err) {
        // Push failed but state was already persisted
        // Job is lost but cron state is consistent - no duplicates on retry
        cronLog.error('Failed to push cron job (state already persisted)', {
          name: cron.name,
          error: String(err),
        });
        this.dashboardEmit?.('cron:missed', {
          name: cron.name,
          queue: cron.queue,
          error: String(err),
        });
        // Re-insert to continue scheduling (next execution will work)
        toReinsert.push(entry);
      }
    }

    // Re-insert processed crons with updated nextRun
    for (const entry of toReinsert) {
      this.cronHeap.push(entry);
    }

    // Remove limit-reached crons from map
    for (const name of toRemove) {
      this.cronJobs.delete(name);
    }

    // Schedule next precise wake-up
    this.scheduleNext();
  }

  /** Fire a cron job with overlap and worker detection */
  private async fireCronJob(cron: CronJob, now: number): Promise<void> {
    // skipIfNoWorker: skip if no workers registered for this queue
    if (cron.skipIfNoWorker && this.hasWorkers && !this.hasWorkers(cron.queue)) {
      this.dashboardEmit?.('cron:skipped', {
        name: cron.name,
        queue: cron.queue,
        reason: 'no-worker',
      });
      return;
    }

    // Overlap detection: skip if last fire was within repeatEvery window
    const lastFire = this.lastFiredAt.get(cron.name);
    const interval = cron.repeatEvery ?? 60000;
    if (lastFire && now - lastFire < interval * 0.8) {
      this.dashboardEmit?.('cron:skipped', {
        name: cron.name,
        queue: cron.queue,
        reason: 'overlap',
      });
      return;
    }

    // preventOverlap: auto-set uniqueKey to block pushes while previous job is active
    const effectiveUniqueKey =
      cron.uniqueKey ?? (cron.preventOverlap ? `cron:${cron.name}` : undefined);
    const opts = cron.jobOptions;
    await this.pushJob!(cron.queue, {
      data: cron.data,
      priority: cron.priority,
      uniqueKey: effectiveUniqueKey,
      dedup: cron.dedup ?? undefined,
      maxAttempts: opts?.maxAttempts,
      backoff: opts?.backoff,
      timeout: opts?.timeout,
      delay: opts?.delay,
      stallTimeout: opts?.stallTimeout,
      removeOnComplete: opts?.removeOnComplete,
      removeOnFail: opts?.removeOnFail,
    });
    this.lastFiredAt.set(cron.name, now);
    this.dashboardEmit?.('cron:fired', { name: cron.name, queue: cron.queue });
  }

  /**
   * Get scheduler stats
   * O(1) for nextRun using min-heap peek (skips stale entries)
   */
  getStats(): { total: number; pending: number; nextRun: number | null } {
    // Find first non-stale entry for nextRun
    let nextRun: number | null = null;

    // Peek and skip stale entries to find valid nextRun
    // This is typically O(1) as stale entries are cleaned during tick()
    const entry = this.cronHeap.peek();
    if (entry) {
      const current = this.cronJobs.get(entry.cron.name);
      if (current?.generation === entry.generation && !isAtLimit(entry.cron)) {
        nextRun = entry.cron.nextRun;
      }
    }

    // Count pending (crons not at limit) - still O(n) but called rarely
    let pending = 0;
    for (const { cron } of this.cronJobs.values()) {
      if (!isAtLimit(cron)) {
        pending++;
      }
    }

    return {
      total: this.cronJobs.size,
      pending,
      nextRun,
    };
  }
}
