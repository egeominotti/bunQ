import { createCronJob, type CronJob, type CronJobInput } from '../../../domain/types/cron';
import { MinHeap } from '../../../shared/minHeap';
import {
  expandCronShortcut,
  getNextCronRun,
  getNextIntervalRun,
  validateCronExpression,
} from '../cronParser';
import type {
  CronHeapEntry,
  CronRegistryEntry,
  CronSchedulerConfig,
  PersistCronCallback,
  PushJobCallback,
} from '../types/cronScheduler';

const SAFETY_FALLBACK_MS = 60_000;
const MAX_TIMER_DELAY_MS = 2_147_483_647;

/** Registry, timers, and schedule mutation shared with cron execution. */
export abstract class CronRuntime {
  protected readonly cronJobs = new Map<string, CronRegistryEntry>();
  protected readonly cronHeap = new MinHeap<CronHeapEntry>(
    (a, b) => a.cron.nextRun - b.cron.nextRun
  );
  protected generation = 0;
  protected nextTimer: ReturnType<typeof setTimeout> | null = null;
  protected safetyInterval: ReturnType<typeof setInterval> | null = null;
  protected started = false;
  protected pushJob: PushJobCallback | null = null;
  protected persistCron: PersistCronCallback | null = null;
  protected hasWorkers: ((queue: string) => boolean) | null = null;
  protected dashboardEmit: ((event: string, data: Record<string, unknown>) => void) | null = null;
  protected readonly lastFiredAt = new Map<string, number>();

  protected abstract tick(): Promise<void>;

  constructor(config?: CronSchedulerConfig) {
    void config;
  }

  setPushCallback(callback: PushJobCallback): void {
    this.pushJob = callback;
  }

  setPersistCallback(callback: PersistCronCallback): void {
    this.persistCron = callback;
  }

  setWorkerCheckCallback(callback: (queue: string) => boolean): void {
    this.hasWorkers = callback;
  }

  setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void {
    this.dashboardEmit = callback;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.safetyInterval = setInterval(() => {
      void this.tick();
    }, SAFETY_FALLBACK_MS);
    this.scheduleNext();
  }

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

  add(input: CronJobInput): CronJob {
    if (!input.schedule && !input.repeatEvery) {
      throw new Error('Cron job must have either schedule or repeatEvery');
    }

    if (input.schedule) {
      const expanded = expandCronShortcut(input.schedule);
      const error = validateCronExpression(expanded, input.timezone);
      if (error) throw new Error(`Invalid cron expression: ${error}`);
    }

    const now = Date.now();
    const nextRun = input.schedule
      ? getNextCronRun(expandCronShortcut(input.schedule), now, input.timezone)
      : getNextIntervalRun(input.repeatEvery as number, now);
    const cron = createCronJob(input, nextRun);
    const existing = this.cronJobs.get(cron.name);
    if (existing) {
      cron.executions = existing.cron.executions;
      this.lastFiredAt.delete(cron.name);
    }
    if (input.immediately && !existing) cron.nextRun = Date.now();

    const generation = this.generation++;
    this.cronJobs.set(cron.name, { cron, generation });
    this.cronHeap.push({ cron, generation });
    if (this.started) this.scheduleNext();
    return cron;
  }

  remove(name: string): boolean {
    const entry = this.cronJobs.get(name);
    if (!entry) return false;
    this.cronJobs.delete(name);
    this.lastFiredAt.delete(name);
    if (this.started) this.scheduleNext();
    return true;
  }

  get(name: string): CronJob | undefined {
    return this.cronJobs.get(name)?.cron;
  }

  list(): CronJob[] {
    return Array.from(this.cronJobs.values()).map((entry) => entry.cron);
  }

  load(crons: CronJob[]): void {
    const now = Date.now();
    const entries: CronHeapEntry[] = [];
    for (const cron of crons) {
      if ((cron.skipMissedOnRestart || cron.skipIfNoWorker) && cron.nextRun < now) {
        if (cron.schedule) {
          cron.nextRun = getNextCronRun(cron.schedule, now, cron.timezone ?? undefined);
        } else if (cron.repeatEvery) {
          cron.nextRun = getNextIntervalRun(cron.repeatEvery, now);
        }
        this.persistCron?.(cron.name, cron.executions, cron.nextRun);
      }
      const generation = this.generation++;
      this.cronJobs.set(cron.name, { cron, generation });
      entries.push({ cron, generation });
    }
    this.cronHeap.buildFrom(entries);
    if (this.started) this.scheduleNext();
  }

  protected scheduleNext(): void {
    if (!this.started) return;
    if (this.nextTimer !== null) {
      clearTimeout(this.nextTimer);
      this.nextTimer = null;
    }

    while (!this.cronHeap.isEmpty) {
      const entry = this.cronHeap.peek();
      if (!entry) return;
      if (this.cronJobs.get(entry.cron.name)?.generation !== entry.generation) {
        this.cronHeap.pop();
        continue;
      }

      const delay = Math.min(Math.max(0, entry.cron.nextRun - Date.now()), MAX_TIMER_DELAY_MS);
      this.nextTimer = setTimeout(() => {
        this.nextTimer = null;
        void this.tick();
      }, delay);
      return;
    }
  }
}
