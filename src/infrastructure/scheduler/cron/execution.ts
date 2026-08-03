import { isAtLimit, type CronJob } from '../../../domain/types/cron';
import { cronLog } from '../../../shared/logger';
import { expandCronShortcut, getNextCronRun, getNextIntervalRun } from '../cronParser';
import type { CronHeapEntry, PushJobCallback } from '../types/cronScheduler';
import { CronRuntime } from './runtime';

/** Due-job execution and scheduler statistics. */
export class CronExecution extends CronRuntime {
  protected async tick(): Promise<void> {
    if (!this.pushJob) return;

    const now = Date.now();
    const toReinsert: CronHeapEntry[] = [];
    const toRemove: string[] = [];

    while (!this.cronHeap.isEmpty) {
      const entry = this.cronHeap.peek();
      if (!entry || entry.cron.nextRun > now) break;
      this.cronHeap.pop();

      const current = this.cronJobs.get(entry.cron.name);
      if (current?.generation !== entry.generation) continue;

      const cron = entry.cron;
      if (isAtLimit(cron)) {
        toRemove.push(cron.name);
        continue;
      }

      try {
        const newExecutions = cron.executions + 1;
        const executionTime = Date.now();
        const scheduledRun = cron.nextRun;
        let newNextRun: number;
        if (cron.schedule) {
          newNextRun = getNextCronRun(
            expandCronShortcut(cron.schedule),
            executionTime,
            cron.timezone ?? undefined
          );
        } else if (cron.repeatEvery) {
          newNextRun = getNextIntervalRun(cron.repeatEvery, scheduledRun);
        } else {
          newNextRun = executionTime;
        }

        const skipReason = this.getSkipReason(cron, now);
        if (skipReason) {
          if (this.persistCron) {
            try {
              this.persistCron(cron.name, cron.executions, newNextRun);
            } catch (error) {
              cronLog.error('Failed to persist cron nextRun on skip', {
                name: cron.name,
                error: String(error),
              });
            }
          }
          cron.nextRun = newNextRun;
          this.dashboardEmit?.('cron:skipped', {
            name: cron.name,
            queue: cron.queue,
            reason: skipReason,
          });
          toReinsert.push(entry);
          continue;
        }

        if (this.persistCron) {
          try {
            this.persistCron(cron.name, newExecutions, newNextRun);
          } catch (error) {
            cronLog.error('Failed to persist cron state, skipping job push', {
              name: cron.name,
              error: String(error),
            });
            this.dashboardEmit?.('cron:missed', {
              name: cron.name,
              queue: cron.queue,
              error: String(error),
            });
            toReinsert.push(entry);
            continue;
          }
        }

        cron.executions = newExecutions;
        cron.nextRun = newNextRun;
        await this.fireCronJob(cron, now);
        toReinsert.push(entry);
      } catch (error) {
        cronLog.error('Failed to push cron job (state already persisted)', {
          name: cron.name,
          error: String(error),
        });
        this.dashboardEmit?.('cron:missed', {
          name: cron.name,
          queue: cron.queue,
          error: String(error),
        });
        toReinsert.push(entry);
      }
    }

    for (const entry of toReinsert) this.cronHeap.push(entry);
    for (const name of toRemove) this.cronJobs.delete(name);
    this.scheduleNext();
  }

  private getSkipReason(cron: CronJob, now: number): 'no-worker' | 'overlap' | null {
    if (cron.skipIfNoWorker && this.hasWorkers && !this.hasWorkers(cron.queue)) {
      return 'no-worker';
    }
    const lastFire = this.lastFiredAt.get(cron.name);
    const interval = cron.repeatEvery ?? 60000;
    if (lastFire && now - lastFire < interval * 0.8) return 'overlap';
    return null;
  }

  private async fireCronJob(cron: CronJob, now: number): Promise<void> {
    const effectiveUniqueKey =
      cron.uniqueKey ?? (cron.preventOverlap ? `cron:${cron.name}` : undefined);
    const options = cron.jobOptions;
    await (this.pushJob as PushJobCallback)(cron.queue, {
      name: cron.jobName,
      data: cron.data,
      priority: cron.priority,
      uniqueKey: effectiveUniqueKey,
      dedup: cron.dedup ?? undefined,
      maxAttempts: options?.maxAttempts,
      backoff: options?.backoff,
      timeout: options?.timeout,
      delay: options?.delay,
      stallTimeout: options?.stallTimeout,
      removeOnComplete: options?.removeOnComplete,
      removeOnFail: options?.removeOnFail,
    });
    this.lastFiredAt.set(cron.name, now);
    this.dashboardEmit?.('cron:fired', { name: cron.name, queue: cron.queue });
  }

  getStats(): { total: number; pending: number; nextRun: number | null } {
    let nextRun: number | null = null;
    const entry = this.cronHeap.peek();
    if (entry) {
      const current = this.cronJobs.get(entry.cron.name);
      if (current?.generation === entry.generation && !isAtLimit(entry.cron)) {
        nextRun = entry.cron.nextRun;
      }
    }

    let pending = 0;
    for (const { cron } of this.cronJobs.values()) {
      if (!isAtLimit(cron)) pending++;
    }
    return { total: this.cronJobs.size, pending, nextRun };
  }
}
