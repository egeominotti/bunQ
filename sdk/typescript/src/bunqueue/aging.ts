/**
 * Bunqueue Simple Mode — priority aging.
 * Port of src/client/bunqueue/aging.ts: automatically boosts the priority of
 * old waiting jobs. Same semantics; the job scan goes over TCP (GetJobs on
 * the waiting and prioritized states) instead of the embedded manager.
 */

import type { Queue } from '../queue.js';
import type { PriorityAgingConfig } from './types.js';

export class PriorityAger<T = unknown> {
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly config: PriorityAgingConfig;
  private readonly queue: Queue<T>;

  constructor(config: PriorityAgingConfig, queue: Queue<T>) {
    this.config = config;
    this.queue = queue;
  }

  start(): void {
    const interval = this.config.interval ?? 60000;
    this.timer = setInterval(() => {
      void this.tick();
    }, interval);
    this.timer.unref?.();
  }

  private async tick(): Promise<void> {
    const minAge = this.config.minAge ?? 60000;
    const boost = this.config.boost ?? 1;
    const maxPriority = this.config.maxPriority ?? 100;
    const maxScan = this.config.maxScan ?? 100;

    // Both waiting and prioritized jobs (priority > 0 means "prioritized")
    const [waiting, prioritized] = await Promise.all([
      this.queue.getJobs({ state: 'waiting', start: 0, end: maxScan }),
      this.queue.getJobs({ state: 'prioritized', start: 0, end: maxScan }),
    ]);
    const jobs = [...waiting, ...prioritized];
    const now = Date.now();

    for (const job of jobs) {
      const age = now - job.timestamp;
      if (age >= minAge && job.priority < maxPriority) {
        const newPriority = Math.min(job.priority + boost, maxPriority);
        try {
          await this.queue.changeJobPriority(job.id, { priority: newPriority });
        } catch {
          // Best-effort — the job may have been processed meanwhile
        }
      }
    }
  }

  destroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
