/**
 * Bunqueue — Priority Aging
 * Automatically boosts priority of old waiting jobs.
 */

import type { Queue } from '../queue/queue';
import type { PriorityAgingConfig } from './types';

export class PriorityAger<T = unknown> {
  private timer: ReturnType<typeof setInterval> | null = null;
  private generation = 0;
  private readonly config: PriorityAgingConfig;
  private readonly queue: Queue<T>;

  constructor(config: PriorityAgingConfig, queue: Queue<T>) {
    this.config = config;
    this.queue = queue;
  }

  start(): void {
    if (this.timer !== null) return;
    const interval = this.config.interval ?? 60000;
    const generation = ++this.generation;
    this.timer = setInterval(() => {
      void this.tick(generation);
    }, interval);
  }

  private async tick(generation: number): Promise<void> {
    if (generation !== this.generation) return;
    const minAge = this.config.minAge ?? 60000;
    const boost = this.config.boost ?? 1;
    const maxPriority = this.config.maxPriority ?? 100;
    const maxScan = this.config.maxScan ?? 100;

    // Get both waiting and prioritized jobs (jobs with priority > 0 are "prioritized")
    const [waiting, prioritized] = await Promise.all([
      this.queue.getWaitingAsync(0, maxScan),
      this.queue.getJobsAsync({ state: 'prioritized', start: 0, end: maxScan }),
    ]);
    if (generation !== this.generation) return;
    const jobs = [...waiting, ...prioritized];
    const now = Date.now();

    for (const job of jobs) {
      if (generation !== this.generation) return;
      const age = now - job.timestamp;
      if (age >= minAge && job.priority < maxPriority) {
        const newPriority = Math.min(job.priority + boost, maxPriority);
        try {
          await this.queue.changeJobPriority(job.id, {
            priority: newPriority,
          });
        } catch {
          // Best-effort — job may have been processed
        }
      }
    }
  }

  destroy(): void {
    this.generation++;
    const timer = this.timer;
    this.timer = null;
    if (timer !== null) clearInterval(timer);
  }
}
