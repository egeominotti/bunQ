/**
 * Bunqueue Simple Mode — event triggers.
 * 1:1 port of src/client/bunqueue/triggers.ts: when a job completes/fails,
 * automatically create another job.
 */

import type { Job } from '../job.js';
import type { Queue } from '../queue.js';
import type { Worker } from '../worker.js';
import type { TriggerRule } from './types.js';

export class TriggerManager<T = unknown, R = unknown> {
  private readonly rules: TriggerRule<T>[] = [];
  private active = false;
  private readonly queue: Queue<T>;
  private readonly worker: Worker<T, R>;

  constructor(queue: Queue<T>, worker: Worker<T, R>) {
    this.queue = queue;
    this.worker = worker;
  }

  add(rule: TriggerRule<T>): void {
    this.rules.push(rule);
    this.ensureActive();
  }

  private ensureActive(): void {
    if (this.active) return;
    this.active = true;

    this.worker.on('completed', (job, result) => {
      this.fire('completed', job as Job<T>, result);
    });

    this.worker.on('failed', (job, error) => {
      this.fire('failed', job as Job<T>, error);
    });
  }

  private fire(event: 'completed' | 'failed', job: Job<T>, resultOrError: unknown): void {
    for (const rule of this.rules) {
      if (rule.on !== job.name) continue;
      if ((rule.event ?? 'completed') !== event) continue;
      if (rule.condition && !rule.condition(resultOrError, job)) continue;

      const data = rule.data(resultOrError, job);
      void this.queue.add(rule.create, data, rule.opts);
    }
  }
}
