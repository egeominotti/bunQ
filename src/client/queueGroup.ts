/**
 * QueueGroup - Namespace isolation for queues
 */

import { Queue } from './queue';
import { Worker } from './worker';
import { getSharedManager, peekSharedManager } from './manager';
import type { QueueOptions, WorkerOptions, Processor } from './types';

/**
 * QueueGroup provides namespace isolation for queues.
 * All queues in a group share a common prefix.
 *
 * @example
 * ```typescript
 * const billing = new QueueGroup('billing');
 *
 * const invoices = billing.getQueue('invoices');
 * const payments = billing.getQueue('payments');
 *
 * // Creates queues: "billing:invoices", "billing:payments"
 *
 * billing.pauseAll(); // Pauses all billing queues
 * ```
 */
export class QueueGroup {
  readonly prefix: string;
  private readonly queues = new Map<string, Queue<unknown>>();

  constructor(namespace: string) {
    this.prefix = namespace.endsWith(':') ? namespace : `${namespace}:`;
  }

  /** Get a queue within this group */
  getQueue<T = unknown>(name: string, opts?: QueueOptions): Queue<T> {
    const queue = new Queue<T>(this.prefix + name, opts);
    this.queues.set(name, queue as Queue<unknown>);
    return queue;
  }

  /** Create a worker for a queue in this group */
  getWorker<T = unknown, R = unknown>(
    name: string,
    processor: Processor<T, R>,
    opts?: WorkerOptions
  ): Worker<T, R> {
    return new Worker<T, R>(this.prefix + name, processor, opts);
  }

  /** List all queues in this group */
  listQueues(): string[] {
    const manager = getSharedManager();
    return manager
      .listQueues()
      .filter((q) => q.startsWith(this.prefix))
      .map((q) => q.slice(this.prefix.length));
  }

  /** Pause all queues in this group */
  pauseAll(): void {
    const manager = getSharedManager();
    for (const queue of manager.listQueues()) {
      if (queue.startsWith(this.prefix)) {
        manager.pause(queue);
      }
    }
  }

  /** Resume all queues in this group */
  resumeAll(): void {
    const manager = getSharedManager();
    for (const queue of manager.listQueues()) {
      if (queue.startsWith(this.prefix)) {
        manager.resume(queue);
      }
    }
  }

  /** Drain all queues in this group (remove waiting jobs) */
  drainAll(): void {
    const manager = getSharedManager();
    for (const queue of manager.listQueues()) {
      if (queue.startsWith(this.prefix)) {
        manager.drain(queue);
      }
    }
  }

  /** Obliterate all queues in this group (remove all data) */
  obliterateAll(): void {
    const manager = getSharedManager();
    for (const queue of manager.listQueues()) {
      if (queue.startsWith(this.prefix)) {
        manager.obliterate(queue);
      }
    }
  }

  /** List queues created through this group, including remote queues. */
  async listQueuesAsync(): Promise<string[]> {
    const names = new Set(this.queues.keys());
    for (const name of peekSharedManager()?.listQueues() ?? []) {
      if (name.startsWith(this.prefix)) names.add(name.slice(this.prefix.length));
    }
    return [...names].sort();
  }

  /** Pause every queue created through this group. */
  async pauseAllAsync(): Promise<void> {
    await Promise.all([...this.queues.values()].map((queue) => queue.pauseAsync()));
  }

  /** Resume every queue created through this group. */
  async resumeAllAsync(): Promise<void> {
    await Promise.all([...this.queues.values()].map((queue) => queue.resumeAsync()));
  }

  /** Drain waiting jobs from every queue and return the aggregate count. */
  async drainAllAsync(): Promise<number> {
    const counts = await Promise.all([...this.queues.values()].map((queue) => queue.drainAsync()));
    return counts.reduce((total, count) => total + count, 0);
  }

  /** Remove all data from every queue created through this group. */
  async obliterateAllAsync(): Promise<void> {
    await Promise.all([...this.queues.values()].map((queue) => queue.obliterateAsync()));
  }
}
