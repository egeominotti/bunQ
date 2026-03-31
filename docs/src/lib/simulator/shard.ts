/**
 * Shard Implementation
 * Contains queues, DLQ, and stats for a single shard
 */

import type { Job, DlqEntry, ShardStats, JobState } from './types';
import { PriorityQueue } from './priority-queue';

export class Shard {
  readonly index: number;
  private queues: Map<string, PriorityQueue> = new Map();
  private dlq: Map<string, DlqEntry[]> = new Map();
  private pausedQueues: Set<string> = new Set();
  private activeJobs: Map<string, Job> = new Map();
  private completedJobs: Map<string, Job> = new Map();
  private failedJobs: Map<string, Job> = new Map();

  constructor(index: number) {
    this.index = index;
  }

  getQueue(name: string): PriorityQueue {
    let queue = this.queues.get(name);
    if (!queue) {
      queue = new PriorityQueue();
      this.queues.set(name, queue);
    }
    return queue;
  }

  push(job: Job): void {
    this.getQueue(job.queue).push(job);
  }

  pull(queueName: string): Job | undefined {
    if (this.pausedQueues.has(queueName)) {
      return undefined;
    }

    const queue = this.queues.get(queueName);
    if (!queue) return undefined;

    const job = queue.pop();
    if (job) {
      job.state = 'active';
      job.processedAt = Date.now();
      this.activeJobs.set(job.id, job);
    }
    return job;
  }

  complete(jobId: string, result?: unknown): Job | undefined {
    const job = this.activeJobs.get(jobId);
    if (!job) return undefined;

    this.activeJobs.delete(jobId);
    job.state = 'completed';
    job.completedAt = Date.now();
    job.result = result;
    this.completedJobs.set(jobId, job);

    // Keep only last 100 completed
    if (this.completedJobs.size > 100) {
      const first = this.completedJobs.keys().next().value;
      if (first) this.completedJobs.delete(first);
    }

    return job;
  }

  fail(jobId: string, error: string, maxAttempts: number): { job: Job; toDlq: boolean } | undefined {
    const job = this.activeJobs.get(jobId);
    if (!job) return undefined;

    this.activeJobs.delete(jobId);
    job.attempts++;
    job.error = error;

    if (job.attempts >= maxAttempts) {
      // Move to DLQ
      job.state = 'dlq';
      job.failedAt = Date.now();
      this.addToDlq(job, 'max_attempts', error);
      return { job, toDlq: true };
    }

    // Retry with exponential backoff
    job.state = 'waiting';
    const backoffMs = job.backoff * Math.pow(2, job.attempts);
    job.runAt = Date.now() + backoffMs;
    this.push(job);
    return { job, toDlq: false };
  }

  addToDlq(job: Job, reason: DlqEntry['reason'], error: string): void {
    const entry: DlqEntry = {
      id: `dlq-${job.id}`,
      job,
      reason,
      error,
      enteredAt: Date.now(),
      retryCount: 0,
    };

    let queueDlq = this.dlq.get(job.queue);
    if (!queueDlq) {
      queueDlq = [];
      this.dlq.set(job.queue, queueDlq);
    }
    queueDlq.push(entry);

    // Keep only last 100 in DLQ per queue
    if (queueDlq.length > 100) {
      queueDlq.shift();
    }
  }

  getDlq(queueName: string): DlqEntry[] {
    return this.dlq.get(queueName) || [];
  }

  retryDlq(queueName: string): number {
    const entries = this.dlq.get(queueName);
    if (!entries || entries.length === 0) return 0;

    let retried = 0;
    for (const entry of entries) {
      entry.job.state = 'waiting';
      entry.job.attempts = 0;
      entry.job.runAt = Date.now();
      entry.retryCount++;
      this.push(entry.job);
      retried++;
    }

    this.dlq.set(queueName, []);
    return retried;
  }

  purgeDlq(queueName: string): number {
    const entries = this.dlq.get(queueName);
    const count = entries?.length || 0;
    this.dlq.set(queueName, []);
    return count;
  }

  pause(queueName: string): void {
    this.pausedQueues.add(queueName);
  }

  resume(queueName: string): void {
    this.pausedQueues.delete(queueName);
  }

  isPaused(queueName: string): boolean {
    return this.pausedQueues.has(queueName);
  }

  drain(queueName: string): number {
    const queue = this.queues.get(queueName);
    if (!queue) return 0;
    const count = queue.size;
    this.queues.set(queueName, new PriorityQueue());
    return count;
  }

  getJob(jobId: string): Job | undefined {
    // Check all locations
    for (const queue of this.queues.values()) {
      const job = queue.get(jobId);
      if (job) return job;
    }
    return this.activeJobs.get(jobId) ||
           this.completedJobs.get(jobId) ||
           this.failedJobs.get(jobId);
  }

  getQueueNames(): string[] {
    return Array.from(this.queues.keys());
  }

  getStats(queueName?: string): ShardStats {
    let queuedJobs = 0;
    let delayedJobs = 0;
    let activeJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;
    let dlqJobs = 0;

    const now = Date.now();

    if (queueName) {
      const queue = this.queues.get(queueName);
      if (queue) {
        for (const job of queue.getAll()) {
          if (job.runAt > now) delayedJobs++;
          else queuedJobs++;
        }
      }
      for (const job of this.activeJobs.values()) {
        if (job.queue === queueName) activeJobs++;
      }
      for (const job of this.completedJobs.values()) {
        if (job.queue === queueName) completedJobs++;
      }
      dlqJobs = this.dlq.get(queueName)?.length || 0;
    } else {
      for (const queue of this.queues.values()) {
        for (const job of queue.getAll()) {
          if (job.runAt > now) delayedJobs++;
          else queuedJobs++;
        }
      }
      activeJobs = this.activeJobs.size;
      completedJobs = this.completedJobs.size;
      for (const entries of this.dlq.values()) {
        dlqJobs += entries.length;
      }
    }

    return { queuedJobs, delayedJobs, activeJobs, completedJobs, failedJobs, dlqJobs };
  }

  getAllJobs(): Job[] {
    const jobs: Job[] = [];
    for (const queue of this.queues.values()) {
      jobs.push(...queue.getAll());
    }
    jobs.push(...this.activeJobs.values());
    jobs.push(...this.completedJobs.values());
    return jobs;
  }

  getActiveJobs(): Job[] {
    return Array.from(this.activeJobs.values());
  }

  updateProgress(jobId: string, progress: number): boolean {
    const job = this.activeJobs.get(jobId);
    if (job) {
      job.progress = Math.min(100, Math.max(0, progress));
      return true;
    }
    return false;
  }
}
