import { MinHeap } from './heap';
import type { SimJob } from './types';

const COMPLETED_KEEP = 30;
const DLQ_KEEP = 50;

// One logical queue: waiting (priority order), delayed (time order),
// active set, recent completed, dead letters. Pause, drain and a
// token-bucket rate limit — the same controls real bunqueue exposes.
export class QueueSim {
  readonly waiting: MinHeap<SimJob>;
  readonly delayed: MinHeap<SimJob>;
  readonly active = new Map<number, SimJob>();
  completed: SimJob[] = [];
  dlq: SimJob[] = [];

  paused = false;
  rateLimit = 0; // jobs/sec, 0 = off
  private tokens = 0;
  lastPushAt = Number.NEGATIVE_INFINITY;
  durationRange: [number, number] | null = null; // per-queue override

  counters = { pushed: 0, completed: 0, failed: 0, retried: 0, dead: 0 };

  constructor(
    readonly name: string,
    readonly shardIndex: number,
  ) {
    // Higher priority first; FIFO (by id) within the same priority.
    this.waiting = new MinHeap<SimJob>((a, b) =>
      a.priority !== b.priority ? a.priority > b.priority : a.id < b.id,
    );
    this.delayed = new MinHeap<SimJob>((a, b) => a.runAt < b.runAt);
  }

  enqueue(job: SimJob, simTime: number): void {
    this.lastPushAt = simTime;
    this.counters.pushed++;
    if (job.runAt > simTime) {
      job.state = 'delayed';
      this.delayed.push(job);
    } else {
      job.state = 'waiting';
      this.waiting.push(job);
    }
  }

  // Move due delayed jobs (initial delay elapsed or retry backoff expired)
  // into the waiting heap.
  promoteDue(simTime: number): void {
    for (;;) {
      const next = this.delayed.peek();
      if (!next || next.runAt > simTime) break;
      this.delayed.pop();
      next.state = 'waiting';
      this.waiting.push(next);
    }
  }

  refillTokens(dtMs: number): void {
    if (this.rateLimit <= 0) return;
    const burst = Math.max(1, this.rateLimit);
    this.tokens = Math.min(burst, this.tokens + (this.rateLimit * dtMs) / 1000);
  }

  setRateLimit(jobsPerSec: number): void {
    this.rateLimit = Math.max(0, jobsPerSec);
    this.tokens = this.rateLimit > 0 ? 1 : 0;
  }

  // Hand the highest-priority ready job to a worker, honoring pause
  // and the rate limit.
  take(): SimJob | null {
    if (this.paused) return null;
    if (this.waiting.size === 0) return null;
    if (this.rateLimit > 0) {
      if (this.tokens < 1) return null;
      this.tokens -= 1;
    }
    return this.waiting.pop() ?? null;
  }

  recordCompleted(job: SimJob): void {
    this.counters.completed++;
    this.completed.push(job);
    if (this.completed.length > COMPLETED_KEEP) {
      this.completed = this.completed.slice(-COMPLETED_KEEP);
    }
  }

  recordDead(job: SimJob): void {
    this.counters.dead++;
    this.dlq.push(job);
    if (this.dlq.length > DLQ_KEEP) {
      this.dlq = this.dlq.slice(-DLQ_KEEP);
    }
  }

  // Remove all waiting + delayed jobs (active ones finish normally).
  drain(): number {
    return this.waiting.clear().length + this.delayed.clear().length;
  }

  // Re-enqueue every dead letter with a fresh attempt budget.
  retryDlq(simTime: number): number {
    const entries = this.dlq;
    this.dlq = [];
    for (const job of entries) {
      job.attemptsMade = 0;
      job.failedReason = null;
      job.progress = 0;
      job.runAt = simTime;
      job.state = 'waiting';
      this.waiting.push(job);
    }
    return entries.length;
  }

  get depth(): number {
    return this.waiting.size + this.delayed.size + this.active.size;
  }
}
