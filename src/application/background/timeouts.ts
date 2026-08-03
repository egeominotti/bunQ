import type { Job, JobId } from '../../domain/types/job';
import { FailureReason } from '../../domain/types/dlq';
import { MinHeap } from '../../shared/minHeap';
import { queueLog } from '../../shared/logger';
import type { BackgroundContext } from '../types';

export const MAX_TIMER_DELAY_MS = 2_147_483_647;

interface TimeoutEntry {
  readonly deadline: number;
  readonly jobId: JobId;
  readonly startedAt: number;
}

export function timeoutTimerDelay(deadline: number, now = Date.now()): number {
  return Math.max(1, Math.min(MAX_TIMER_DELAY_MS, deadline - now));
}

function deadlineFor(job: Job): number | null {
  if (!job.timeout || job.startedAt === null) return null;
  const deadline = job.startedAt + job.timeout;
  return Number.isSafeInteger(deadline) ? deadline : Number.MAX_SAFE_INTEGER;
}

function processingJob(ctx: BackgroundContext, id: JobId): Job | null {
  const location = ctx.jobIndex.get(id);
  if (location?.type !== 'processing') return null;
  return ctx.processingShards[location.shardIdx].get(id) ?? null;
}

async function failTimedOutJob(ctx: BackgroundContext, job: Job): Promise<void> {
  await ctx.fail(job.id, 'Job timeout exceeded', FailureReason.Timeout);
  ctx.dashboardEmit?.('job:timeout', {
    jobId: String(job.id),
    queue: job.queue,
    timeout: job.timeout,
  });
}

/** Compatibility entry point for explicit timeout checks in tests and tooling. */
export async function checkJobTimeouts(ctx: BackgroundContext): Promise<void> {
  const now = Date.now();
  const timedOut: Array<{ deadline: number; job: Job }> = [];
  for (const processingShard of ctx.processingShards) {
    for (const job of processingShard.values()) {
      const deadline = deadlineFor(job);
      if (deadline !== null && now >= deadline) timedOut.push({ deadline, job });
    }
  }
  timedOut.sort((a, b) => a.deadline - b.deadline);
  for (const { job } of timedOut) {
    if (processingJob(ctx, job.id) !== job) continue;
    try {
      await failTimedOutJob(ctx, job);
    } catch (error) {
      queueLog.error('Failed to mark timed out job as failed', {
        jobId: String(job.id),
        error: String(error),
      });
    }
  }
}

/** One timer tracks the earliest active processing deadline. */
export class JobTimeoutScheduler {
  private readonly active = new Map<JobId, TimeoutEntry>();
  private readonly deadlines = new MinHeap<TimeoutEntry>((a, b) => {
    if (a.deadline !== b.deadline) return a.deadline - b.deadline;
    return String(a.jobId).localeCompare(String(b.jobId));
  });
  private timer: ReturnType<typeof setTimeout> | null = null;
  private armedDeadline: number | null = null;
  private stopped = true;
  private ctx: BackgroundContext | null = null;

  get pendingCount(): number {
    return this.active.size;
  }

  start(ctx: BackgroundContext): void {
    if (!this.stopped) return;
    this.ctx = ctx;
    this.stopped = false;
    this.armNext();
  }

  schedule(job: Job): void {
    if (this.stopped) return;
    const deadline = deadlineFor(job);
    const startedAt = job.startedAt;
    if (deadline === null || startedAt === null) {
      this.cancel(job.id);
      return;
    }
    const current = this.active.get(job.id);
    if (current?.deadline === deadline && current.startedAt === startedAt) return;
    const entry = { deadline, jobId: job.id, startedAt };
    this.active.set(job.id, entry);
    this.deadlines.push(entry);
    this.maybeCompact();
    if (this.armedDeadline === null || deadline < this.armedDeadline) this.armNext();
  }

  cancel(jobId: JobId): void {
    const current = this.active.get(jobId);
    if (!current) return;
    this.active.delete(jobId);
    this.maybeCompact();
    if (current.deadline === this.armedDeadline) this.armNext();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.armedDeadline = null;
    this.active.clear();
    this.deadlines.clear();
    this.ctx = null;
  }

  private isCurrent(entry: TimeoutEntry): boolean {
    return this.active.get(entry.jobId) === entry;
  }

  private pruneStaleRoots(): void {
    while (true) {
      const next = this.deadlines.peek();
      if (!next || this.isCurrent(next)) return;
      this.deadlines.pop();
    }
  }

  private maybeCompact(): void {
    const stale = this.deadlines.size - this.active.size;
    if (stale >= 256 && stale >= this.active.size) {
      this.deadlines.buildFrom([...this.active.values()]);
    }
  }

  private armNext(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.armedDeadline = null;
    if (this.stopped) return;
    this.pruneStaleRoots();
    const next = this.deadlines.peek();
    if (!next) return;
    this.armedDeadline = next.deadline;
    this.timer = setTimeout(() => void this.expireDue(), timeoutTimerDelay(next.deadline));
  }

  private async expireDue(): Promise<void> {
    this.timer = null;
    this.armedDeadline = null;
    const ctx = this.ctx;
    if (this.stopped || !ctx) return;
    const now = Date.now();
    const due: TimeoutEntry[] = [];
    this.pruneStaleRoots();
    while (true) {
      const next = this.deadlines.peek();
      if (!next || next.deadline > now) break;
      const entry = this.deadlines.pop();
      if (!entry) break;
      if (!this.isCurrent(entry)) continue;
      this.active.delete(entry.jobId);
      due.push(entry);
    }

    for (const entry of due) {
      const job = processingJob(ctx, entry.jobId);
      const deadline = job ? deadlineFor(job) : null;
      if (!job || job.startedAt !== entry.startedAt || deadline === null || deadline > now)
        continue;
      try {
        await failTimedOutJob(ctx, job);
      } catch (error) {
        queueLog.error('Failed to mark timed out job as failed', {
          jobId: String(entry.jobId),
          error: String(error),
        });
        const current = processingJob(ctx, entry.jobId);
        if (current?.startedAt === entry.startedAt) {
          const retry = {
            ...entry,
            deadline: Date.now() + ctx.config.jobTimeoutCheckMs,
          };
          this.active.set(entry.jobId, retry);
          this.deadlines.push(retry);
        }
      }
    }
    this.armNext();
  }
}
