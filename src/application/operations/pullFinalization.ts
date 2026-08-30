import type { Job, JobId } from '../../domain/types/job';
import type { EventType, JobEvent } from '../../domain/types/queue';
import { processingShardIndex } from '../../shared/hash';
import { throughputTracker } from '../throughputTracker';
import type { PullContext } from './pullStateTransition';

interface PulledEvent extends JobEvent {
  readonly jobId: JobId;
}

function createPulledEvent(job: Job, queue: string, ctx: PullContext): PulledEvent | null {
  if (!ctx.processingShards[processingShardIndex(job.id)].has(job.id)) return null;
  return {
    eventType: 'pulled' as EventType,
    queue,
    jobId: job.id,
    timestamp: Date.now(),
  };
}

function persistActive(job: Job, fallbackStartedAt: number, ctx: PullContext): void {
  try {
    ctx.storage?.markActive(job.id, job.startedAt ?? fallbackStartedAt, job.timeline);
  } catch {
    // Non-fatal: the in-memory processing map remains authoritative.
  }
}

function persistActiveBatch(
  jobs: readonly Job[],
  events: readonly PulledEvent[],
  ctx: PullContext
): void {
  const storage = ctx.storage;
  if (!storage || jobs.length === 0) return;
  try {
    storage.markActiveBatch(
      jobs.map((job, index) => ({
        jobId: job.id,
        startedAt: job.startedAt ?? events[index].timestamp,
        timeline: job.timeline,
      }))
    );
  } catch {
    for (let index = 0; index < jobs.length; index++) {
      persistActive(jobs[index], events[index].timestamp, ctx);
    }
  }
}

/** Persist and publish a single processing handoff. */
export function finalizeProcessing(job: Job, queue: string, ctx: PullContext): boolean {
  const event = createPulledEvent(job, queue, ctx);
  if (!event) return false;
  persistActive(job, event.timestamp, ctx);
  ctx.totalPulled.value++;
  throughputTracker.pullRate.increment();
  ctx.broadcast(event);
  return true;
}

/** Persist and publish all processing handoffs with one durable state transaction. */
export function finalizeProcessingBatch(jobs: Job[], queue: string, ctx: PullContext): Job[] {
  const delivered: Job[] = [];
  const events: PulledEvent[] = [];
  for (const job of jobs) {
    const event = createPulledEvent(job, queue, ctx);
    if (!event) continue;
    delivered.push(job);
    events.push(event);
  }
  persistActiveBatch(delivered, events, ctx);
  ctx.totalPulled.value += BigInt(delivered.length);
  throughputTracker.pullRate.increment(delivered.length);
  if (ctx.broadcastBatch) ctx.broadcastBatch(events);
  else for (const event of events) ctx.broadcast(event);
  return delivered;
}
