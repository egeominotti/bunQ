import type { SimulatorEngine } from './engine';
import type { SimJob, WorkerSim } from './types';
import { FAILURE_REASONS, fmtMs } from './util';

const BACKOFF_BASE_MS = 1200;
const BACKOFF_CAP_MS = 8000;
const DEFAULT_DURATION: [number, number] = [450, 1500];

// The worker side of each tick: fill free concurrency slots, advance
// in-flight jobs, and settle finished ones into completed / retry / DLQ.

export function pullJobs(engine: SimulatorEngine): void {
  for (const worker of engine.workers) {
    if (worker.status !== 'running') continue;
    const q = engine.queues.get(worker.queue);
    if (!q) continue;
    while (worker.active.length < worker.concurrency) {
      const job = q.take();
      if (!job) break;
      const [lo, hi] = q.durationRange ?? DEFAULT_DURATION;
      job.state = 'active';
      job.workerId = worker.id;
      job.startedAt = engine.simTime;
      job.progress = 0;
      job.duration = lo + Math.random() * (hi - lo);
      q.active.set(job.id, job);
      worker.active.push(job);
    }
  }
}

export function progressJobs(engine: SimulatorEngine): void {
  for (const worker of engine.workers) {
    if (worker.active.length === 0) {
      if (worker.status === 'stopping') {
        worker.status = 'stopped';
        pruneStopped(engine);
      }
      continue;
    }
    for (const job of [...worker.active]) {
      job.progress = Math.min(1, (engine.simTime - (job.startedAt ?? 0)) / job.duration);
      if (job.progress < 1) continue;
      worker.active.splice(worker.active.indexOf(job), 1);
      engine.queues.get(job.queue)?.active.delete(job.id);
      settle(engine, job, worker);
    }
  }
}

// Keep at most the two most recent stopped workers around (their final
// counters are informative for a beat) so repeated start/stop cycles
// don't accumulate dead cards.
export function pruneStopped(engine: SimulatorEngine): void {
  let stopped = engine.workers.filter((w) => w.status === 'stopped').length;
  for (let i = 0; i < engine.workers.length && stopped > 2; ) {
    if (engine.workers[i].status === 'stopped') {
      engine.workers.splice(i, 1);
      stopped--;
    } else {
      i++;
    }
  }
}

function settle(engine: SimulatorEngine, job: SimJob, worker: WorkerSim): void {
  const q = engine.queueFor(job.queue);
  if (Math.random() < engine.failureRate) {
    job.attemptsMade++;
    q.counters.failed++;
    worker.failed++;
    if (job.attemptsMade >= job.maxAttempts) {
      job.state = 'dlq';
      job.finishedAt = engine.simTime;
      job.failedReason = FAILURE_REASONS[job.id % FAILURE_REASONS.length];
      q.recordDead(job);
      engine.emit('dlq', `dead #${job.id} ${job.name} — ${job.failedReason}`);
    } else {
      const backoff = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** (job.attemptsMade - 1));
      job.state = 'delayed';
      job.lastBackoff = backoff;
      job.runAt = engine.simTime + backoff;
      q.delayed.push(job);
      q.counters.retried++;
      engine.emit(
        'retry',
        `retry #${job.id} in ${fmtMs(backoff)} (attempt ${job.attemptsMade + 1}/${job.maxAttempts})`,
      );
    }
  } else {
    job.state = 'completed';
    job.finishedAt = engine.simTime;
    worker.processed++;
    q.recordCompleted(job);
    engine.completionLog.push(engine.simTime);
    if (engine.completionLog.length > 4000) {
      engine.completionLog = engine.completionLog.slice(-2000);
    }
    engine.emit('done', `done #${job.id} ${job.name} in ${fmtMs(engine.simTime - (job.startedAt ?? 0))}`);
  }
}
