import { shardIndexFor } from './hash';
import { QueueSim } from './queue';
import { buildSnapshot } from './snapshot';
import type { PushOptions, SimEvent, SimEventType, SimJob, Snapshot, WorkerSim } from './types';
import { fmtMs } from './util';
import { progressJobs, pruneStopped, pullJobs } from './workers';

const TICK_MS = 60;
const MAX_FRAME_MS = 250; // cap catch-up after background-tab throttling
const DEFAULT_MAX_ATTEMPTS = 3;
const EVENT_KEEP = 80;

// The simulation engine: a controllable clock (pause + speed) driving
// push → waiting/delayed → active → completed | retry | DLQ, with
// workers, per-queue rate limits and bunqueue's shard mapping.
export class SimulatorEngine {
  simTime = 0;
  speed = 1;
  running = true;
  failureRate = 0.15;

  readonly queues = new Map<string, QueueSim>();
  readonly workers: WorkerSim[] = [];
  events: SimEvent[] = []; // newest first
  completionLog: number[] = []; // sim-time stamps of completions

  private seq = 1;
  private eventSeq = 1;
  private workerSeq = 1;
  private timer: ReturnType<typeof setInterval> | null = null;
  private raf = 0;
  private lastReal = 0;
  seeded = false;

  // Drive the clock with requestAnimationFrame (smooth and accurate while
  // the tab is visible) plus a low-rate interval fallback so time still
  // creeps forward when the browser throttles a hidden tab. tick() derives
  // dt from a shared lastReal timestamp, so overlapping sources are safe.
  start(): void {
    if (this.timer) return;
    this.lastReal = performance.now();
    this.timer = setInterval(() => this.tick(), TICK_MS);
    const loop = () => {
      this.tick();
      this.raf = requestAnimationFrame(loop);
    };
    this.raf = requestAnimationFrame(loop);
  }

  destroy(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    cancelAnimationFrame(this.raf);
  }

  // ── Producers ────────────────────────────────────────────────────────

  push(queueName: string, name: string, opts: PushOptions = {}, silent = false): SimJob {
    const q = this.queueFor(queueName);
    const delay = Math.max(0, opts.delay ?? 0);
    const job: SimJob = {
      id: this.seq++,
      queue: q.name,
      name,
      priority: opts.priority ?? 0,
      state: 'waiting',
      attemptsMade: 0,
      maxAttempts: opts.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
      createdAt: this.simTime,
      runAt: this.simTime + delay,
      startedAt: null,
      finishedAt: null,
      duration: 0,
      progress: 0,
      workerId: null,
      failedReason: null,
      lastBackoff: 0,
    };
    q.enqueue(job, this.simTime);
    if (!silent) {
      const extra = delay > 0 ? ` in ${fmtMs(delay)}` : '';
      this.emit('push', `push #${job.id} ${name} → ${q.name} (P${job.priority})${extra}`);
    }
    return job;
  }

  pushBulk(
    queueName: string,
    count: number,
    make: (i: number) => { name: string } & PushOptions,
  ): void {
    for (let i = 0; i < count; i++) {
      const { name, ...opts } = make(i);
      this.push(queueName, name, opts, true);
    }
    this.emit('push', `push ${count} jobs → ${queueName}`);
  }

  // ── Workers ──────────────────────────────────────────────────────────

  createWorker(queueName: string, concurrency: number): WorkerSim {
    this.queueFor(queueName);
    const worker: WorkerSim = {
      id: `w${this.workerSeq++}`,
      queue: queueName,
      concurrency: Math.max(1, concurrency),
      active: [],
      processed: 0,
      failed: 0,
      status: 'running',
    };
    this.workers.push(worker);
    this.emit('worker', `worker ${worker.id} started on ${queueName} (c=${worker.concurrency})`);
    return worker;
  }

  stopWorker(id: string): void {
    const worker = this.workers.find((w) => w.id === id);
    if (!worker || worker.status !== 'running') return;
    worker.status = worker.active.length > 0 ? 'stopping' : 'stopped';
    this.emit('worker', `worker ${id} ${worker.status === 'stopping' ? 'draining' : 'stopped'}`);
    pruneStopped(this);
  }

  // ── Queue controls ───────────────────────────────────────────────────

  pause(queueName: string): void {
    const q = this.queues.get(queueName);
    if (!q || q.paused) return;
    q.paused = true;
    this.emit('pause', `pause ${queueName}`);
  }

  resume(queueName: string): void {
    const q = this.queues.get(queueName);
    if (!q || !q.paused) return;
    q.paused = false;
    this.emit('resume', `resume ${queueName}`);
  }

  drain(queueName: string): void {
    const q = this.queues.get(queueName);
    if (!q) return;
    const removed = q.drain();
    this.emit('drain', `drain ${queueName} — removed ${removed} jobs`);
  }

  retryDlq(queueName: string): void {
    const q = this.queues.get(queueName);
    if (!q) return;
    const retried = q.retryDlq(this.simTime);
    if (retried > 0) this.emit('retry-dlq', `retry ${retried} dead letters → ${queueName}`);
  }

  setRateLimit(queueName: string, jobsPerSec: number): void {
    const q = this.queueFor(queueName);
    if (q.rateLimit === jobsPerSec) return;
    q.setRateLimit(jobsPerSec);
    this.emit(
      'rate-limit',
      jobsPerSec > 0 ? `rate limit ${queueName} ≤ ${jobsPerSec}/s` : `rate limit off — ${queueName}`,
    );
  }

  setFailureRate(rate: number): void {
    this.failureRate = Math.min(1, Math.max(0, rate));
  }

  setSpeed(speed: number): void {
    this.speed = speed;
  }

  setRunning(running: boolean): void {
    this.running = running;
  }

  reset(): void {
    this.queues.clear();
    this.workers.length = 0;
    this.events = [];
    this.completionLog = [];
    this.simTime = 0;
    this.failureRate = 0.15;
    this.running = true;
    this.seq = 1;
    this.workerSeq = 1;
  }

  emit(type: SimEventType, msg: string): void {
    this.events.unshift({ id: this.eventSeq++, t: this.simTime, type, msg });
    if (this.events.length > EVENT_KEEP) this.events.length = EVENT_KEEP;
  }

  snapshot(): Snapshot {
    return buildSnapshot(this);
  }

  queueFor(name: string): QueueSim {
    let q = this.queues.get(name);
    if (!q) {
      q = new QueueSim(name, shardIndexFor(name));
      this.queues.set(name, q);
    }
    return q;
  }

  // ── Clock ────────────────────────────────────────────────────────────

  private tick(): void {
    const now = performance.now();
    const dtReal = Math.min(now - this.lastReal, MAX_FRAME_MS);
    this.lastReal = now;
    if (!this.running) return;
    const dt = dtReal * this.speed;
    this.simTime += dt;

    for (const q of this.queues.values()) {
      q.refillTokens(dt);
      q.promoteDue(this.simTime);
    }
    pullJobs(this);
    progressJobs(this);
  }
}
