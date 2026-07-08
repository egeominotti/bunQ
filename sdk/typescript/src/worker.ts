/**
 * Worker: pulls jobs over TCP and runs a processor with bounded concurrency.
 *
 * All queue semantics (retry, backoff, DLQ, stall detection, priorities)
 * live in the bunqueue server — the worker pulls, heartbeats and acks/fails.
 * Lifecycle state and cooperative cancel live in WorkerBase.
 */

import { hostname } from 'node:os';
import { CommandTimeoutError, ConnectionClosedError, UnrecoverableError } from './errors.js';
import { compact } from './frame.js';
import { Job } from './job.js';
import { WorkerBase } from './worker-base.js';
import {
  MAX_STACK_LINES,
  type Processor,
  RECONNECT_BACKOFF_MS,
  sleep,
  type WorkerOptions,
} from './worker-types.js';

export class Worker<T = unknown, R = unknown> extends WorkerBase {
  private readonly processor: Processor<T, R>;

  constructor(queue: string, processor: Processor<T, R>, opts: WorkerOptions = {}) {
    super(queue, opts);
    this.processor = processor;
    if (opts.autorun !== false) this.run();
  }

  /** Start the pull loop (no-op if already running). */
  run(): void {
    if (this.running || this.closedFlag) return;
    this.running = true;
    this.loopPromise = this.loop().catch((err) => {
      this.emit('error', err);
    });
  }

  // -------------------------------------------------------------------- loop

  private async loop(): Promise<void> {
    await this.register();
    this.readyResolve?.();
    this.emit('ready');
    this.startHeartbeat();

    let backoffIdx = 0;
    while (!this.stopped) {
      if (this.paused) {
        await sleep(50);
        continue;
      }
      try {
        await this.pollOnce();
        backoffIdx = 0;
      } catch (err) {
        this.emit('error', err);
        if (err instanceof ConnectionClosedError || err instanceof CommandTimeoutError) {
          const delay = RECONNECT_BACKOFF_MS[Math.min(backoffIdx, RECONNECT_BACKOFF_MS.length - 1)];
          backoffIdx += 1;
          await sleep(delay);
        } else {
          await sleep(200);
        }
      }
    }
  }

  private async pollOnce(): Promise<void> {
    const free = this.concurrency - this.active.size;
    if (free <= 0) {
      await sleep(20);
      return;
    }

    const response = await this.connection.call(
      {
        cmd: 'PULLB',
        queue: this.queue,
        count: Math.min(free, this.batchSize),
        timeout: this.pollTimeoutMs,
        owner: this.workerId,
        lockTtl: this.lockTtlMs,
      },
      this.pollTimeoutMs + 10_000
    );

    const jobs = (response.jobs ?? []) as Record<string, unknown>[];
    const tokens = (response.tokens ?? []) as string[];

    if (jobs.length === 0) {
      if (this.wasBusy && this.active.size === 0) {
        this.wasBusy = false;
        this.emit('drained');
      }
      return;
    }

    this.wasBusy = true;
    for (let i = 0; i < jobs.length; i++) {
      const jobId = String(jobs[i].id);
      this.active.set(jobId, tokens[i]);
      // fire-and-forget: bounded by the free-slot accounting above
      void this.runJob(jobs[i], tokens[i]);
    }
  }

  private async runJob(raw: Record<string, unknown>, token: string): Promise<void> {
    const job = new Job<T>(raw, this.connection, token, (j, p) => this.emit('progress', j, p));
    this.emit('active', job);
    try {
      const result = await this.processor(job);
      this.processed += 1;
      await this.safeCall(
        compact({ cmd: 'ACK', id: job.id, token, result: result ?? undefined }) as { cmd: string }
      );
      this.emit('completed', job, result);
    } catch (err) {
      this.failedCount += 1;
      const error = err instanceof Error ? err : new Error(String(err));
      const stack = (error.stack ?? error.message).split('\n').slice(-MAX_STACK_LINES);
      await this.safeCall(
        compact({
          cmd: 'FAIL',
          id: job.id,
          token,
          error: error.message || error.name,
          stack,
          unrecoverable: err instanceof UnrecoverableError ? true : undefined,
        }) as { cmd: string }
      );
      this.emit('failed', job, error);
    } finally {
      this.active.delete(job.id);
      this.cancelledJobs.delete(job.id);
    }
  }

  // --------------------------------------------------------------- heartbeat

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      void (async () => {
        await this.safeCall({
          cmd: 'Heartbeat',
          id: this.workerId,
          activeJobs: this.active.size,
          processed: this.processed,
          failed: this.failedCount,
        });
        if (this.active.size > 0) {
          const ids = [...this.active.keys()];
          const tokens = ids.map((id) => this.active.get(id) as string);
          await this.safeCall({ cmd: 'JobHeartbeatB', ids, tokens });
        }
      })();
    }, this.heartbeatIntervalS * 1000);
    this.heartbeatTimer.unref?.(); // don't keep the process alive for heartbeats
  }

  private async register(): Promise<void> {
    await this.safeCall({
      cmd: 'RegisterWorker',
      name: this.name,
      queues: [this.queue],
      concurrency: this.concurrency,
      workerId: this.workerId,
      hostname: hostname(),
      pid: process.pid,
      startedAt: Date.now(),
    });
  }
}
