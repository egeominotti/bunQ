/**
 * Worker: pulls jobs over TCP and runs a processor with bounded concurrency.
 *
 * All queue semantics (retry, backoff, DLQ, stall detection, priorities)
 * live in the bunqueue server — the worker pulls, heartbeats and acks/fails.
 * Lifecycle state and cooperative cancel live in WorkerBase.
 */

import { hostname } from 'node:os';
import { AckBatcher } from './ack-batcher.js';
import { CommandTimeoutError, ConnectionClosedError, UnrecoverableError } from './errors.js';
import { compact } from './frame.js';
import { Job } from './job.js';
import type { PulledJobsResponse } from './responses.js';
import { WorkerBase } from './worker-base.js';
import {
  MAX_STACK_LINES,
  type Processor,
  RECONNECT_BACKOFF_MS,
  sleep,
  type WorkerOptions,
} from './worker-types.js';

export class Worker<T = unknown, R = unknown> extends WorkerBase<T, R> {
  private readonly processor: Processor<T, R>;
  private readonly ackBatcher: AckBatcher | null;

  constructor(queue: string, processor: Processor<T, R>, opts: WorkerOptions = {}) {
    super(queue, opts);
    this.processor = processor;
    const ab = opts.ackBatch;
    this.ackBatcher = ab?.enabled
      ? new AckBatcher(this.connection, ab.maxSize ?? 50, ab.maxDelayMs ?? 5)
      : null;
    if (opts.autorun !== false) this.run();
  }

  /** Flush batched ACKs before the base class drains in-flight jobs. */
  protected override async beforeClose(): Promise<void> {
    if (this.ackBatcher) await this.ackBatcher.flush();
  }

  /** Start the pull loop (no-op if already running). */
  run(): void {
    if (this.running || this.closedFlag) return;
    this.running = true;
    this.loopPromise = this.loop().catch((err: unknown) => {
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
    });
  }

  // -------------------------------------------------------------------- loop

  private async loop(): Promise<void> {
    await this.register();
    this.readyResolve?.();
    this.readyFired = true;
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
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
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

    // The registration is per-connection server state: after a reconnect the
    // server no longer knows this worker (ListWorkers, skipIfNoWorker crons).
    // Detect the new connection generation and re-register before pulling.
    if (this.connection.isConnected && this.connection.generation !== this.registeredGeneration) {
      await this.register();
    }

    const response = await this.connection.call<PulledJobsResponse>(
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

    const jobs = response.jobs ?? [];
    const tokens = response.tokens ?? [];

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
      if (this.ackBatcher) {
        // Defer the ACK into a batch; the job stays active (lock renewed) until
        // the ACKB settles. onSettled frees the slot FIRST — a throwing
        // listener (e.g. an unhandled 'error' emit) must never leak the slot
        // and permanently shrink the worker's effective concurrency.
        this.ackBatcher.add({
          id: job.id,
          token,
          result: result ?? undefined,
          onSettled: (err) => {
            this.finishJob(job.id);
            if (err) {
              this.emit('error', err instanceof Error ? err : new Error(String(err)));
            } else {
              this.processed += 1;
              this.emit('completed', job, result);
            }
          },
        });
        return;
      }
      const acked = await this.safeCall(
        compact({ cmd: 'ACK', id: job.id, token, result: result ?? undefined }) as { cmd: string }
      );
      // Free the slot BEFORE emitting: a throwing 'completed' listener must
      // not leak the active slot (same rationale as the batched path).
      this.finishJob(job.id);
      // Mirror the batched path: a failed ACK already emitted 'error' — do not
      // also claim completion (no 'completed', no processed++).
      if (acked) {
        this.processed += 1;
        this.emit('completed', job, result);
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      // Keep the FIRST lines: in a JS stack the message + throw site lead, so
      // slice(0,N) preserves them (slice(-N) would drop them on long stacks).
      const stack = (error.stack ?? error.message).split('\n').slice(0, MAX_STACK_LINES);
      const failed = await this.safeCall(
        compact({
          cmd: 'FAIL',
          id: job.id,
          token,
          error: error.message || error.name,
          stack,
          unrecoverable: err instanceof UnrecoverableError ? true : undefined,
        }) as { cmd: string }
      );
      this.finishJob(job.id);
      // Same asymmetry guard as the ACK path: if the FAIL never reached the
      // server, only 'error' fires (the lock expiry will retry the job).
      if (failed) {
        this.failedCount += 1;
        this.emit('failed', job, error);
      }
    }
  }

  private finishJob(id: string): void {
    this.active.delete(id);
    this.cancelledJobs.delete(id);
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
    this.registeredGeneration = this.connection.generation;
  }
}
