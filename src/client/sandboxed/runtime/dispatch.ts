import type { Job as DomainJob } from '../../../domain/types/job';
import type { Job } from '../../types';
import type { IPCRequest, IPCResponse, WorkerProcess } from '../types';
import { log } from './log';
import { SandboxedPool } from './pool';

export abstract class SandboxedDispatch<T = unknown> extends SandboxedPool<T> {
  protected dispatch(worker: WorkerProcess, job: DomainJob, token: string | null): void {
    this.lastActivityTime = Date.now();
    worker.busy = true;
    worker.currentJob = job;
    worker.currentToken = token;

    if (this.options.timeout > 0) {
      worker.timeoutId = setTimeout(() => {
        this.handleTimeout(worker, job);
      }, this.options.timeout);
    }

    this.emit('active', this.createEventJob(job));
    const jobData = job.data as Record<string, unknown> | null;
    const parentId = jobData?.__flowParentId as string | undefined;
    const request: IPCRequest = {
      type: 'job',
      job: {
        id: String(job.id),
        data: job.data,
        queue: job.queue,
        attempts: job.attempts,
        ...(parentId ? { parentId } : {}),
      },
    };
    try {
      worker.worker.postMessage(request);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      log('error', 'Failed to dispatch job', {
        jobId: String(job.id),
        error: errorMessage,
      });
      this.safeEmitError(
        Object.assign(new Error(`Dispatch failed: ${errorMessage}`), {
          jobId: String(job.id),
          context: 'dispatch' as const,
        })
      );
      this.resetWorkerState(worker);
      this.ops
        .fail(job.id, 'Dispatch failed: worker terminated', token ?? undefined)
        .catch((failure: unknown) => {
          log('error', 'Failed to mark dispatched job as failed', {
            jobId: String(job.id),
            error: failure instanceof Error ? failure.message : String(failure),
          });
        });
    }
  }

  protected handleMessage(worker: WorkerProcess, message: IPCResponse): void {
    if (message.type === 'ready') return;
    if (!worker.currentJob || message.jobId !== String(worker.currentJob.id)) return;

    switch (message.type) {
      case 'result':
        this.complete(worker, message.result);
        break;
      case 'error':
        this.fail(worker, message.error ?? 'Unknown error');
        break;
      case 'fail':
        this.fail(worker, message.error ?? 'Job explicitly failed');
        break;
      case 'progress':
        if (message.progress !== undefined) {
          this.ops
            .updateProgress(worker.currentJob.id, message.progress)
            .catch((error: unknown) => {
              log('error', 'Failed to update job progress', {
                jobId: String(worker.currentJob?.id),
                error: error instanceof Error ? error.message : String(error),
              });
            });
          this.emit('progress', this.createEventJob(worker.currentJob), message.progress);
        }
        break;
      case 'log':
        if (message.message) {
          this.ops.addLog(worker.currentJob.id, message.message);
          this.emit('log', this.createEventJob(worker.currentJob), message.message);
        }
        break;
    }
  }

  protected complete(worker: WorkerProcess, result: unknown): void {
    this.lastActivityTime = Date.now();
    const job = worker.currentJob;
    if (job) {
      const token = worker.currentToken ?? undefined;
      this.ops.ack(job.id, result, token).catch((error: unknown) => {
        log('error', 'Failed to ack job', {
          jobId: String(job.id),
          error: error instanceof Error ? error.message : String(error),
        });
      });
      const eventJob = this.createEventJob(job);
      (eventJob as { returnvalue?: unknown }).returnvalue = result;
      this.emit('completed', eventJob, result);
    }
    this.resetWorkerState(worker);
  }

  protected fail(worker: WorkerProcess, error: string): void {
    const job = worker.currentJob;
    if (job) {
      const token = worker.currentToken ?? undefined;
      this.ops.fail(job.id, error, token).catch((failure: unknown) => {
        log('error', 'Failed to mark job as failed', {
          jobId: String(job.id),
          error: failure instanceof Error ? failure.message : String(failure),
        });
      });
      const eventJob = this.createEventJob(job);
      (eventJob as { failedReason?: string }).failedReason = error;
      this.emit('failed', eventJob, new Error(error));
    }
    this.resetWorkerState(worker);
  }

  protected handleTimeout(worker: WorkerProcess, job: DomainJob): void {
    worker.worker.terminate();
    const errorMessage = `Job timed out after ${this.options.timeout}ms`;
    const token = worker.currentToken ?? undefined;
    this.ops.fail(job.id, errorMessage, token).catch((error: unknown) => {
      log('error', 'Failed to mark timed-out job as failed', {
        jobId: String(job.id),
        error: error instanceof Error ? error.message : String(error),
      });
    });
    const eventJob = this.createEventJob(job);
    (eventJob as { failedReason?: string }).failedReason = errorMessage;
    this.emit('failed', eventJob, new Error(errorMessage));
    this.resetWorkerState(worker);
    const index = this.workers.indexOf(worker);
    if (index !== -1) this.handleCrash(worker, index);
  }

  protected abstract safeEmitError(error: Error): void;
  protected abstract createEventJob(domainJob: DomainJob): Job;
}
