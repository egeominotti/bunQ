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
        void this.handleTimeout(worker, job);
      }, this.options.timeout);
    }

    this.emit('active', this.createEventJob(job));
    const jobData = job.data as Record<string, unknown> | null;
    const parentId = jobData?.__flowParentId as string | undefined;
    const request: IPCRequest = {
      type: 'job',
      job: {
        id: String(job.id),
        name: job.name,
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
        void this.complete(worker, message.result);
        break;
      case 'error':
        void this.fail(worker, message.error ?? 'Unknown error');
        break;
      case 'fail':
        void this.fail(worker, message.error ?? 'Job explicitly failed');
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

  protected async complete(worker: WorkerProcess, result: unknown): Promise<void> {
    this.lastActivityTime = Date.now();
    const job = worker.currentJob;
    if (!job) return;
    const token = worker.currentToken ?? undefined;
    // Claim this result message locally before awaiting the broker so duplicate
    // thread messages cannot send a second outcome for the same generation.
    worker.currentJob = null;
    if (worker.timeoutId) {
      clearTimeout(worker.timeoutId);
      worker.timeoutId = null;
    }
    try {
      const applied = await this.ops.ack(job.id, result, token);
      if (applied) {
        const eventJob = this.createEventJob(job);
        (eventJob as { returnvalue?: unknown }).returnvalue = result;
        this.emit('completed', eventJob, result);
      }
    } catch (error) {
      const ackError = error instanceof Error ? error : new Error(String(error));
      log('error', 'Failed to ack job', {
        jobId: String(job.id),
        error: ackError.message,
      });
      this.safeEmitError(Object.assign(ackError, { context: 'ack', jobId: String(job.id) }));
    } finally {
      this.resetWorkerState(worker);
    }
  }

  protected async fail(worker: WorkerProcess, error: string): Promise<void> {
    const job = worker.currentJob;
    if (!job) return;
    const token = worker.currentToken ?? undefined;
    worker.currentJob = null;
    if (worker.timeoutId) {
      clearTimeout(worker.timeoutId);
      worker.timeoutId = null;
    }
    try {
      const applied = await this.ops.fail(job.id, error, token);
      if (applied) {
        const eventJob = this.createEventJob(job);
        (eventJob as { failedReason?: string }).failedReason = error;
        this.emit('failed', eventJob, new Error(error));
      }
    } catch (failure) {
      const failError = failure instanceof Error ? failure : new Error(String(failure));
      log('error', 'Failed to mark job as failed', {
        jobId: String(job.id),
        error: failError.message,
      });
      this.safeEmitError(Object.assign(failError, { context: 'fail', jobId: String(job.id) }));
    } finally {
      this.resetWorkerState(worker);
    }
  }

  protected async handleTimeout(worker: WorkerProcess, job: DomainJob): Promise<void> {
    if (worker.currentJob !== job) return;
    const token = worker.currentToken ?? undefined;
    worker.currentJob = null;
    if (worker.timeoutId) {
      clearTimeout(worker.timeoutId);
      worker.timeoutId = null;
    }
    worker.worker.terminate();
    const errorMessage = `Job timed out after ${this.options.timeout}ms`;
    let applied = false;
    try {
      applied = await this.ops.fail(job.id, errorMessage, token);
      if (applied) {
        const eventJob = this.createEventJob(job);
        (eventJob as { failedReason?: string }).failedReason = errorMessage;
        this.emit('failed', eventJob, new Error(errorMessage));
      }
    } catch (error) {
      const failError = error instanceof Error ? error : new Error(String(error));
      log('error', 'Failed to mark timed-out job as failed', {
        jobId: String(job.id),
        error: failError.message,
      });
      this.safeEmitError(
        Object.assign(failError, { context: 'timeout-fail', jobId: String(job.id) })
      );
    } finally {
      this.resetWorkerState(worker);
      const index = this.workers.indexOf(worker);
      if (index !== -1) this.handleCrash(worker, index, applied);
    }
  }

  protected abstract safeEmitError(error: Error): void;
  protected abstract createEventJob(domainJob: DomainJob): Job;
}
