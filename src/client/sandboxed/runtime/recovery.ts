import { jobId, type Job as DomainJob } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import { createPublicJob } from '../../jobConversion';
import { buildFailCommand, failEmbeddedArgs } from '../../queue/failWire';
import type { Job } from '../../types';
import {
  createChangeDelayHandler,
  createChangePriorityHandler,
  createClearLogsHandler,
  createDiscardHandler,
  createExtendLockHandler,
  createGetChildrenValuesHandler,
  createGetDependenciesCountHandler,
  createGetDependenciesHandler,
  createGetFailedChildrenValuesHandler,
  createGetIgnoredChildrenFailuresHandler,
  createGetStateHandler,
  createLogHandler,
  createMoveToDelayedHandler,
  createMoveToWaitHandler,
  createMoveToWaitingChildrenHandler,
  createProgressHandler,
  createPromoteHandler,
  createRemoveChildDependencyHandler,
  createRemoveDeduplicationKeyHandler,
  createRemoveHandler,
  createRemoveUnprocessedChildrenHandler,
  createRetryHandler,
  createUpdateDataHandler,
  createWaitUntilFinishedHandler,
} from '../../worker/processorHandlers';
import type { WorkerProcess } from '../types';
import { SandboxedDispatch } from './dispatch';
import { log } from './log';

export class SandboxedRecovery<T = unknown> extends SandboxedDispatch<T> {
  protected handleCrash(worker: WorkerProcess, index: number): void {
    if (worker.currentJob) {
      const token = worker.currentToken ?? undefined;
      this.ops.fail(worker.currentJob.id, 'Worker crashed', token).catch((error: unknown) => {
        log('error', 'Failed to mark crashed job as failed', {
          jobId: String(worker.currentJob?.id),
          error: error instanceof Error ? error.message : String(error),
        });
      });
    }

    this.safeEmitError(
      Object.assign(new Error('Worker crashed'), {
        workerIndex: index,
        context: 'crash' as const,
      })
    );
    this.resetWorkerState(worker);
    worker.restarts++;

    if (this.options.autoRestart && worker.restarts < this.options.maxRestarts && this.running) {
      this.spawnWorker(index).catch((error: unknown) => {
        log('error', 'Failed to restart worker', {
          workerIndex: index,
          error: error instanceof Error ? error.message : String(error),
        });
      });
    } else if (worker.restarts >= this.options.maxRestarts) {
      log('error', 'Worker exceeded max restarts', {
        workerIndex: index,
        maxRestarts: this.options.maxRestarts,
      });
    }
  }

  protected startHeartbeat(): void {
    if (this.heartbeatInterval <= 0) return;
    this.heartbeatTimer = setInterval(() => void this.sendHeartbeat(), this.heartbeatInterval);
  }

  protected async sendHeartbeat(): Promise<void> {
    const active = this.workers.filter(
      (worker) => worker.busy && worker.currentJob && !worker.terminated
    );
    if (active.length === 0) return;
    try {
      const ids = active.map((worker) => String(worker.currentJob?.id));
      const tokens = active.map((worker) => worker.currentToken ?? '');
      await this.ops.sendHeartbeat(ids, tokens);
    } catch (error) {
      this.safeEmitError(
        Object.assign(error instanceof Error ? error : new Error(String(error)), {
          context: 'heartbeat' as const,
        })
      );
    }
  }

  protected safeEmitError(error: Error): void {
    if (this.listenerCount('error') > 0) this.emit('error', error);
  }

  protected createEventJob(domainJob: DomainJob): Job {
    const data = domainJob.data as { name?: string } | null;
    const embedded = !this.tcp;
    const tcp = this.tcp;
    const moveToCompleted = async (
      id: string,
      returnValue: unknown,
      _token?: string
    ): Promise<unknown> => {
      if (embedded) await getSharedManager().ack(jobId(id), returnValue);
      else if (tcp) await tcp.send({ cmd: 'ACK', id, result: returnValue });
      return null;
    };
    const moveToFailed = async (id: string, error: Error, token?: string): Promise<void> => {
      if (embedded) await getSharedManager().fail(jobId(id), ...failEmbeddedArgs(error, token));
      else if (tcp) await tcp.send(buildFailCommand(id, error, token));
    };
    return createPublicJob({
      job: domainJob,
      name: data?.name ?? 'default',
      updateProgress: createProgressHandler(embedded, tcp, this, { current: null }),
      log: createLogHandler(embedded, tcp, this, { current: null }),
      getState: createGetStateHandler(embedded, tcp),
      getChildrenValues: createGetChildrenValuesHandler(embedded, tcp),
      getFailedChildrenValues: createGetFailedChildrenValuesHandler(embedded, tcp),
      getIgnoredChildrenFailures: createGetIgnoredChildrenFailuresHandler(embedded, tcp),
      removeChildDependency: createRemoveChildDependencyHandler(embedded, tcp),
      removeUnprocessedChildren: createRemoveUnprocessedChildrenHandler(embedded, tcp),
      remove: createRemoveHandler(embedded, tcp),
      retry: createRetryHandler(embedded, tcp, domainJob),
      updateData: createUpdateDataHandler(embedded, tcp),
      promote: createPromoteHandler(embedded, tcp),
      changeDelay: createChangeDelayHandler(embedded, tcp),
      changePriority: createChangePriorityHandler(embedded, tcp),
      extendLock: createExtendLockHandler(embedded, tcp),
      clearLogs: createClearLogsHandler(embedded, tcp),
      moveToWait: createMoveToWaitHandler(embedded, tcp),
      moveToDelayed: createMoveToDelayedHandler(embedded, tcp),
      moveToWaitingChildren: createMoveToWaitingChildrenHandler(embedded, tcp),
      waitUntilFinished: createWaitUntilFinishedHandler(embedded, tcp),
      discard: createDiscardHandler(embedded, tcp),
      getDependencies: createGetDependenciesHandler(embedded, tcp, domainJob),
      getDependenciesCount: createGetDependenciesCountHandler(embedded, tcp, domainJob),
      removeDeduplicationKey: createRemoveDeduplicationKeyHandler(embedded, tcp),
      moveToCompleted,
      moveToFailed,
    });
  }
}
