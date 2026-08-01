import { jobId } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import { WorkerManual } from './manual';

export abstract class WorkerLifecycle<T = unknown, R = unknown> extends WorkerManual<T, R> {
  // biome-ignore lint/suspicious/useAwait: preserves the public async close contract
  async close(force = false): Promise<void> {
    if (this.closed) return;
    if (this._closingPromise) {
      if (force) this._forceClose = true;
      return this._closingPromise;
    }
    this._forceClose = force;
    this._closingPromise = this._doClose(force);
    return this._closingPromise;
  }

  protected async _doClose(force: boolean): Promise<void> {
    this._closing = true;
    this.running = false;
    this.paused = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.workerHeartbeatTimer) {
      clearInterval(this.workerHeartbeatTimer);
      this.workerHeartbeatTimer = null;
    }

    await this.releaseBufferedJobs();
    if (!force) {
      while (this.activeJobs > 0 && !this._forceClose) await Bun.sleep(50);
    }

    await this.ackBatcher.flush();
    await this.ackBatcher.waitForInFlight();
    this.ackBatcher.stop();

    if (this.registered) {
      if (this.embedded) {
        getSharedManager().unregisterWorker(this.workerId);
      } else if (this.tcp) {
        try {
          await this.tcp.send({ cmd: 'UnregisterWorker', workerId: this.workerId });
        } catch {
          // Best-effort unregister — server will cleanup via stale timeout.
        }
      }
      this.registered = false;
    }

    await Bun.sleep(100);
    if (this.stalledUnsubscribe) {
      this.stalledUnsubscribe();
      this.stalledUnsubscribe = null;
    }

    this.activeJobIds.clear();
    this.pulledJobIds.clear();
    this.jobTokens.clear();
    this.cancelledJobs.clear();
    this.pendingJobs = [];
    this.pendingJobsHead = 0;
    if (this.groupLimiter) this.groupLimiter.clear();

    if (this.tcpPool) this.tcpPool.close();
    this.closed = true;
    this._closing = false;
    this.emit('closed');
  }

  protected async releaseBufferedJobs(): Promise<void> {
    const buffered = this.pendingJobs.slice(this.pendingJobsHead);
    this.pendingJobs = [];
    this.pendingJobsHead = 0;
    if (buffered.length === 0) return;

    for (const { job, token } of buffered) {
      const id = String(job.id);
      try {
        if (this.embedded) {
          const manager = getSharedManager();
          await manager.moveActiveToWait(jobId(id));
          if (this.opts.useLocks) manager.releaseLock(jobId(id), token ?? undefined);
        } else if (this.tcp) {
          await this.tcp.send({ cmd: 'MoveToWait', id });
        }
      } catch {
        // Lock expiration recovers anything that could not be released.
      } finally {
        this.pulledJobIds.delete(id);
        this.jobTokens.delete(id);
      }
    }
  }
}
