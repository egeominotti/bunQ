import { jobId } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import { WorkerManual } from './manual';
import type { WorkerDelivery } from './state';

export abstract class WorkerLifecycle<T = unknown, R = unknown> extends WorkerManual<T, R> {
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
    this.clearPollTimer();
    this.ackBatcher.notifyCapacityChanged();
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

    this.clearDeliveries();
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

    for (const delivery of buffered) {
      if (this.isCurrentDelivery(delivery)) await this.releasePulledJob(delivery);
    }

    for (const id of [...this.pulledJobIds]) {
      if (this.hasActiveDelivery(id)) continue;
      const delivery = this.currentDelivery(id);
      if (delivery) await this.releasePulledJob(delivery);
    }
  }

  private async releasePulledJob(delivery: WorkerDelivery): Promise<void> {
    const id = String(delivery.job.id);
    const { token } = delivery;
    try {
      if (this.embedded) {
        const manager = getSharedManager();
        await manager.moveActiveToWait(jobId(id), token ?? undefined);
      } else if (this.tcp) {
        await this.tcp.send({
          cmd: 'MoveToWait',
          id,
          ...(token ? { token } : {}),
        });
      }
    } catch {
      // Lock expiration recovers anything that could not be released.
    } finally {
      this.forgetDelivery(delivery);
    }
  }
}
