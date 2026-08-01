import { releaseSharedPool } from '../../tcpPool';
import type { WorkerProcess } from '../types';
import { cleanupWrapperScript, createWrapperScript } from '../wrapper';
import { SandboxedState } from './state';

export abstract class SandboxedLifecycle<T = unknown> extends SandboxedState<T> {
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.lastActivityTime = Date.now();

    if (this.tcp) await this.tcp.connect();
    this.wrapperPath = await createWrapperScript(this.queueName, this.options.processor);
    await this.spawnWorker(0);
    if (this.options.concurrency > 1) {
      const spawnPromises: Promise<void>[] = [];
      for (let index = 1; index < this.options.concurrency; index++) {
        spawnPromises.push(this.spawnWorker(index));
      }
      await Promise.all(spawnPromises);
    }

    this.startHeartbeat();
    this.emit('ready');
    this.pullPromise = this.pullLoop();
  }

  async stop(force = false): Promise<void> {
    this.running = false;
    if (this.autoStartTimer) {
      clearInterval(this.autoStartTimer);
      this.autoStartTimer = null;
    }
    if (this.pullPromise) await this.pullPromise;
    if (!force) {
      while (this.workers.some((worker) => worker.busy && !worker.terminated)) {
        await Bun.sleep(50);
      }
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    for (const worker of this.workers) {
      if (worker.timeoutId) clearTimeout(worker.timeoutId);
      if (!worker.terminated) worker.worker.terminate();
    }
    this.workers.length = 0;

    await cleanupWrapperScript(this.wrapperPath);
    if (this.tcp) releaseSharedPool(this.tcp);
    this.emit('closed');
  }

  protected async stopAndWatch(): Promise<void> {
    await this.stop();
    this.autoStartTimer = setInterval(() => {
      void this.checkAndRestart();
    }, this.autoStartPollMs);
  }

  protected async checkAndRestart(): Promise<void> {
    try {
      const count = await this.ops.countWaiting(this.queueName);
      if (count > 0) {
        if (this.autoStartTimer) {
          clearInterval(this.autoStartTimer);
          this.autoStartTimer = null;
        }
        await this.start();
      }
    } catch {
      // Ignore poll errors during watch.
    }
  }

  isRunning(): boolean {
    return this.running;
  }

  getStats(): { total: number; busy: number; idle: number; recycled: number; restarts: number } {
    const busy = this.workers.filter((worker) => worker.busy && !worker.terminated).length;
    const recycled = this.workers.filter((worker) => worker.terminated).length;
    const alive = this.workers.length - recycled;
    const restarts = this.workers.reduce((sum, worker) => sum + worker.restarts, 0);
    return { total: this.workers.length, busy, idle: alive - busy, recycled, restarts };
  }

  protected resetWorkerState(worker: WorkerProcess): void {
    if (worker.timeoutId) {
      clearTimeout(worker.timeoutId);
      worker.timeoutId = null;
    }
    worker.busy = false;
    worker.currentJob = null;
    worker.currentToken = null;
    worker.lastIdleAt = Date.now();
  }

  protected abstract spawnWorker(index: number): Promise<void>;
  protected abstract pullLoop(): Promise<void>;
  protected abstract startHeartbeat(): void;
}
