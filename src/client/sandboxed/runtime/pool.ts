import type { Job as DomainJob } from '../../../domain/types/job';
import type { IPCResponse, WorkerProcess } from '../types';
import { SandboxedLifecycle } from './lifecycle';
import { log } from './log';

export abstract class SandboxedPool<T = unknown> extends SandboxedLifecycle<T> {
  protected spawnWorker(index: number): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.wrapperPath) {
        resolve();
        return;
      }

      const worker = new Worker(this.wrapperPath, { smol: this.options.maxMemory <= 64 });
      const workerProcess: WorkerProcess = {
        worker,
        busy: false,
        currentJob: null,
        currentToken: null,
        restarts: this.workers[index]?.restarts ?? 0,
        timeoutId: null,
        lastIdleAt: Date.now(),
        terminated: false,
      };

      let resolved = false;
      const readyTimeout = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          resolve();
        }
      }, 5000);

      worker.onmessage = (event: MessageEvent<IPCResponse>) => {
        if (event.data.type === 'ready' && !resolved) {
          resolved = true;
          clearTimeout(readyTimeout);
          resolve();
          return;
        }
        this.handleMessage(workerProcess, event.data);
      };

      worker.onerror = (error) => {
        log('error', 'Worker error', { workerIndex: index, error: error.message });
        if (!resolved) {
          resolved = true;
          clearTimeout(readyTimeout);
          reject(new Error(error.message));
        }
        this.handleCrash(workerProcess, index);
      };

      if (this.workers[index]) this.workers[index] = workerProcess;
      else this.workers.push(workerProcess);
    });
  }

  protected async pullLoop(): Promise<void> {
    while (this.running) {
      let idle = this.workers.find((worker) => !worker.busy && !worker.terminated);
      if (!idle) {
        const recycled = this.workers.find((worker) => worker.terminated);
        if (recycled) {
          const index = this.workers.indexOf(recycled);
          await this.spawnWorker(index);
          idle = this.workers[index];
        } else {
          await Bun.sleep(this.options.pollInterval);
          continue;
        }
      }

      const { job, token } = await this.ops.pull(this.queueName, this.workerId, 1000);
      if (job) {
        if (idle.terminated) {
          const index = this.workers.indexOf(idle);
          await this.spawnWorker(index);
          idle = this.workers[index];
        }
        this.dispatch(idle, job, token);
      } else {
        this.recycleIdleWorkers();
        if (this.idleTimeout > 0 && Date.now() - this.lastActivityTime >= this.idleTimeout) {
          if (this.autoStart) {
            this.stopAndWatch().catch((error: unknown) => {
              log('error', 'Idle stop-and-watch failed', {
                queue: this.queueName,
                error: error instanceof Error ? error.message : String(error),
              });
            });
          } else {
            this.stop().catch((error: unknown) => {
              log('error', 'Idle timeout stop failed', {
                queue: this.queueName,
                error: error instanceof Error ? error.message : String(error),
              });
            });
          }
          return;
        }
      }
    }
  }

  protected recycleIdleWorkers(): void {
    if (this.idleRecycleMs <= 0) return;
    const now = Date.now();
    let aliveIdleCount = 0;
    for (const worker of this.workers) {
      if (!worker.busy && !worker.terminated) aliveIdleCount++;
    }
    for (const worker of this.workers) {
      if (worker.busy || worker.terminated) continue;
      if (aliveIdleCount <= 1) break;
      if (worker.lastIdleAt > 0 && now - worker.lastIdleAt >= this.idleRecycleMs) {
        worker.worker.terminate();
        worker.terminated = true;
        aliveIdleCount--;
      }
    }
  }

  protected abstract dispatch(worker: WorkerProcess, job: DomainJob, token: string | null): void;
  protected abstract handleMessage(worker: WorkerProcess, message: IPCResponse): void;
  protected abstract handleCrash(worker: WorkerProcess, index: number): void;
}
