/**
 * Bunqueue Simple Mode — batch processing.
 * 1:1 port of src/client/bunqueue/batch.ts: accumulates jobs and processes
 * them in groups; flushes on size, on timeout, and on destroy (close).
 */

import type { Job } from '../job.js';
import type { Processor } from '../worker-types.js';
import type { BatchConfig } from './types.js';

interface BufferEntry<T, R> {
  job: Job<T>;
  resolve: (value: R) => void;
  reject: (err: Error) => void;
}

export class BatchAccumulator<T = unknown, R = unknown> {
  private readonly buffer: BufferEntry<T, R>[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  private readonly config: BatchConfig<T, R>;

  constructor(config: BatchConfig<T, R>) {
    this.config = config;
  }

  /** Build a Processor that buffers jobs into batches. */
  buildProcessor(): Processor<T, R> {
    return (job: Job<T>): Promise<R> => {
      return new Promise<R>((resolve, reject) => {
        this.buffer.push({ job, resolve, reject });

        if (this.buffer.length >= this.config.size) {
          this.flush();
        } else if (!this.timer) {
          const timeout = this.config.timeout ?? 5000;
          this.timer = setTimeout(() => {
            this.flush();
          }, timeout);
        }
      });
    };
  }

  flush(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    const batch = this.buffer.splice(0);
    if (batch.length === 0) return;

    const jobs = batch.map((b) => b.job);
    this.config.processor(jobs).then(
      (results) => {
        for (let i = 0; i < batch.length; i++) {
          batch[i].resolve(results[i] ?? (undefined as unknown as R));
        }
      },
      (err: unknown) => {
        const error = err instanceof Error ? err : new Error(String(err));
        for (const b of batch) {
          b.reject(error);
        }
      }
    );
  }

  destroy(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    // Flush remaining
    if (this.buffer.length > 0) {
      this.flush();
    }
  }
}
