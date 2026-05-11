/**
 * Bunqueue — Batch Processing
 * Accumulates jobs and processes them in groups.
 */

import type { Job, FlowJobData, Processor } from '../types';
import type { BatchConfig } from './types';

interface BufferEntry<T, R> {
  job: Job<T & FlowJobData>;
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

  /** Build a Processor that buffers jobs into batches */
  buildProcessor(): Processor<T, R> {
    return (job: Job<T & FlowJobData>): Promise<R> => {
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

    const jobs = batch.map((b) => b.job as unknown as Job<T>);
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
