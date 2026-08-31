import type { FlowJobData, Job, Processor } from '../types';
import { resolveProcessorResult } from './processorResult';

/** Coordinates one processor invocation across independently acknowledged jobs. */
export class BatchExecution<T, R> {
  private readonly jobs: Array<Job<T & FlowJobData> | undefined>;
  private readonly failures: Array<Error | undefined>;
  private readonly signals: AbortSignal[];
  private readonly ready = Promise.withResolvers<undefined>();
  private registered = 0;
  private outcome: Promise<R> | null = null;

  constructor(
    private readonly processor: Processor<T, R>,
    private readonly size: number
  ) {
    this.jobs = new Array(size);
    this.failures = new Array(size);
    this.signals = new Array(size);
  }

  handler(index: number): Processor<T, R> {
    return async (job, context) => {
      this.jobs[index] = job;
      job.getBatch = () =>
        this.jobs.filter((member): member is Job<T & FlowJobData> => member !== undefined);
      job.setAsFailed = (error: Error) => {
        this.failures[index] = error;
      };
      this.signals[index] = context?.signal ?? new AbortController().signal;
      if (++this.registered === this.size) this.ready.resolve(undefined);
      await this.ready.promise;

      if (!this.outcome) {
        const leader = this.jobs[0] as Job<T & FlowJobData>;
        this.outcome = Promise.resolve().then(() => {
          const signal = AbortSignal.any(this.signals);
          this.signals.length = 0;
          return resolveProcessorResult(this.processor(leader, { signal }), signal);
        });
      }
      const result = await this.outcome;
      const failure = this.failures[index];
      if (failure) throw failure;
      return result;
    };
  }
}
