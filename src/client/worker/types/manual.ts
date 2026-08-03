import type { Job as InternalJob } from '../../../domain/types/job';

/** Job returned by Worker.getNextJob() for explicit manual processing. */
export type ManualJob<T = unknown> = Omit<InternalJob, 'data'> & {
  readonly data: T;
  /** Broker lease token when WorkerOptions.useLocks is enabled. */
  readonly token?: string;
};
