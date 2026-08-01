import type { Job, JobId } from './job';

export interface HeapEntry {
  jobId: JobId;
  priority: number;
  runAt: number;
  lifo: boolean;
  generation: bigint;
}

export interface IndexedJob {
  job: Job;
  generation: bigint;
}
