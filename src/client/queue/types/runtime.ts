import type {
  ChangePriorityOpts,
  GetDependenciesOpts,
  Job,
  JobDependencies,
  JobDependenciesCount,
  JobStateType,
} from '../../types';

/** Internal contract used by context callbacks across the Queue runtime layers. */
export interface QueueRuntime<T> {
  getJobState(id: string): Promise<JobStateType>;
  removeAsync(id: string): Promise<void>;
  retryJob(id: string): Promise<void>;
  getChildrenValues(id: string): Promise<Record<string, unknown>>;
  updateJobData(id: string, data: unknown): Promise<void>;
  promoteJob(id: string): Promise<void>;
  changeJobDelay(id: string, delay: number): Promise<void>;
  changeJobPriority(id: string, opts: ChangePriorityOpts): Promise<void>;
  extendJobLock(id: string, token: string, duration: number): Promise<number>;
  clearJobLogs(id: string, keepLogs?: number): Promise<void>;
  getJobDependencies(id: string, opts?: GetDependenciesOpts): Promise<JobDependencies>;
  getJobDependenciesCount(id: string, opts?: GetDependenciesOpts): Promise<JobDependenciesCount>;
  getJobCountsAsync(): Promise<unknown>;
  getJobsAsync(options: {
    state?: string | string[];
    start?: number;
    end?: number;
    asc?: boolean;
  }): Promise<Job<T>[]>;
  moveJobToCompleted(id: string, result: unknown, token?: string): Promise<unknown>;
  moveJobToFailed(id: string, error: Error, token?: string): Promise<void>;
  moveJobToWait(id: string, token?: string): Promise<boolean>;
  moveJobToDelayed(id: string, timestamp: number, token?: string): Promise<void>;
  moveJobToWaitingChildren(
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ): Promise<boolean>;
  waitJobUntilFinished(id: string, queueEvents: unknown, ttl?: number): Promise<unknown>;
  getWaitingAsync(start?: number, end?: number): Promise<Job<T>[]>;
}
