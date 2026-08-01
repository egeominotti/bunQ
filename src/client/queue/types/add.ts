import type { TcpConnectionPool } from '../../tcpPool';
import type {
  GetDependenciesOpts,
  JobDependencies,
  JobDependenciesCount,
  JobOptions,
  JobStateType,
  QueueOptions,
} from '../../types';

export interface ExtendedJobOptions extends JobOptions {
  ttl?: number;
  dependsOn?: string[];
  tags?: string[];
  groupId?: string;
}

export interface AddContext {
  name: string;
  opts: QueueOptions;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
  updateJobData: (id: string, data: unknown) => Promise<void>;
  promoteJob: (id: string) => Promise<void>;
  changeJobDelay: (id: string, delay: number) => Promise<void>;
  changeJobPriority: (id: string, opts: { priority: number; lifo?: boolean }) => Promise<void>;
  extendJobLock: (id: string, token: string, duration: number) => Promise<number>;
  clearJobLogs: (id: string, keepLogs?: number) => Promise<void>;
  getJobDependencies: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependencies>;
  getJobDependenciesCount: (
    id: string,
    opts?: GetDependenciesOpts
  ) => Promise<JobDependenciesCount>;
  moveJobToCompleted: (id: string, result: unknown, token?: string) => Promise<unknown>;
  moveJobToFailed: (id: string, error: Error, token?: string) => Promise<void>;
  moveJobToWait: (id: string, token?: string) => Promise<boolean>;
  moveJobToDelayed: (id: string, timestamp: number, token?: string) => Promise<void>;
  moveJobToWaitingChildren: (
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) => Promise<boolean>;
  waitJobUntilFinished: (id: string, queueEvents: unknown, ttl?: number) => Promise<unknown>;
}
