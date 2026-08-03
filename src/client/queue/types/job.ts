import type { TcpConnectionPool } from '../../tcpPool';
import type { JobOptions, JobStateType } from '../../types';

export interface JobProxyContext {
  queueName: string;
  tcp: TcpConnectionPool;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
}

export interface JobReflectionMeta {
  attemptsMade?: number;
  attemptsStarted?: number;
  progress?: number;
  stalledCounter?: number;
  priority?: number;
  delay?: number;
  processedOn?: number;
  finishedOn?: number;
  opts?: JobOptions;
  timestamp?: number;
  stacktrace?: string[] | null;
  returnvalue?: unknown;
  failedReason?: string;
}

export interface ReflectedFields {
  delay: number;
  priority: number;
  opts: JobOptions;
  deduplicationId: string | undefined;
  parentKey: string | undefined;
  parent: { id: string; queueQualifiedName: string } | undefined;
  repeatJobKey: string | undefined;
  stacktrace: string[] | null;
  returnvalue: unknown;
  failedReason: string | undefined;
}

export interface SimpleJobContext {
  queueName: string;
  embedded?: boolean;
  tcp?: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
  meta?: JobReflectionMeta;
}
