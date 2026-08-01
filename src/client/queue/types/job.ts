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
  priority?: number;
  delay?: number;
  opts?: JobOptions;
  timestamp?: number;
  stacktrace?: string[] | null;
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
