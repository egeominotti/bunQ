import type { AtomicFlowJobInput } from '../flow';
import type { JobInput } from '../job';
import type { BaseCommand } from './base';

export interface PushCommand extends BaseCommand {
  readonly cmd: 'PUSH';
  readonly queue: string;
  readonly data: unknown;
  readonly priority?: number;
  readonly delay?: number;
  readonly maxAttempts?: number;
  readonly backoff?: number | { type: 'fixed' | 'exponential'; delay: number };
  readonly ttl?: number;
  readonly timeout?: number;
  readonly uniqueKey?: string;
  readonly jobId?: string;
  readonly dependsOn?: string[];
  readonly childrenIds?: string[];
  readonly parentId?: string;
  readonly tags?: string[];
  readonly groupId?: string;
  readonly lifo?: boolean;
  readonly removeOnComplete?: boolean;
  readonly removeOnFail?: boolean;
  readonly durable?: boolean;
  readonly repeat?: {
    every?: number;
    limit?: number;
    pattern?: string;
    count?: number;
    startDate?: number;
    endDate?: number;
    tz?: string;
    immediately?: boolean;
    prevMillis?: number;
    offset?: number;
    jobId?: string;
  };
  readonly failParentOnFailure?: boolean;
  readonly removeDependencyOnFailure?: boolean;
  readonly ignoreDependencyOnFailure?: boolean;
  readonly continueParentOnFailure?: boolean;
  readonly stallTimeout?: number;
  readonly stackTraceLimit?: number;
  readonly keepLogs?: number;
  readonly sizeLimit?: number;
  readonly dedup?: { ttl?: number; extend?: boolean; replace?: boolean };
  readonly debounceId?: string;
  readonly debounceTtl?: number;
  readonly timestamp?: number;
}

export interface PushBatchCommand extends BaseCommand {
  readonly cmd: 'PUSHB';
  readonly queue: string;
  readonly jobs: JobInput[];
}

export interface PushFlowCommand extends BaseCommand {
  readonly cmd: 'PUSHF';
  readonly jobs: AtomicFlowJobInput[];
}

export interface PullCommand extends BaseCommand {
  readonly cmd: 'PULL';
  readonly queue: string;
  readonly timeout?: number;
  readonly owner?: string;
  readonly lockTtl?: number;
  readonly detach?: boolean;
}

export interface PullBatchCommand extends BaseCommand {
  readonly cmd: 'PULLB';
  readonly queue: string;
  readonly count: number;
  readonly timeout?: number;
  readonly owner?: string;
  readonly lockTtl?: number;
}

export interface AckCommand extends BaseCommand {
  readonly cmd: 'ACK';
  readonly id: string;
  readonly result?: unknown;
  readonly token?: string;
}

export interface AckBatchCommand extends BaseCommand {
  readonly cmd: 'ACKB';
  readonly ids: string[];
  readonly results?: unknown[];
  readonly tokens?: string[];
}

export interface FailCommand extends BaseCommand {
  readonly cmd: 'FAIL';
  readonly id: string;
  readonly error?: string;
  readonly token?: string;
  readonly unrecoverable?: boolean;
  readonly stack?: string[];
}
