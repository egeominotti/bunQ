/**
 * bunqueue Client API
 *
 * @example
 * ```typescript
 * import { Queue, Worker } from 'bunqueue/client';
 *
 * const queue = new Queue('emails');
 * await queue.add('send', { to: 'user@test.com' });
 *
 * const worker = new Worker('emails', async (job) => {
 *   await job.updateProgress(50);
 *   await job.log('Processing...');
 *   return { success: true };
 * });
 *
 * worker.on('completed', (job, result) => console.log(result));
 * worker.on('progress', (job, progress) => console.log(progress));
 * ```
 */

// Bun-only runtime guard — must evaluate before any module touching Bun.* globals.
import '../require-bun';

export { defineConfig } from '../config';
export type { BunqueueConfig } from '../config';
export { Queue } from './queue';
export { Worker } from './worker';
export type { ManualJob } from './worker/types';
export { Bunqueue } from './bunqueue';
export type {
  BunqueueOptions,
  BunqueueMiddleware,
  RetryStrategy,
  RetryConfig,
  CircuitBreakerConfig,
  TriggerRule,
  PriorityAgingConfig,
  BatchProcessor,
  BatchConfig,
  JobTtlConfig,
  BunqueueDeduplicationConfig,
  BunqueueDebounceConfig,
  BunqueueDlqConfig,
} from './bunqueue';
export { SandboxedWorker } from './sandboxedWorker';
export { Forwarder } from './forwarder';
export type { ForwardOptions, ForwardedInfo } from './forwarder';
export { QueueEvents } from './events';
export type {
  ActiveEvent,
  CompletedEvent,
  DelayedEvent,
  DrainedEvent,
  DuplicatedEvent,
  FailedEvent,
  ProgressEvent,
  QueueEventsOptions,
  RemovedEvent,
  RetriedEvent,
  StalledEvent,
  WaitingChildrenEvent,
  WaitingEvent,
} from './types/events';
export { QueueGroup } from './queueGroup';
export { FlowProducer } from './flow';
export { UnrecoverableError, DelayedError } from './errors';
export { shutdownManager } from './manager';
export { closeSharedTcpClient } from './tcpClient';
export type { ConnectionHealth } from './tcpClient';
export { TcpConnectionPool, getSharedPool, closeAllSharedPools } from './tcpPool';
export type {
  Job,
  JobOptions,
  GroupJobOptions,
  GroupWorkerOptions,
  JobJson,
  JobJsonRaw,
  QueueOptions,
  WorkerOptions,
  Processor,
  FlowJobData,
  StallConfig,
  DlqConfig,
  DlqEntry,
  DlqStats,
  DlqFilter,
  FailureReason,
  ConnectionOptions,
  ParentOpts,
  RateLimiterOptions,
  ChangePriorityOpts,
  GetDependenciesOpts,
  JobDependencies,
  JobDependenciesCount,
  QueueMetrics,
  QueueMetricsMeta,
  QueueMetricType,
} from './types';
export type { FlowStep, FlowResult, FlowJob, JobNode, FlowProducerOptions, FlowOpts } from './flow';
export type { RepeatOpts, JobTemplate, SchedulerInfo } from './queue/scheduler';
export type { SandboxedWorkerOptions } from './sandboxedWorker';
