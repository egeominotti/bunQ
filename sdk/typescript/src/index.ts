/**
 * bunqueue-client — cross-runtime TypeScript SDK for bunqueue (TCP mode).
 * Works on Node.js ≥20, Bun and Deno ≥2 (uses only node: builtins + msgpackr).
 */

// Simple Mode (Bunqueue): all-in-one Queue + Worker, 1:1 with the official client
export { Bunqueue } from './bunqueue/bunqueue.js';
export type { DlqFilter, DlqStats } from './bunqueue/dlq-rate-limit.js';
export type {
  BatchConfig,
  BatchProcessor,
  BunqueueConnection,
  BunqueueDebounceConfig,
  BunqueueDeduplicationConfig,
  BunqueueDlqConfig,
  BunqueueMiddleware,
  BunqueueOptions,
  CircuitBreakerConfig,
  CircuitState,
  JobTtlConfig,
  PriorityAgingConfig,
  RateLimiterOptions,
  RetryConfig,
  RetryStrategy,
  TriggerRule,
} from './bunqueue/types.js';
export type { Command, ConnectionOptions, Response, TlsOption } from './connection.js';
export { Connection } from './connection.js';
export {
  AuthError,
  BunqueueError,
  CommandError,
  CommandTimeoutError,
  ConnectionClosedError,
  UnrecoverableError,
} from './errors.js';
export { FlowProducer } from './flow.js';
export type {
  FlowJob,
  FlowProducerOptions,
  FlowStep,
  GetFlowOptions,
  JobNode,
} from './flow-types.js';
export { MAX_FRAME_SIZE, PROTOCOL_VERSION } from './frame.js';
export type { JobRaw } from './job.js';
export { Job } from './job.js';
export type { BulkJobEntry, QueueOptions } from './queue.js';
export { Queue } from './queue.js';
export type { SchedulerOptions } from './queue-admin.js';
export type {
  BackoffOptions,
  DeduplicationOptions,
  JobCounts,
  JobOptions,
  JobStateName,
  RepeatOptions,
} from './types.js';
export { Worker } from './worker.js';
export type { Processor, WorkerOptions } from './worker-types.js';

export const __version__ = '0.1.2';
