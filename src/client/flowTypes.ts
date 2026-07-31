/**
 * FlowProducer Types
 * Type definitions for job flows and pipelines
 */

import type { Job, JobOptions, ConnectionOptions } from './types';

/** FlowProducer options */
export interface FlowProducerOptions {
  /** Use embedded mode (no server) */
  embedded?: boolean;
  /** TCP connection options */
  connection?: ConnectionOptions;
}

/** Step definition in a flow (legacy bunqueue API) */
export interface FlowStep<T = unknown> {
  /** Job name */
  name: string;
  /** Queue name */
  queueName: string;
  /** Job data */
  data: T;
  /** Job options */
  opts?: JobOptions;
  /** Child steps (for tree structures) */
  children?: FlowStep<T>[];
}

/** Result of adding a flow (legacy bunqueue API) */
export interface FlowResult {
  /** Job IDs in order */
  jobIds: string[];
}

/**
 * FlowJob - BullMQ v5 compatible flow job definition.
 * Children are processed BEFORE the parent.
 */
export interface FlowJob<T = unknown> {
  /** Job name */
  name: string;
  /** Queue name */
  queueName: string;
  /** Job data */
  data?: T;
  /** Job options */
  opts?: JobOptions;
  /** Child jobs (processed BEFORE this job) */
  children?: FlowJob<T>[];
}

/**
 * JobNode - BullMQ v5 compatible result from adding a flow.
 * Contains the job and its children nodes.
 */
export interface JobNode<T = unknown> {
  /** The job instance */
  job: Job<T>;
  /** Child nodes (if any) */
  children?: JobNode<T>[];
}

/**
 * Options for getFlow method (BullMQ v5 compatible).
 */
export interface GetFlowOpts {
  /** Job ID to get the flow for */
  id: string;
  /** Queue name where the job is located */
  queueName: string;
  /** Maximum depth to traverse (default: unlimited) */
  depth?: number;
  /** Maximum number of children to fetch per level (default: unlimited) */
  maxChildren?: number;
}

/**
 * FlowOpts - BullMQ v5 compatible options for flow creation.
 * Allows setting default job options per queue when adding flows.
 */
export interface FlowOpts {
  /** Default job options per queue name. Set `jobId` on the individual flow node. */
  queuesOptions?: Record<string, Omit<Partial<JobOptions>, 'jobId'>>;
}
