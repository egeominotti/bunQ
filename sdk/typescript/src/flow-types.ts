/** FlowProducer types. */

import type { Connection, TlsOption } from './connection.js';
import type { Job } from './job.js';
import type { JobOptions } from './types.js';

export interface FlowJob<T = unknown> {
  name: string;
  queueName: string;
  data?: T;
  opts?: JobOptions;
  children?: FlowJob<T>[];
}

export interface JobNode<T = unknown> {
  job: Job<T>;
  children?: JobNode<T>[];
}

export interface FlowStep<T = unknown> {
  name: string;
  queueName: string;
  data?: T;
  opts?: JobOptions;
}

export interface FlowProducerOptions {
  host?: string;
  port?: number;
  token?: string;
  tls?: TlsOption;
  connection?: Connection;
}

export interface GetFlowOptions {
  id: string;
  queueName?: string;
  depth?: number;
  maxChildren?: number;
}
