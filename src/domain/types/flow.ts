import type { Job, JobId, JobInput } from './job';

/** One fully-resolved job in an atomic flow commit. */
export interface AtomicFlowJobInput {
  readonly id: JobId;
  readonly queue: string;
  readonly input: JobInput;
}

/** Wire-neutral batch consumed by embedded and TCP FlowProducer paths. */
export interface AtomicFlowBatchInput {
  readonly jobs: AtomicFlowJobInput[];
}

/** Authoritative snapshots returned after a successful flow commit. */
export interface AtomicFlowBatchResult {
  readonly jobs: Job[];
}

export type FlowFailureMode = 'fail' | 'remove' | 'ignore' | 'continue';

/** Durable recovery record for a terminal child failure. */
export interface FlowFailureRecord {
  readonly parentId: JobId;
  readonly childId: JobId;
  readonly childQueue: string;
  readonly mode: FlowFailureMode;
  readonly error: string;
  readonly createdAt: number;
}
