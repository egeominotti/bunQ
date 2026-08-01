import type { Job } from './job';

export interface FlowJobData {
  __flowParentId?: string;
  __flowParentIds?: string[];
  __parentId?: string;
  __parentQueue?: string;
  __childrenIds?: string[];
}

export type Processor<T = unknown, R = unknown> = (job: Job<T & FlowJobData>) => Promise<R> | R;

export type QueueEventType =
  | 'waiting'
  | 'active'
  | 'completed'
  | 'failed'
  | 'progress'
  | 'removed'
  | 'drained';
