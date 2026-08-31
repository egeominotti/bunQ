import { Queue } from './queue';
import { Worker } from './worker';
import { QueueEvents } from './events';
import type { Job } from './types';

/** BullMQ Pro-compatible import aliases backed by bunqueue's native implementations. */
export const QueuePro = Queue;
export const WorkerPro = Worker;
export const QueueEventsPro = QueueEvents;
export type JobPro<T = unknown> = Job<T>;
