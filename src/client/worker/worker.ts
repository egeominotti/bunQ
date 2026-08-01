/** Public Worker façade. Runtime implementation lives in worker/runtime/. */
import { WorkerExecution } from './runtime/execution';

export class Worker<T = unknown, R = unknown> extends WorkerExecution<T, R> {}
