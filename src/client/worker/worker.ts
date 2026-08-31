/** Public Worker façade. Runtime implementation lives in worker/runtime/. */
import { WorkerExecution } from './runtime/execution';
import { RateLimitError } from '../errors';

export class Worker<T = unknown, R = unknown> extends WorkerExecution<T, R> {
  static RateLimitError(): RateLimitError {
    return new RateLimitError();
  }
}
