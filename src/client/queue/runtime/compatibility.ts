import * as bullmqCompatOps from '../bullmqCompat';
import * as workersOps from '../workers';
import { QueueScheduling } from './scheduling';

/** BullMQ compatibility helpers plus worker and metrics inspection. */
export class QueueCompatibility<T> extends QueueScheduling<T> {
  getPrioritized(start?: number, end?: number) {
    return bullmqCompatOps.getPrioritized<T>(
      {
        ...this.addCtx,
        getWaitingAsync: (from?: number, to?: number) => this.getWaitingAsync(from, to),
        getPrioritizedAsync: (from?: number, to?: number) =>
          this.getJobsAsync({ state: 'prioritized', start: from, end: to }),
      } as never,
      start,
      end
    );
  }

  getPrioritizedCount() {
    return bullmqCompatOps.getPrioritizedCount<T>({
      ...this.addCtx,
      getWaitingAsync: (from?: number, to?: number) => this.getWaitingAsync(from, to),
      getPrioritizedAsync: (from?: number, to?: number) =>
        this.getJobsAsync({ state: 'prioritized', start: from, end: to }),
    } as never);
  }

  getWaitingChildren(start?: number, end?: number) {
    return bullmqCompatOps.getWaitingChildren<T>(this.addCtx as never, start, end);
  }

  getWaitingChildrenCount() {
    return bullmqCompatOps.getWaitingChildrenCount<T>(this.addCtx as never);
  }

  trimEvents(maxLength: number) {
    return workersOps.trimEvents(this.ctx, maxLength);
  }

  getWorkers() {
    return workersOps.getWorkers(this.ctx);
  }

  getWorkersCount() {
    return workersOps.getWorkersCount(this.ctx);
  }

  getMetrics(type: 'completed' | 'failed', start?: number, end?: number) {
    return workersOps.getMetrics(this.ctx, type, start, end);
  }
}
