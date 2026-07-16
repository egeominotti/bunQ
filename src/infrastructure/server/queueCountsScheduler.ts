import type { QueueManager } from '../../application/queueManager';
import type { QueueJobCounts } from '../../application/queueStatsAggregator';

const COUNTS_COALESCE_MS = 10;

/** Coalesce high-volume job events into one queue-count aggregation per tick. */
export class QueueCountsScheduler {
  private readonly pendingQueues = new Set<string>();
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly queueManager: QueueManager,
    private readonly emit: (queue: string, counts: QueueJobCounts) => void
  ) {}

  schedule(queue: string): void {
    this.pendingQueues.add(queue);
    this.timer ??= setTimeout(() => this.flush(), COUNTS_COALESCE_MS);
  }

  stop(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.pendingQueues.clear();
  }

  private flush(): void {
    this.timer = null;
    if (this.pendingQueues.size === 0) return;

    const queues = Array.from(this.pendingQueues);
    this.pendingQueues.clear();
    const countsByQueue = this.queueManager.getQueueJobCountsBatch(queues);
    for (const queue of queues) {
      const counts = countsByQueue.get(queue);
      if (counts) this.emit(queue, counts);
    }
  }
}
