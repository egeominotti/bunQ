import type { QueueState } from '../../types/queue';
import type { ShardOptions } from '../../types/shard';
import { DependencyTracker } from '../dependencyTracker';
import { DlqShard } from '../dlqShard';
import { LimiterManager } from '../limiterManager';
import { IndexedPriorityQueue } from '../priorityQueue';
import { ShardCounters } from '../shardCounters';
import { TemporalManager } from '../temporalManager';
import { UniqueKeyManager } from '../uniqueKeyManager';
import { WaiterManager } from '../waiterManager';

/** Owns the collaborating shard managers and basic queue/waiter operations. */
export class ShardState {
  readonly queues = new Map<string, IndexedPriorityQueue>();
  readonly activeGroups = new Map<string, Set<string>>();
  protected readonly uniqueKeyManager = new UniqueKeyManager();
  protected readonly dlqManager: DlqShard;
  protected readonly limiterManager = new LimiterManager();
  protected readonly dependencyTracker = new DependencyTracker();
  protected readonly temporalManager = new TemporalManager();
  protected readonly waiterManager = new WaiterManager();
  protected readonly counters: ShardCounters;

  constructor(options: ShardOptions = {}) {
    this.counters = new ShardCounters(this.temporalManager);
    this.dlqManager = new DlqShard({
      incrementDlq: () => {
        this.counters.incrementDlq();
      },
      decrementDlq: (count) => {
        this.counters.decrementDlq(count);
      },
      onEvict: options.onDlqEvicted,
    });
  }

  notify(queue?: string): void {
    this.waiterManager.notify(queue);
  }

  notifyBatch(count: number): void;
  notifyBatch(queue: string, count: number): void;
  notifyBatch(queueOrCount: string | number, maybeCount?: number): void {
    if (typeof queueOrCount === 'number') {
      this.waiterManager.notifyBatch(queueOrCount);
    } else {
      this.waiterManager.notifyBatch(queueOrCount, maybeCount ?? 0);
    }
  }

  waitForJob(timeoutMs: number, signal?: AbortSignal): Promise<void>;
  waitForJob(queue: string, timeoutMs: number, signal?: AbortSignal): Promise<void>;
  waitForJob(
    queueOrTimeout: string | number,
    maybeTimeoutOrSignal?: number | AbortSignal,
    maybeSignal?: AbortSignal
  ): Promise<void> {
    if (typeof queueOrTimeout === 'number') {
      return this.waiterManager.waitForJob(
        queueOrTimeout,
        maybeTimeoutOrSignal as AbortSignal | undefined
      );
    }
    return this.waiterManager.waitForJob(
      queueOrTimeout,
      typeof maybeTimeoutOrSignal === 'number' ? maybeTimeoutOrSignal : 0,
      maybeSignal
    );
  }

  getQueue(name: string): IndexedPriorityQueue {
    let queue = this.queues.get(name);
    if (!queue) {
      queue = new IndexedPriorityQueue();
      this.queues.set(name, queue);
    }
    return queue;
  }

  getState(name: string): QueueState {
    return this.limiterManager.getState(name);
  }

  isPaused(name: string): boolean {
    return this.limiterManager.isPaused(name);
  }

  pause(name: string): void {
    this.limiterManager.pause(name);
  }

  resume(name: string): void {
    this.limiterManager.resume(name);
    this.waiterManager.notify(name);
  }

  getQueueNames(): string[] {
    const names = new Set<string>();
    for (const name of this.queues.keys()) names.add(name);
    for (const name of this.dlqManager.getQueueNames()) names.add(name);
    for (const name of this.limiterManager.getQueueNames()) names.add(name);
    return Array.from(names);
  }
}
