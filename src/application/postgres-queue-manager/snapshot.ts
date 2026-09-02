import type { DlqEntry, DlqFilter } from '../../domain/types/dlq';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type {
  ClaimedPostgresJob,
  PostgresCompletionResult,
  PostgresCounts,
  PostgresJobState,
  PostgresQueueState,
  PostgresStoredJob,
  PostgresStoreEvent,
} from '../../infrastructure/persistence/postgres';
import { processingShardIndex, shardIndex } from '../../shared/hash';
import { LRUMap } from '../../shared/lru';
import {
  countSnapshotJobs,
  countSnapshotPriorities,
  findSnapshotJob,
  listSnapshotDlq,
  listSnapshotJobs,
  snapshotJobIds,
  snapshotQueueNames,
} from './snapshotViews';

/** Eventually-consistent read model; PostgreSQL remains authoritative for every transition. */
export class PostgresQueueSnapshot {
  private readonly jobs = new Map<JobId, PostgresStoredJob>();
  private readonly results: LRUMap<JobId, { queue: string; result: unknown }>;
  private readonly queues = new Map<string, PostgresQueueState>();
  private readonly knownQueues = new Set<string>();
  private readonly completedOrder = new Map<JobId, true>();

  constructor(
    private readonly maxCompletedJobs: number,
    private readonly maxJobResults: number
  ) {
    this.results = new LRUMap(maxJobResults);
  }

  hydrate(rows: readonly PostgresStoredJob[]): void {
    this.jobs.clear();
    this.completedOrder.clear();
    this.knownQueues.clear();
    for (const row of rows) this.put(row);
  }

  replaceQueue(
    queue: string,
    rows: readonly PostgresStoredJob[],
    state: PostgresQueueState,
    completions: readonly PostgresCompletionResult[] = []
  ): void {
    for (const [id, row] of this.jobs) {
      if (row.job.queue === queue) this.remove(id);
    }
    this.removeQueueResults(queue);
    for (const row of rows) this.put(row);
    for (const completion of completions) this.putCompletion(completion);
    this.queues.set(queue, state);
    this.knownQueues.add(queue);
  }

  removeQueue(queue: string): JobId[] {
    const removed: JobId[] = [];
    for (const [id, row] of this.jobs) {
      if (row.job.queue !== queue) continue;
      this.remove(id);
      removed.push(id);
    }
    this.removeQueueResults(queue);
    this.queues.delete(queue);
    this.knownQueues.delete(queue);
    return removed;
  }

  hydrateResults(entries: readonly PostgresCompletionResult[]): void {
    this.results.clear();
    for (const entry of entries) this.putCompletion(entry);
  }

  put(row: PostgresStoredJob): void {
    this.knownQueues.add(row.job.queue);
    this.completedOrder.delete(row.job.id);
    this.jobs.set(row.job.id, row);
    this.results.delete(row.job.id);
    if (row.state === 'completed') {
      this.putResult(row.job.id, row.job.queue, row.result);
      this.completedOrder.set(row.job.id, true);
      this.trimCompletedJobs();
    }
  }

  claim(claimed: ClaimedPostgresJob): void {
    const current = this.jobs.get(claimed.job.id);
    this.put({
      job: claimed.job,
      state: 'active',
      result: current?.result,
      error: null,
      dlqEntry: null,
      dlqRetryState: current?.dlqRetryState ?? null,
      token: claimed.token,
      leaseOwner: claimed.owner,
      leaseBrokerId: claimed.brokerId,
      leaseUntil: claimed.leaseUntil,
      leaseRenewals: 0,
      version: (current?.version ?? 0) + 1,
    });
  }

  remove(id: JobId): void {
    this.jobs.delete(id);
    this.completedOrder.delete(id);
    this.results.delete(id);
  }

  reconcile(
    id: JobId,
    row: PostgresStoredJob | null,
    completion: PostgresCompletionResult | null
  ): void {
    if (row) {
      this.put(row);
      return;
    }
    this.remove(id);
    if (completion) this.putCompletion(completion);
  }

  setQueueState(state: PostgresQueueState): void {
    this.queues.set(state.queue, state);
    this.knownQueues.add(state.queue);
  }

  apply(event: PostgresStoreEvent): void {
    if (event.removed || event.type === 'removed') {
      this.remove(event.jobId);
      if (
        event.type === 'completed' &&
        event.state === 'completed' &&
        Object.hasOwn(event, 'result')
      ) {
        this.putResult(event.jobId, event.queue, event.result);
      }
      return;
    }
    if (Object.hasOwn(event, 'result')) this.putResult(event.jobId, event.queue, event.result);
    if (!event.job || !event.state) return;
    const current = this.jobs.get(event.jobId);
    this.put({
      job: event.job,
      state: event.state,
      result: Object.hasOwn(event, 'result') ? event.result : current?.result,
      error: event.error ?? current?.error ?? null,
      dlqEntry: Object.hasOwn(event, 'dlqEntry')
        ? (event.dlqEntry ?? null)
        : (current?.dlqEntry ?? null),
      dlqRetryState: Object.hasOwn(event, 'dlqRetryState')
        ? (event.dlqRetryState ?? null)
        : (current?.dlqRetryState ?? null),
      token: event.state === 'active' ? (current?.token ?? null) : null,
      leaseOwner: event.state === 'active' ? (current?.leaseOwner ?? null) : null,
      leaseBrokerId: event.state === 'active' ? (current?.leaseBrokerId ?? null) : null,
      leaseUntil: event.state === 'active' ? (current?.leaseUntil ?? null) : null,
      leaseRenewals: event.state === 'active' ? (current?.leaseRenewals ?? 0) : 0,
      version: (current?.version ?? 0) + 1,
    });
  }

  get(id: JobId): PostgresStoredJob | null {
    return this.jobs.get(id) ?? null;
  }

  getResult(id: JobId): unknown {
    return this.results.get(id)?.result;
  }

  hasResult(id: JobId): boolean {
    return this.results.has(id);
  }

  locations(): Map<JobId, JobLocation> {
    const locations = new Map<JobId, JobLocation>();
    for (const [id, row] of this.jobs) {
      if (row.state === 'active') {
        locations.set(id, {
          type: 'processing',
          shardIdx: processingShardIndex(id),
          queueName: row.job.queue,
        });
      } else if (row.state === 'completed') {
        locations.set(id, { type: 'completed', queueName: row.job.queue });
      } else if (row.state === 'failed') {
        locations.set(id, { type: 'dlq', queueName: row.job.queue });
      } else {
        locations.set(id, {
          type: 'queue',
          shardIdx: shardIndex(row.job.queue),
          queueName: row.job.queue,
        });
      }
    }
    return locations;
  }

  completedIds(): Set<JobId> {
    return new Set(
      [...this.jobs.values()].flatMap((row) => (row.state === 'completed' ? [row.job.id] : []))
    );
  }

  completionIds(): Set<JobId> {
    return new Set([...this.completedIds(), ...this.results.keys()]);
  }

  private putCompletion(entry: PostgresCompletionResult): void {
    this.putResult(entry.jobId, entry.queue, entry.result);
  }

  private putResult(id: JobId, queue: string, result: unknown): void {
    if (this.maxJobResults === 0) return;
    this.results.set(id, { queue, result });
  }

  private removeQueueResults(queue: string): void {
    for (const [id, entry] of this.results) {
      if (entry.queue === queue) this.results.delete(id);
    }
  }

  private trimCompletedJobs(): void {
    while (this.completedOrder.size > this.maxCompletedJobs) {
      const oldest = this.completedOrder.keys().next().value;
      if (oldest === undefined) return;
      this.completedOrder.delete(oldest);
      this.jobs.delete(oldest);
    }
  }

  getByCustomId(customId: string): Job | null {
    return findSnapshotJob(this.jobs, (job) => job.customId === customId);
  }

  getByUniqueKey(queue: string, key: string): Job | null {
    return findSnapshotJob(this.jobs, (job) => job.queue === queue && job.uniqueKey === key);
  }

  ids(queue: string, states?: readonly PostgresJobState[]): JobId[] {
    return snapshotJobIds(this.jobs, queue, states);
  }

  removeStates(queue: string, states: readonly PostgresJobState[]): number {
    let count = 0;
    for (const [id, row] of this.jobs) {
      if (row.job.queue !== queue || !states.includes(row.state)) continue;
      this.remove(id);
      count++;
    }
    return count;
  }

  queueState(queue: string): PostgresQueueState | null {
    return this.queues.get(queue) ?? null;
  }

  queueNames(): string[] {
    return snapshotQueueNames(this.jobs, this.queues, this.knownQueues);
  }

  list(
    queue: string,
    options: { state?: string | string[]; start?: number; end?: number; asc?: boolean } = {}
  ): Job[] {
    return listSnapshotJobs(this.jobs, this.queues, queue, options);
  }

  counts(queue?: string): PostgresCounts {
    return countSnapshotJobs(this.jobs, queue);
  }

  priorities(queue: string): Record<number, number> {
    return countSnapshotPriorities(this.jobs, queue);
  }

  dlq(queue: string, filter?: DlqFilter): DlqEntry[] {
    return listSnapshotDlq(this.jobs, queue, filter);
  }

  size(): number {
    return this.jobs.size;
  }
}
