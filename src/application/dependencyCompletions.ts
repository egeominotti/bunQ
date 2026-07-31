import type { Job, JobId } from '../domain/types/job';
import type { Shard } from '../domain/queue/shard';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';
import type { SetLike } from '../shared/lru';

interface CompletionRecord {
  jobId: JobId;
  pinned: boolean;
}

/**
 * Payload-free completion evidence has two retention classes:
 * recent IDs are FIFO-bounded, while IDs referenced by live dependency edges
 * stay pinned until their final consumer is durably resolved or removed.
 */
export class DependencyCompletionTracker implements SetLike<JobId> {
  private readonly recent = new Set<JobId>();
  private readonly pinned = new Set<JobId>();
  private readonly maxRecent: number;

  constructor(
    maxRecent: number,
    private readonly onRecentEvict?: (jobId: JobId) => void
  ) {
    this.maxRecent = Math.max(1, Math.trunc(maxRecent));
  }

  add(jobId: JobId): void {
    if (this.pinned.has(jobId) || this.recent.has(jobId)) return;
    if (this.recent.size >= this.maxRecent) {
      const oldest = this.recent.values().next().value;
      if (oldest !== undefined) {
        this.recent.delete(oldest);
        this.onRecentEvict?.(oldest);
      }
    }
    this.recent.add(jobId);
  }

  pin(jobId: JobId): void {
    this.recent.delete(jobId);
    this.pinned.add(jobId);
  }

  unpin(jobId: JobId, retainAsRecent: boolean): void {
    this.pinned.delete(jobId);
    if (retainAsRecent) this.recent.add(jobId);
    else this.recent.delete(jobId);
  }

  hydrate(records: Iterable<CompletionRecord>): void {
    this.clear();
    for (const record of records) {
      if (record.pinned) this.pin(record.jobId);
      else this.add(record.jobId);
    }
  }

  pinnedValues(): IterableIterator<JobId> {
    return this.pinned.values();
  }

  has(jobId: JobId): boolean {
    return this.pinned.has(jobId) || this.recent.has(jobId);
  }

  delete(jobId: JobId): boolean {
    const wasPinned = this.pinned.delete(jobId);
    return this.recent.delete(jobId) || wasPinned;
  }

  clear(): void {
    this.recent.clear();
    this.pinned.clear();
  }

  get size(): number {
    return this.recent.size + this.pinned.size;
  }
}

export interface DependencyCompletionContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
}

export function hasDependencyWaiters(shards: Shard[], jobId: JobId): boolean {
  return shards.some((shard) => (shard.getJobsWaitingFor(jobId)?.size ?? 0) > 0);
}

/** Persist a removeOnComplete transition before publishing its RAM evidence. */
export function commitRemovedCompletion(
  job: Pick<Job, 'id' | 'queue'>,
  ctx: DependencyCompletionContext,
  completedAt: number = Date.now()
): void {
  const pinned = hasDependencyWaiters(ctx.shards, job.id);
  ctx.storage?.commitRemovedCompletion(job, ctx.maxDependencyCompletions, pinned, completedAt);
  if (pinned) ctx.depCompletions?.pin(job.id);
  else ctx.depCompletions?.add(job.id);
}

/**
 * Protect recent proofs when a newly accepted parent waits on another
 * dependency. SQLite is updated first so RAM never advertises a stronger
 * durability guarantee than the database.
 */
export function pinReferencedCompletions(
  dependencyIds: Iterable<JobId>,
  ctx: Pick<DependencyCompletionContext, 'storage' | 'depCompletions'>
): void {
  const ids = [...new Set(dependencyIds)].filter((id) => ctx.depCompletions?.has(id) ?? false);
  if (ids.length === 0) return;
  ctx.storage?.pinDependencyCompletions(ids);
  for (const id of ids) ctx.depCompletions?.pin(id);
}

/**
 * Unpin candidates only after every shard has released its reverse edge.
 * Storage returns the complete retained set because pruning can also evict
 * older recent entries; hydrating from it keeps both RAM tiers exact.
 */
export function releaseDependencyCompletionPins(
  dependencyIds: Iterable<JobId>,
  ctx: DependencyCompletionContext
): void {
  const candidates = [...new Set(dependencyIds)].filter(
    (id) => !hasDependencyWaiters(ctx.shards, id)
  );
  if (candidates.length === 0) return;

  if (ctx.storage) {
    const records = ctx.storage.unpinDependencyCompletions(
      candidates,
      ctx.maxDependencyCompletions
    );
    ctx.depCompletions?.hydrate(records);
    return;
  }

  for (const id of candidates) ctx.depCompletions?.unpin(id, true);
}

/** Rebuild pin ownership from the authoritative reverse dependency indexes. */
export function reconcileDependencyCompletionPins(ctx: DependencyCompletionContext): void {
  const referenced = new Set<JobId>();
  for (const shard of ctx.shards) {
    for (const dependencyId of shard.dependencyIndex.keys()) referenced.add(dependencyId);
  }

  if (ctx.storage) {
    const records = ctx.storage.reconcileDependencyCompletionPins(
      referenced,
      ctx.maxDependencyCompletions
    );
    ctx.depCompletions?.hydrate(records);
    return;
  }

  for (const id of ctx.depCompletions?.pinnedValues() ?? []) {
    if (!referenced.has(id)) ctx.depCompletions?.unpin(id, true);
  }
}
