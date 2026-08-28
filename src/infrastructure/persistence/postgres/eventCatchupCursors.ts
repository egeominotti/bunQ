import type { PostgresEventPruneWatermark } from './eventPruneWatermarks';

interface HandledPruneWatermark {
  readonly commitSeq: number;
  readonly prunedThrough: number;
  readonly selfPrunedCommitSeq: number;
}

/** Per-queue catch-up bookkeeping for commit-ordered durable event delivery. */
export class PostgresEventCatchupCursors {
  private readonly appliedCommitSeqs = new Map<string, number>();
  private readonly handledPruneWatermarks = new Map<string, HandledPruneWatermark>();
  private baselineCommitSeq = 0;

  reset(baselineCommitSeq: number): void {
    this.baselineCommitSeq = baselineCommitSeq;
    this.appliedCommitSeqs.clear();
    this.handledPruneWatermarks.clear();
  }

  appliedThrough(queue: string): number {
    return this.appliedCommitSeqs.get(queue) ?? this.baselineCommitSeq;
  }

  applied(queue: string, commitSeq: number): void {
    this.appliedCommitSeqs.set(queue, Math.max(this.appliedThrough(queue), commitSeq));
  }

  /** Record a prune watermark as accounted for and report whether it was new. */
  remember(watermark: PostgresEventPruneWatermark): boolean {
    const { queue, commitSeq, prunedThrough, selfPrunedCommitSeq } = watermark;
    const handled = this.handledPruneWatermarks.get(queue);
    if (
      handled !== undefined &&
      handled.commitSeq >= commitSeq &&
      handled.prunedThrough >= prunedThrough &&
      handled.selfPrunedCommitSeq >= selfPrunedCommitSeq
    ) {
      return false;
    }
    this.handledPruneWatermarks.set(queue, {
      commitSeq: Math.max(handled?.commitSeq ?? 0, commitSeq),
      prunedThrough: Math.max(handled?.prunedThrough ?? 0, prunedThrough),
      selfPrunedCommitSeq: Math.max(handled?.selfPrunedCommitSeq ?? 0, selfPrunedCommitSeq),
    });
    return true;
  }

  /** Report whether a commit pruned its own events without this reader knowing. */
  private hasUnhandledSelfPrune(watermark: PostgresEventPruneWatermark): boolean {
    const handled = this.handledPruneWatermarks.get(watermark.queue);
    return watermark.selfPrunedCommitSeq > (handled?.selfPrunedCommitSeq ?? 0);
  }

  /**
   * Report whether a prune watermark requires an authoritative queue refresh.
   *
   * Two cases need one. A reader at or behind the pruned frontier may have
   * missed discarded history. And a commit that writes more queue events than
   * the retained window prunes its own older events before any reader can
   * observe them, so every reader must reload once per such commit, even one
   * whose applied position has already moved past it. `self_pruned_commit_seq`
   * carries that evidence forward per queue, so a superseding watermark does
   * not drop it; queue destruction erases it with the queue itself, which is
   * safe because the same transaction also invalidates the queue and commit
   * sequence numbers never rewind. A watermark already accounted for never
   * refreshes again. The `<=` comparison is deliberately wider than the
   * observed cases: it also covers a commit split across drain batches, where
   * the applied cursor already equals the pruned frontier while the rest of
   * that commit is still unread.
   */
  requiresRefresh(watermark: PostgresEventPruneWatermark): boolean {
    const behind = this.appliedThrough(watermark.queue) <= watermark.prunedCommitSeq;
    const selfPruned = this.hasUnhandledSelfPrune(watermark);
    if (!this.remember(watermark)) return false;
    if (!behind && !selfPruned) return false;
    this.applied(watermark.queue, watermark.commitSeq);
    return true;
  }
}
