import { SkipList } from '../../shared/skipList';
import type { JobId } from '../types/job';

export interface TemporalEntry {
  createdAt: number;
  jobId: JobId;
  queue: string;
}

const compareEntries = (a: TemporalEntry, b: TemporalEntry): number =>
  a.createdAt - b.createdAt || (a.jobId < b.jobId ? -1 : a.jobId > b.jobId ? 1 : 0);

const createQueueIndex = (): SkipList<TemporalEntry> => new SkipList(compareEntries);

/** Queue-local temporal indexes with direct lookup by job ID. */
export class TemporalIndex {
  private readonly indexesByQueue = new Map<string, SkipList<TemporalEntry>>();
  private readonly entriesByJobId = new Map<JobId, TemporalEntry[]>();
  private entryCount = 0;

  get size(): number {
    return this.entryCount;
  }

  add(createdAt: number, jobId: JobId, queue: string): void {
    const jobEntries = this.entriesByJobId.get(jobId);
    if (jobEntries?.some((entry) => entry.createdAt === createdAt)) return;

    const entry = { createdAt, jobId, queue };
    let queueIndex = this.indexesByQueue.get(queue);
    if (!queueIndex) {
      queueIndex = createQueueIndex();
      this.indexesByQueue.set(queue, queueIndex);
    }
    queueIndex.insert(entry);

    if (jobEntries) jobEntries.push(entry);
    else this.entriesByJobId.set(jobId, [entry]);
    this.entryCount++;
  }

  getOldJobs(
    queue: string,
    threshold: number,
    limit: number
  ): Array<{ jobId: JobId; createdAt: number }> {
    if (limit <= 0) return [];
    const queueIndex = this.indexesByQueue.get(queue);
    if (!queueIndex) return [];

    return queueIndex
      .takeWhile((entry) => entry.createdAt <= threshold, limit)
      .map(({ jobId, createdAt }) => ({ jobId, createdAt }));
  }

  remove(jobId: JobId): void {
    const jobEntries = this.entriesByJobId.get(jobId);
    if (!jobEntries?.length) return;

    let targetIndex = 0;
    for (let i = 1; i < jobEntries.length; i++) {
      if (compareEntries(jobEntries[i], jobEntries[targetIndex]) < 0) targetIndex = i;
    }
    const [entry] = jobEntries.splice(targetIndex, 1);
    this.removeFromQueueIndex(entry);

    if (jobEntries.length === 0) this.entriesByJobId.delete(jobId);
    this.entryCount--;
  }

  clearQueue(queue: string): void {
    const queueIndex = this.indexesByQueue.get(queue);
    if (!queueIndex) return;

    for (const entry of queueIndex.values()) this.detachJobEntry(entry);
    this.entryCount -= queueIndex.size;
    this.indexesByQueue.delete(queue);
  }

  cleanOrphaned(validJobIds: Set<JobId>): number {
    let removed = 0;
    for (const [jobId, entries] of this.entriesByJobId) {
      if (validJobIds.has(jobId)) continue;

      for (const entry of entries) {
        this.removeFromQueueIndex(entry);
        removed++;
      }
      this.entriesByJobId.delete(jobId);
    }
    this.entryCount -= removed;
    return removed;
  }

  clear(): void {
    this.indexesByQueue.clear();
    this.entriesByJobId.clear();
    this.entryCount = 0;
  }

  private removeFromQueueIndex(entry: TemporalEntry): void {
    const queueIndex = this.indexesByQueue.get(entry.queue);
    if (!queueIndex) return;
    queueIndex.delete(entry);
    if (queueIndex.isEmpty) this.indexesByQueue.delete(entry.queue);
  }

  private detachJobEntry(entry: TemporalEntry): void {
    const jobEntries = this.entriesByJobId.get(entry.jobId);
    if (!jobEntries) return;
    const index = jobEntries.indexOf(entry);
    if (index !== -1) jobEntries.splice(index, 1);
    if (jobEntries.length === 0) this.entriesByJobId.delete(entry.jobId);
  }
}
