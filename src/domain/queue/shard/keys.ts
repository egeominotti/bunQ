import type { UniqueKeyEntry } from '../../types/deduplication';
import type { JobId } from '../../types/job';
import { ShardState } from './state';

/** Deduplication keys and FIFO group ownership. */
export class ShardKeys extends ShardState {
  isUniqueAvailable(queue: string, key: string): boolean {
    return this.uniqueKeyManager.isAvailable(queue, key);
  }
  getUniqueKeyEntry(queue: string, key: string): UniqueKeyEntry | null {
    return this.uniqueKeyManager.getEntry(queue, key);
  }
  registerUniqueKey(queue: string, key: string, jobId: JobId): void {
    this.uniqueKeyManager.register(queue, key, jobId);
  }
  registerUniqueKeyWithTtl(queue: string, key: string, jobId: JobId, ttl?: number): void {
    this.uniqueKeyManager.registerWithTtl(queue, key, jobId, ttl);
  }
  extendUniqueKeyTtl(queue: string, key: string, ttl: number): boolean {
    return this.uniqueKeyManager.extendTtl(queue, key, ttl);
  }
  releaseUniqueKey(queue: string, key: string): boolean {
    return this.uniqueKeyManager.release(queue, key);
  }
  releaseUniqueKeyIfOwned(queue: string, key: string, ownerId: JobId): boolean {
    return this.uniqueKeyManager.releaseIfOwned(queue, key, ownerId);
  }
  cleanExpiredUniqueKeys(): number {
    return this.uniqueKeyManager.cleanExpired();
  }
  get uniqueKeys(): Map<string, Map<string, UniqueKeyEntry>> {
    return this.uniqueKeyManager.getMap();
  }
  isGroupActive(queue: string, groupId: string): boolean {
    return (this.activeGroupCounts.get(queue)?.get(groupId) ?? 0) > 0;
  }
  activateGroup(queue: string, groupId: string): void {
    let groups = this.activeGroups.get(queue);
    if (!groups) {
      groups = new Set();
      this.activeGroups.set(queue, groups);
    }
    groups.add(groupId);
    let counts = this.activeGroupCounts.get(queue);
    if (!counts) {
      counts = new Map();
      this.activeGroupCounts.set(queue, counts);
    }
    counts.set(groupId, (counts.get(groupId) ?? 0) + 1);
  }
  releaseGroup(queue: string, groupId: string): void {
    const counts = this.activeGroupCounts.get(queue);
    const count = (counts?.get(groupId) ?? 0) - 1;
    if (count > 0) {
      counts?.set(groupId, count);
    } else {
      counts?.delete(groupId);
      const groups = this.activeGroups.get(queue);
      groups?.delete(groupId);
      if (groups?.size === 0) this.activeGroups.delete(queue);
    }
    if (counts?.size === 0) this.activeGroupCounts.delete(queue);
    this.notify(queue);
  }
  getGroupActiveCount(queue: string, groupId: string): number {
    return this.activeGroupCounts.get(queue)?.get(groupId) ?? 0;
  }
}
