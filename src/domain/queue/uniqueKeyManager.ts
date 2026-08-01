/**
 * UniqueKeyManager - Handles job deduplication with TTL support
 */

import type { JobId } from '../types/job';
import type { UniqueKeyEntry } from '../types/deduplication';
import { isUniqueKeyExpired, calculateExpiration } from '../types/deduplication';

/**
 * Manages unique keys per queue for job deduplication
 * Supports TTL-based expiration
 */
export class UniqueKeyManager {
  /** Unique keys per queue (queue -> key -> entry) */
  private readonly keys = new Map<string, Map<string, UniqueKeyEntry>>();

  /** Check if unique key is available (not registered or expired) */
  isAvailable(queue: string, key: string): boolean {
    const entry = this.keys.get(queue)?.get(key);
    if (!entry) return true;
    if (isUniqueKeyExpired(entry)) {
      this.keys.get(queue)?.delete(key);
      return true;
    }
    return false;
  }

  /** Get unique key entry (returns null if not found or expired) */
  getEntry(queue: string, key: string): UniqueKeyEntry | null {
    const entry = this.keys.get(queue)?.get(key);
    if (!entry) return null;
    if (isUniqueKeyExpired(entry)) {
      this.keys.get(queue)?.delete(key);
      return null;
    }
    return entry;
  }

  /** Register unique key (legacy method without TTL) */
  register(queue: string, key: string, jobId: JobId): void {
    this.registerWithTtl(queue, key, jobId, undefined);
  }

  /** Register unique key with TTL support */
  registerWithTtl(queue: string, key: string, jobId: JobId, ttl?: number): void {
    let queueKeys = this.keys.get(queue);
    if (!queueKeys) {
      queueKeys = new Map();
      this.keys.set(queue, queueKeys);
    }
    const now = Date.now();
    queueKeys.set(key, {
      jobId,
      expiresAt: calculateExpiration(ttl, now),
      registeredAt: now,
    });
  }

  /** Extend TTL for an existing unique key */
  extendTtl(queue: string, key: string, ttl: number): boolean {
    const entry = this.keys.get(queue)?.get(key);
    if (!entry) return false;
    entry.expiresAt = calculateExpiration(ttl);
    return true;
  }

  /** Release unique key */
  release(queue: string, key: string): boolean {
    return this.keys.get(queue)?.delete(key) ?? false;
  }

  /** Release a key only when it is still owned by the given job. */
  releaseIfOwned(queue: string, key: string, ownerId: JobId): boolean {
    const queueKeys = this.keys.get(queue);
    const entry = queueKeys?.get(key);
    if (!entry || entry.jobId !== ownerId) return false;
    return queueKeys?.delete(key) ?? false;
  }

  /** Clean expired unique keys (call periodically) */
  cleanExpired(): number {
    let cleaned = 0;
    const now = Date.now();
    for (const [_queue, queueKeys] of this.keys) {
      for (const [key, entry] of queueKeys) {
        if (isUniqueKeyExpired(entry, now)) {
          queueKeys.delete(key);
          cleaned++;
        }
      }
    }
    return cleaned;
  }

  /** Clear all keys for a queue */
  clearQueue(queue: string): void {
    this.keys.delete(queue);
  }

  /** Get the underlying map (for backward compatibility) */
  getMap(): Map<string, Map<string, UniqueKeyEntry>> {
    return this.keys;
  }
}
