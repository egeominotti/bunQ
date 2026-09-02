/**
 * Serialization utilities
 * Handle BigInt and other special types for JSON serialization
 */

import type { Job } from '../domain/types/job';

/** Compare Unicode text in the same code-point order as SQLite BINARY UTF-8 text. */
export function compareSqliteBinaryText(left: string, right: string): number {
  if (left === right) return 0;
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftPoint = left.codePointAt(leftIndex);
    const rightPoint = right.codePointAt(rightIndex);
    if (leftPoint === undefined || rightPoint === undefined) break;
    if (leftPoint !== rightPoint) return leftPoint - rightPoint;
    leftIndex += leftPoint > 0xffff ? 2 : 1;
    rightIndex += rightPoint > 0xffff ? 2 : 1;
  }
  return left.length - leftIndex - (right.length - rightIndex);
}

/**
 * Serialize a Job for JSON output
 * Converts BigInt IDs to strings
 */
export function serializeJob(job: Job): Record<string, unknown> {
  return {
    id: job.id.toString(),
    queue: job.queue,
    name: job.name,
    data: job.data,
    priority: job.priority,
    createdAt: job.createdAt,
    runAt: job.runAt,
    startedAt: job.startedAt,
    completedAt: job.completedAt,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    backoff: job.backoff,
    ttl: job.ttl,
    timeout: job.timeout,
    uniqueKey: job.uniqueKey,
    customId: job.customId,
    dependsOn: job.dependsOn.map((id) => id.toString()),
    parentId: job.parentId?.toString() ?? null,
    childrenIds: job.childrenIds.map((id) => id.toString()),
    childrenCompleted: job.childrenCompleted,
    tags: job.tags,
    groupId: job.groupId,
    progress: job.progress,
    progressMessage: job.progressMessage,
    removeOnComplete: job.removeOnComplete,
    removeOnFail: job.removeOnFail,
    lastHeartbeat: job.lastHeartbeat,
    stallTimeout: job.stallTimeout,
    stallCount: job.stallCount,
    lifo: job.lifo,
    timeline: job.timeline,
  };
}

/**
 * Serialize multiple jobs
 */
export function serializeJobs(jobs: Job[]): Record<string, unknown>[] {
  return jobs.map(serializeJob);
}

/**
 * Custom JSON replacer for BigInt
 */
export function bigIntReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  return value;
}

/**
 * Stringify with BigInt support
 */
export function jsonStringify(value: unknown): string {
  return JSON.stringify(value, bigIntReplacer);
}
