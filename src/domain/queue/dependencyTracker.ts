/**
 * DependencyTracker - Tracks job dependencies and parent-child relationships
 */

import type { Job, JobId } from '../types/job';

/**
 * Manages job dependency tracking for a shard
 * Provides O(1) lookup when dependencies complete
 */
export class DependencyTracker {
  /** Jobs waiting for dependencies */
  readonly waitingDeps = new Map<JobId, Job>();

  /**
   * Reverse index: depId -> Set of jobIds waiting for that dependency
   * Enables O(1) lookup when a dependency completes instead of O(n) scan
   */
  readonly dependencyIndex = new Map<JobId, Set<JobId>>();

  /** Parent jobs waiting for children to complete */
  readonly waitingChildren = new Map<JobId, Job>();

  /**
   * Register a job's dependencies in the reverse index
   * Call when adding a job to waitingDeps
   */
  registerDependencies(jobId: JobId, dependsOn: JobId[]): void {
    for (const depId of dependsOn) {
      let waiters = this.dependencyIndex.get(depId);
      if (!waiters) {
        waiters = new Set();
        this.dependencyIndex.set(depId, waiters);
      }
      waiters.add(jobId);
    }
  }

  /**
   * Unregister a job's dependencies from the reverse index
   * Call when removing a job from waitingDeps
   */
  unregisterDependencies(jobId: JobId, dependsOn: JobId[]): void {
    for (const depId of dependsOn) {
      const waiters = this.dependencyIndex.get(depId);
      if (waiters) {
        waiters.delete(jobId);
        if (waiters.size === 0) {
          this.dependencyIndex.delete(depId);
        }
      }
    }
  }

  /**
   * Get jobs waiting for a specific dependency - O(1)
   */
  getJobsWaitingFor(depId: JobId): Set<JobId> | undefined {
    return this.dependencyIndex.get(depId);
  }

  /**
   * Add a job that is waiting for dependencies
   */
  addWaitingJob(job: Job): void {
    this.waitingDeps.set(job.id, job);
    if (job.dependsOn && job.dependsOn.length > 0) {
      this.registerDependencies(job.id, job.dependsOn);
    }
  }

  /**
   * Remove a job from waiting deps
   */
  removeWaitingJob(jobId: JobId): Job | undefined {
    const job = this.waitingDeps.get(jobId);
    if (job) {
      this.waitingDeps.delete(jobId);
      if (job.dependsOn && job.dependsOn.length > 0) {
        this.unregisterDependencies(jobId, job.dependsOn);
      }
    }
    return job;
  }

  /**
   * Get a job waiting for dependencies
   */
  getWaitingJob(jobId: JobId): Job | undefined {
    return this.waitingDeps.get(jobId);
  }

  /**
   * Check if a job is waiting for dependencies
   */
  isWaiting(jobId: JobId): boolean {
    return this.waitingDeps.has(jobId);
  }

  /**
   * Add a parent job waiting for children
   */
  addWaitingParent(job: Job): void {
    this.waitingChildren.set(job.id, job);
  }

  /**
   * Remove a parent job from waiting
   */
  removeWaitingParent(jobId: JobId): Job | undefined {
    const job = this.waitingChildren.get(jobId);
    if (job) {
      this.waitingChildren.delete(jobId);
    }
    return job;
  }

  /**
   * Remove every dependency-gated job owned by a queue.
   * Returns the removed ids so the caller can purge global indexes and storage.
   */
  removeQueue(queue: string): JobId[] {
    const removed: JobId[] = [];
    for (const [jobId, job] of this.waitingDeps) {
      if (job.queue !== queue) continue;
      this.removeWaitingJob(jobId);
      removed.push(jobId);
    }
    for (const [jobId, job] of this.waitingChildren) {
      if (job.queue !== queue) continue;
      this.waitingChildren.delete(jobId);
      removed.push(jobId);
    }
    return removed;
  }

  /**
   * Get a parent job waiting for children
   */
  getWaitingParent(jobId: JobId): Job | undefined {
    return this.waitingChildren.get(jobId);
  }

  /**
   * Check if a parent is waiting for children
   */
  isParentWaiting(jobId: JobId): boolean {
    return this.waitingChildren.has(jobId);
  }

  /**
   * Get counts for debugging
   */
  getCounts(): { waitingDeps: number; dependencyIndex: number; waitingChildren: number } {
    return {
      waitingDeps: this.waitingDeps.size,
      dependencyIndex: this.dependencyIndex.size,
      waitingChildren: this.waitingChildren.size,
    };
  }
}
