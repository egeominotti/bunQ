import type { Job, JobId } from '../../types/job';
import { ShardLimits } from './limits';

/** Dependency and waiting-children maps. */
export class ShardDependencies extends ShardLimits {
  get waitingDeps(): Map<JobId, Job> {
    return this.dependencyTracker.waitingDeps;
  }
  get dependencyIndex(): Map<JobId, Set<JobId>> {
    return this.dependencyTracker.dependencyIndex;
  }
  get waitingChildren(): Map<JobId, Job> {
    return this.dependencyTracker.waitingChildren;
  }
  registerDependencies(jobId: JobId, dependsOn: JobId[]): void {
    this.dependencyTracker.registerDependencies(jobId, dependsOn);
  }
  unregisterDependencies(jobId: JobId, dependsOn: JobId[]): void {
    this.dependencyTracker.unregisterDependencies(jobId, dependsOn);
  }
  getJobsWaitingFor(depId: JobId): Set<JobId> | undefined {
    return this.dependencyTracker.getJobsWaitingFor(depId);
  }
}
