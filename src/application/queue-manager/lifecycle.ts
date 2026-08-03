import * as bgTasks from '../backgroundTasks';
import { QueueManagerDependencies } from './dependencies';

export class QueueManagerLifecycle extends QueueManagerDependencies {
  shutdown(): void {
    this.cronScheduler.stop();
    this.workerManager.stop();
    this.eventsManager.clear();
    this.telemetryJournal.clearMemory();
    if (this.backgroundTaskHandles) bgTasks.stopBackgroundTasks(this.backgroundTaskHandles);
    this.storage?.close();

    this.jobIndex.clear();
    this.completedJobs.clear();
    this.completedJobsData.clear();
    this.depCompletions.clear();
    this.timedOutJobs.clear();
    this.retiredTimeoutLeaseTokens.clear();
    this.retiredCronLeaseTokens.clear();
    this.jobResults.clear();
    this.dependencyResults.clear();
    this.jobLogs.clear();
    this.customIdMap.clear();
    this.pendingDepChecks.clear();
    this.queueNamesCache.clear();
    this.jobLocks.clear();
    this.stalledCandidates.clear();
    this.clientJobs.clear();
    this.repeatChain.clear();
    this.failedChildrenValues.clear();
    this.ignoredChildrenFailures.clear();
    for (const shard of this.processingShards) shard.clear();
    for (const shard of this.shards) {
      shard.waitingDeps.clear();
      shard.dependencyIndex.clear();
      shard.waitingChildren.clear();
      shard.uniqueKeys.clear();
      shard.activeGroups.clear();
    }
  }
}
