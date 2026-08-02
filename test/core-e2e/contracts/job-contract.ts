import { EventEmitter } from 'node:events';
import { jobId } from '../../../src/domain/types/job';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { activateFailedJob, createFailedJob, retryFailedJob } from '../support/job-fixtures';
import { CoverageTracker, ensure, ensureEqual, eventually } from '../support/tracker';

export async function runJobContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'job');
  const tracker = new CoverageTracker(mode, 'job-contract');

  try {
    const queue = harness.queue<{ value: number }>('mutations');
    const mutable = await queue.add('mutable', { value: 1 }, { durable: true });
    const json = tracker.call('Job', 'toJSON', () => mutable.toJSON());
    ensure(json.id === mutable.id && json.data.value === 1, 'toJSON did not preserve job data');
    const raw = tracker.call('Job', 'asJSON', () => mutable.asJSON());
    ensure(JSON.parse(raw.data).value === 1, 'asJSON did not serialize job data');
    await tracker.invoke('Job', 'updateData', () => mutable.updateData({ value: 2 }));
    await tracker.invoke('Job', 'updateProgress', () => mutable.updateProgress(40, 'phase one'));
    await tracker.invoke('Job', 'log', () => mutable.log('persisted job log'));
    ensure((await queue.getJobLogs(mutable.id)).count === 1, 'Job.log did not persist');
    await tracker.invoke('Job', 'clearLogs', () => mutable.clearLogs());
    ensure((await queue.getJobLogs(mutable.id)).count === 0, 'Job.clearLogs did not persist');
    ensure(
      await tracker.invoke('Job', 'isWaiting', () => mutable.isWaiting()),
      'job was not waiting'
    );
    await tracker.invoke('Job', 'changePriority', () => mutable.changePriority({ priority: 17 }));
    ensure((await queue.getJob(mutable.id))?.priority === 17, 'Job.changePriority did not persist');
    await mutable.changePriority({ priority: 0 });
    await tracker.invoke('Job', 'changeDelay', () => mutable.changeDelay(60_000));
    ensure(
      await tracker.invoke('Job', 'isDelayed', () => mutable.isDelayed()),
      'job was not delayed'
    );
    await tracker.invoke('Job', 'promote', () => mutable.promote());
    ensure(await mutable.isWaiting(), 'Job.promote did not restore waiting state');
    ensure(
      (await tracker.invoke('Job', 'getState', () => mutable.getState())) === 'waiting',
      'bad state'
    );
    await mutable.remove();

    const removable = await queue.add('remove', { value: 3 }, { durable: true });
    await tracker.invoke('Job', 'remove', () => removable.remove());
    ensure((await queue.getJobState(removable.id)) === 'unknown', 'Job.remove did not delete job');

    const failedQueue = harness.queue<{ value: number }>('failed-state');
    const failed = await createFailedJob(harness, failedQueue, { value: 4 });
    ensure(await tracker.invoke('Job', 'isFailed', () => failed.isFailed()), 'job was not failed');
    ensureEqual(
      await tracker.invoke('Job', 'getFailedChildrenValues', () =>
        failed.getFailedChildrenValues()
      ),
      {},
      'unexpected failed children'
    );
    ensureEqual(
      await tracker.invoke('Job', 'getIgnoredChildrenFailures', () =>
        failed.getIgnoredChildrenFailures()
      ),
      {},
      'unexpected ignored failures'
    );
    await tracker.invoke('Job', 'retry', () => failed.retry());
    ensure(await failed.isWaiting(), 'Job.retry did not restore waiting state');

    const discardQueue = harness.queue<{ value: number }>('discard');
    const discarded = await discardQueue.add('discard', { value: 5 }, { durable: true });
    tracker.call('Job', 'discard', () => discarded.discard());
    await eventually(
      () => discardQueue.getJobState(discarded.id),
      (state) => state === 'failed',
      'Job.discard did not move the job to failed'
    );

    const completedQueue = harness.queue<{ value: number }>('move-completed');
    const completed = await createFailedJob(harness, completedQueue, { value: 6 });
    const completedToken = await activateFailedJob(harness, completedQueue.name, completed.id);
    ensure(
      await tracker.invoke('Job', 'isActive', () => completed.isActive()),
      'job was not active'
    );
    const extended = await tracker.invoke('Job', 'extendLock', () =>
      completed.extendLock(completedToken, 45_000)
    );
    ensure(extended === 45_000, `Job.extendLock returned ${extended}`);
    await tracker.invoke('Job', 'moveToCompleted', () =>
      completed.moveToCompleted({ answer: 42 }, completedToken)
    );
    const completion = await tracker.invoke('Job', 'waitUntilFinished', () =>
      completed.waitUntilFinished(new EventEmitter(), 5_000)
    );
    ensureEqual(completion, { answer: 42 }, 'waitUntilFinished result mismatch');
    ensure(
      await tracker.invoke('Job', 'isCompleted', () => completed.isCompleted()),
      'not completed'
    );

    const movedFailedQueue = harness.queue<{ value: number }>('move-failed');
    const movedFailed = await createFailedJob(harness, movedFailedQueue, { value: 7 });
    const failedToken = await activateFailedJob(harness, movedFailedQueue.name, movedFailed.id);
    await tracker.invoke('Job', 'moveToFailed', () =>
      movedFailed.moveToFailed(new Error('explicit transition'), failedToken)
    );
    ensure(await movedFailed.isFailed(), 'Job.moveToFailed did not fail job');

    const movedWaitQueue = harness.queue<{ value: number }>('move-wait');
    const movedWait = await createFailedJob(harness, movedWaitQueue, { value: 8 });
    ensure(
      await tracker.invoke('Job', 'moveToWait', () => movedWait.moveToWait()),
      'moveToWait false'
    );
    ensure(await movedWait.isWaiting(), 'Job.moveToWait did not restore waiting state');

    const movedDelayedQueue = harness.queue<{ value: number }>('move-delayed');
    const movedDelayed = await createFailedJob(harness, movedDelayedQueue, { value: 9 });
    const delayedToken = await activateFailedJob(harness, movedDelayedQueue.name, movedDelayed.id);
    await tracker.invoke('Job', 'moveToDelayed', () =>
      movedDelayed.moveToDelayed(Date.now() + 60_000, delayedToken)
    );
    ensure(await movedDelayed.isDelayed(), 'Job.moveToDelayed did not delay job');

    const waitingChildrenQueue = harness.queue<{ value: number }>('move-waiting-children');
    const waitingChildren = await createFailedJob(harness, waitingChildrenQueue, { value: 10 });
    const waitingToken = await activateFailedJob(
      harness,
      waitingChildrenQueue.name,
      waitingChildren.id
    );
    ensure(
      await tracker.invoke('Job', 'moveToWaitingChildren', () =>
        waitingChildren.moveToWaitingChildren(waitingToken)
      ),
      'moveToWaitingChildren returned false'
    );
    ensure(
      await tracker.invoke('Job', 'isWaitingChildren', () => waitingChildren.isWaitingChildren()),
      'job was not waiting-children'
    );

    const dependencyQueue = harness.queue<{ value: number }>('dependencies');
    const manager = harness.brokerManager();
    const doneChild = await manager.push(dependencyQueue.name, { data: { value: 11 } });
    const pulledChild = await manager.pull(dependencyQueue.name);
    ensure(pulledChild?.id === doneChild.id, 'completed child was not pulled');
    await manager.ack(doneChild.id, { child: 'done' });
    const waitingChild = await manager.push(dependencyQueue.name, { data: { value: 12 } });
    const parent = await manager.push(dependencyQueue.name, {
      data: { value: 13 },
      childrenIds: [doneChild.id, waitingChild.id],
      priority: 20,
    });
    const parentJob = await dependencyQueue.getJob(String(parent.id));
    ensure(parentJob, 'dependency parent was not readable');
    const children = await tracker.invoke('Job', 'getChildrenValues', () =>
      parentJob.getChildrenValues<{ child: string }>()
    );
    ensure(
      children[`${dependencyQueue.name}:${doneChild.id}`]?.child === 'done',
      'child result absent'
    );
    const dependencies = await tracker.invoke('Job', 'getDependencies', () =>
      parentJob.getDependencies({ processed: { count: 10 }, unprocessed: { count: 10 } })
    );
    ensure(dependencies.unprocessed.length === 1, 'unprocessed dependency absent');
    ensureEqual(
      await tracker.invoke('Job', 'getDependenciesCount', () => parentJob.getDependenciesCount()),
      { processed: 1, unprocessed: 1 },
      'dependency counts mismatch'
    );
    await tracker.invoke('Job', 'removeUnprocessedChildren', () =>
      parentJob.removeUnprocessedChildren()
    );
    ensure(
      (await dependencyQueue.getJobState(String(waitingChild.id))) === 'unknown',
      'child remained'
    );

    const parentOnly = await manager.push(dependencyQueue.name, { data: { value: 14 } });
    const childOnly = await manager.push(dependencyQueue.name, { data: { value: 15 } });
    await manager.updateJobParent(childOnly.id, parentOnly.id);
    const childJob = await dependencyQueue.getJob(String(childOnly.id));
    ensure(childJob, 'child dependency was not readable');
    ensure(
      await tracker.invoke('Job', 'removeChildDependency', () => childJob.removeChildDependency()),
      'removeChildDependency returned false'
    );

    const dedupId = harness.unique('dedup-key');
    const dedup = await queue.add(
      'dedup',
      { value: 16 },
      { durable: true, deduplication: { id: dedupId } }
    );
    ensure(
      await tracker.invoke('Job', 'removeDeduplicationKey', () => dedup.removeDeduplicationKey()),
      'removeDeduplicationKey returned false'
    );
    ensure((await queue.getDeduplicationJobId(dedupId)) === null, 'deduplication key remained');
  } finally {
    await harness.close();
  }

  return tracker;
}
