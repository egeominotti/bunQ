import { EventEmitter } from 'node:events';
import { QueueEvents, type Queue } from '../../../src/client';
import type { Job as InternalJob } from '../../../src/domain/types/job';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

interface ActiveFixture {
  id: string;
  internal: InternalJob;
  queue: Queue<{ value: number }>;
  token: string;
}

export async function runQueueFlowContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-flow');
  const tracker = new CoverageTracker(mode, 'queue-flow-contract');

  const activeFixture = async (label: string): Promise<ActiveFixture> => {
    const queue = harness.queue<{ value: number }>(label);
    const job = await queue.add('active', { value: 1 }, { attempts: 1, durable: true });
    const lease = await harness
      .brokerManager()
      .pullWithLock(queue.name, `core-e2e-${label}`, 0, 30_000);
    const { job: internal, token } = lease;
    ensure(internal && String(internal.id) === job.id, `${label} was not pulled manually`);
    ensure(token, `${label} did not receive a lock token`);
    return { id: job.id, internal, queue, token };
  };

  try {
    const flowQueue = harness.queue<Record<string, unknown>>('dependencies');
    const producer = harness.flow();
    const flow = await producer.add({
      name: 'parent',
      queueName: flowQueue.name,
      data: { kind: 'parent' },
      children: [
        { name: 'child-a', queueName: flowQueue.name, data: { value: 2 } },
        { name: 'child-b', queueName: flowQueue.name, data: { value: 3 } },
      ],
    });
    ensure(flow.children?.length === 2, 'dependency fixture did not create children');

    const waitingChildren = await tracker.invoke('Queue', 'getWaitingChildren', () =>
      flowQueue.getWaitingChildren(0, -1)
    );
    ensure(
      waitingChildren.some((job) => job.id === flow.job.id),
      'getWaitingChildren missed parent'
    );
    const waitingCount = await tracker.invoke('Queue', 'getWaitingChildrenCount', () =>
      flowQueue.getWaitingChildrenCount()
    );
    ensure(waitingCount >= 1, `getWaitingChildrenCount returned ${waitingCount}`);
    const dependencies = await tracker.invoke('Queue', 'getJobDependencies', () =>
      flowQueue.getJobDependencies(flow.job.id)
    );
    ensure(dependencies.unprocessed.length === 2, 'getJobDependencies missed children');
    const dependencyCount = await tracker.invoke('Queue', 'getJobDependenciesCount', () =>
      flowQueue.getJobDependenciesCount(flow.job.id)
    );
    ensure(dependencyCount.unprocessed === 2, 'getJobDependenciesCount mismatch');
    const legacyDependencies = await tracker.invoke('Queue', 'getDependencies', () =>
      flowQueue.getDependencies(flow.job.id, 'unprocessed', 0, -1)
    );
    ensure(legacyDependencies.unprocessed?.length === 2, 'getDependencies missed children');

    const flowWorker = harness.worker<Record<string, unknown>, string>(
      flowQueue.name,
      async (job) => job.name
    );
    await eventually(
      () => flowQueue.getJobState(flow.job.id),
      (state) => state === 'completed',
      'flow parent did not complete'
    );
    const childValues = await tracker.invoke('Queue', 'getChildrenValues', () =>
      flowQueue.getChildrenValues(flow.job.id)
    );
    ensure(Object.keys(childValues).length === 2, 'getChildrenValues missed completed children');
    await flowWorker.close();

    const complete = await activeFixture('move-complete');
    const completedResult = await tracker.invoke('Queue', 'moveJobToCompleted', () =>
      complete.queue.moveJobToCompleted(complete.id, { ok: true }, complete.token)
    );
    ensure(completedResult === null, 'moveJobToCompleted returned a next job unexpectedly');
    ensure((await complete.queue.getJobState(complete.id)) === 'completed', 'move complete failed');

    const fail = await activeFixture('move-fail');
    await tracker.invoke('Queue', 'moveJobToFailed', () =>
      fail.queue.moveJobToFailed(fail.id, new Error('manual failure'), fail.token)
    );
    ensure(
      (await fail.queue.getJobState(fail.id)) === 'failed',
      'moveJobToFailed did not fail job'
    );

    const wait = await activeFixture('move-wait');
    const movedToWait = await tracker.invoke('Queue', 'moveJobToWait', () =>
      wait.queue.moveJobToWait(wait.id, wait.token)
    );
    ensure(
      movedToWait && (await wait.queue.getJobState(wait.id)) === 'waiting',
      'move wait failed'
    );

    const delayed = await activeFixture('move-delayed');
    await tracker.invoke('Queue', 'moveJobToDelayed', () =>
      delayed.queue.moveJobToDelayed(delayed.id, Date.now() + 60_000, delayed.token)
    );
    ensure((await delayed.queue.getJobState(delayed.id)) === 'delayed', 'move delayed failed');

    const parked = await activeFixture('move-waiting-children');
    const movedToChildren = await tracker.invoke('Queue', 'moveJobToWaitingChildren', () =>
      parked.queue.moveJobToWaitingChildren(parked.id, parked.token)
    );
    ensure(movedToChildren, 'moveJobToWaitingChildren returned false');
    ensure(
      (await parked.queue.getJobState(parked.id)) === 'waiting-children',
      'job did not enter waiting-children'
    );

    const finishedQueue = harness.queue<{ value: number }>('wait-finished');
    const finishedWorker = harness.worker(finishedQueue.name, async (job) => {
      await Bun.sleep(50);
      return job.data.value * 2;
    });
    const finished = await finishedQueue.add('finish', { value: 21 }, { durable: true });
    await eventually(
      () => finishedQueue.getJobState(finished.id),
      (state) => state === 'completed',
      'waitUntilFinished fixture did not complete'
    );
    const emitter = new EventEmitter();
    const result = await tracker.invoke('Queue', 'waitJobUntilFinished', () =>
      finishedQueue.waitJobUntilFinished(finished.id, emitter, 2_000)
    );
    ensure(result === 42, `waitJobUntilFinished returned ${String(result)}`);

    const liveEvents = new QueueEvents(finishedQueue.name, harness.queueOptions());
    harness.addCleanup(() => liveEvents.close());
    await liveEvents.waitUntilReady();
    const live = await finishedQueue.add('finish-live', { value: 22 }, { durable: true });
    const liveResult = await tracker.invoke('Queue', 'waitJobUntilFinished', () =>
      finishedQueue.waitJobUntilFinished(live.id, liveEvents, 2_000)
    );
    ensure(liveResult === 44, `in-flight waitJobUntilFinished returned ${String(liveResult)}`);
    await finishedWorker.close();
  } finally {
    await harness.close();
  }

  return tracker;
}
