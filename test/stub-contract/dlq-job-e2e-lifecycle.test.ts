import { afterEach, describe, expect, test } from 'bun:test';
import { jobId } from '../../src/domain/types/job';
import {
  activateDirect,
  createFailedDlqJob,
  retryDirect,
  waitForState,
} from './dlq-job-e2e-helpers';
import { closeTcpHarness, makeTcpQueue, startTcpHarness, type TcpHarness } from './tcp-harness';

let harness: TcpHarness | null = null;

function setup(prefix: string) {
  harness = startTcpHarness();
  const queueName = `${prefix}-${Bun.randomUUIDv7()}`;
  return { harness, queueName, queue: makeTcpQueue<{ value: number }>(harness, queueName) };
}

afterEach(async () => {
  await closeTcpHarness(harness);
  harness = null;
});

describe('DLQ Job lifecycle methods over a real TCP broker', () => {
  test('extendLock renews a real processing lease', async () => {
    const fixture = setup('dlq-extend-lock');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    const lease = await fixture.harness.manager.pullWithLock(
      fixture.queueName,
      'dlq-e2e-worker',
      0,
      30_000
    );
    if (!lease.job || !lease.token) throw new Error('Expected a processing lease');
    expect(await job.extendLock(lease.token, 45_000)).toBe(45_000);
  });

  test('clearLogs removes persisted logs', async () => {
    const fixture = setup('dlq-clear-logs');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await job.log('first');
    await job.log('second');
    await job.clearLogs();
    expect(await fixture.queue.getJobLogs(id)).toEqual({ logs: [], count: 0 });
  });

  test('getDependencies reports processed and unprocessed children', async () => {
    const fixture = setup('dlq-dependencies');
    const completed = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 2 },
    });
    const completedPull = await fixture.harness.manager.pull(fixture.queueName);
    if (!completedPull) throw new Error('Expected completed child');
    await fixture.harness.manager.ack(completedPull.id, 'child-result');
    const waiting = await fixture.harness.manager.push(fixture.queueName, { data: { value: 3 } });
    const parent = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 1 },
      childrenIds: [completed.id, waiting.id],
      maxAttempts: 1,
      priority: 10,
    });
    const parentPull = await fixture.harness.manager.pull(fixture.queueName);
    if (!parentPull || parentPull.id !== parent.id) throw new Error('Expected parent');
    await fixture.harness.manager.fail(parentPull.id, 'parent failure');
    const entry = (await fixture.queue.getDlqAsync()).find((item) => item.job.id === parent.id);
    if (!entry) throw new Error('Expected parent in DLQ');
    expect(await entry.job.getDependencies()).toEqual({
      processed: { [`${fixture.queueName}:${completed.id}`]: 'child-result' },
      unprocessed: [`${fixture.queueName}:${waiting.id}`],
    });
  });

  test('getDependenciesCount counts processed and unprocessed children', async () => {
    const fixture = setup('dlq-dependency-count');
    const child = await fixture.harness.manager.push(fixture.queueName, { data: { value: 2 } });
    const parent = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 1 },
      childrenIds: [child.id],
      maxAttempts: 1,
      priority: 10,
    });
    const parentPull = await fixture.harness.manager.pull(fixture.queueName);
    if (!parentPull || parentPull.id !== parent.id) throw new Error('Expected parent');
    await fixture.harness.manager.fail(parentPull.id, 'parent failure');
    const entry = (await fixture.queue.getDlqAsync()).find((item) => item.job.id === parent.id);
    if (!entry) throw new Error('Expected parent in DLQ');
    expect(await entry.job.getDependenciesCount()).toEqual({ processed: 0, unprocessed: 1 });
  });

  test('moveToCompleted completes the reactivated job', async () => {
    const fixture = setup('dlq-move-completed');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    expect(await job.moveToCompleted({ done: true })).toBeNull();
    expect(await fixture.queue.getJobState(id)).toBe('completed');
    expect(fixture.harness.manager.getResult(jobId(id))).toEqual({ done: true });
  });

  test('moveToFailed returns the active job to the DLQ', async () => {
    const fixture = setup('dlq-move-failed');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    await job.moveToFailed(new Error('second failure'));
    expect(await fixture.queue.getJobState(id)).toBe('failed');
    expect((await fixture.queue.getDlqAsync()).some((entry) => entry.job.id === id)).toBe(true);
  });

  test('moveToWait returns the failed job to waiting', async () => {
    const fixture = setup('dlq-move-wait');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    expect(await job.moveToWait()).toBe(true);
    expect(await fixture.queue.getJobState(id)).toBe('waiting');
  });

  test('moveToDelayed parks an active job until its timestamp', async () => {
    const fixture = setup('dlq-move-delayed');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    await job.moveToDelayed(Date.now() + 60_000);
    expect(await fixture.queue.getJobState(id)).toBe('delayed');
  });

  test('moveToWaitingChildren parks an active job', async () => {
    const fixture = setup('dlq-move-waiting-children');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    expect(await job.moveToWaitingChildren()).toBe(true);
    expect(await fixture.queue.getJobState(id)).toBe('waiting-children');
  });

  test('waitUntilFinished resolves with the completed result', async () => {
    const fixture = setup('dlq-wait-finished');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    const waiting = job.waitUntilFinished({}, 2_000);
    await Bun.sleep(10);
    await fixture.harness.manager.ack(jobId(id), 'finished-result');
    expect(await waiting).toBe('finished-result');
  });

  test('discard asynchronously moves a retried job to failed', async () => {
    const fixture = setup('dlq-discard');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    job.discard();
    await waitForState(fixture.queue, id, 'failed');
    expect((await fixture.queue.getDlqAsync()).some((entry) => entry.job.id === id)).toBe(true);
  });

  test('getFailedChildrenValues reads the broker result map', async () => {
    const fixture = setup('dlq-failed-children');
    const { job } = await createFailedDlqJob(fixture.harness, fixture.queue, fixture.queueName, {
      value: 1,
    });
    expect(await job.getFailedChildrenValues()).toEqual({});
  });

  test('getIgnoredChildrenFailures reads the broker result map', async () => {
    const fixture = setup('dlq-ignored-children');
    const { job } = await createFailedDlqJob(fixture.harness, fixture.queue, fixture.queueName, {
      value: 1,
    });
    expect(await job.getIgnoredChildrenFailures()).toEqual({});
  });

  test('removeChildDependency detaches the retried child and releases its parent', async () => {
    const fixture = setup('dlq-remove-dependency');
    const parent = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 1 },
    });
    const child = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 2 },
      maxAttempts: 1,
    });
    await fixture.harness.manager.updateJobParent(child.id, parent.id);
    const pulledChild = await fixture.harness.manager.pull(fixture.queueName);
    if (!pulledChild || pulledChild.id !== child.id) throw new Error('Expected flow child');
    await fixture.harness.manager.fail(pulledChild.id, 'child failure');
    const entry = (await fixture.queue.getDlqAsync()).find((item) => item.job.id === child.id);
    if (!entry) throw new Error('Expected child in DLQ');
    retryDirect(fixture.harness, fixture.queueName, String(child.id));
    expect(await entry.job.removeChildDependency()).toBe(true);
    expect(await fixture.queue.getJobState(String(parent.id))).toBe('waiting');
  });

  test('removeDeduplicationKey cannot remove a replacement owner', async () => {
    const fixture = setup('dlq-remove-dedup');
    const deduplicationId = Bun.randomUUIDv7();
    const { job } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 },
      { deduplication: { id: deduplicationId } }
    );
    const replacement = await fixture.queue.add(
      'replacement',
      { value: 2 },
      { deduplication: { id: deduplicationId } }
    );
    expect(await job.removeDeduplicationKey()).toBe(false);
    expect(await fixture.queue.getDeduplicationJobId(deduplicationId)).toBe(replacement.id);
  });

  test('removeUnprocessedChildren removes waiting children', async () => {
    const fixture = setup('dlq-remove-children');
    const child = await fixture.harness.manager.push(fixture.queueName, { data: { value: 2 } });
    const parent = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 1 },
      childrenIds: [child.id],
      maxAttempts: 1,
      priority: 10,
    });
    const parentPull = await fixture.harness.manager.pull(fixture.queueName);
    if (!parentPull || parentPull.id !== parent.id) throw new Error('Expected parent');
    await fixture.harness.manager.fail(parentPull.id, 'parent failure');
    const entry = (await fixture.queue.getDlqAsync()).find((item) => item.job.id === parent.id);
    if (!entry) throw new Error('Expected parent in DLQ');
    await entry.job.removeUnprocessedChildren();
    expect(await fixture.queue.getJobState(String(child.id))).toBe('unknown');
  });
});
