import { afterEach, describe, expect, test } from 'bun:test';
import { jobId } from '../../src/domain/types/job';
import { activateDirect, createFailedDlqJob, retryDirect } from './dlq-job-e2e-helpers';
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

describe('DLQ Job state and mutation methods over a real TCP broker', () => {
  test('updateProgress persists progress for the reactivated job', async () => {
    const fixture = setup('dlq-progress');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    await job.updateProgress(47, 'running');
    expect(fixture.harness.manager.getProgress(jobId(id))).toEqual({
      progress: 47,
      message: 'running',
    });
  });

  test('log appends a retrievable log entry', async () => {
    const fixture = setup('dlq-log');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await job.log('persisted line');
    const logs = await fixture.queue.getJobLogs(id);
    expect(logs.logs).toEqual(['[info] persisted line']);
    expect(logs.count).toBe(1);
  });

  test('getState reads the failed state', async () => {
    const fixture = setup('dlq-state');
    const { job } = await createFailedDlqJob(fixture.harness, fixture.queue, fixture.queueName, {
      value: 1,
    });
    expect(await job.getState()).toBe('failed');
  });

  test('remove deletes a retried job', async () => {
    const fixture = setup('dlq-remove');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    await job.remove();
    expect(await fixture.queue.getJobState(id)).toBe('unknown');
  });

  test('retry moves the failed job back to waiting', async () => {
    const fixture = setup('dlq-retry');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await job.retry();
    expect(await fixture.queue.getJobState(id)).toBe('waiting');
  });

  test('getChildrenValues returns completed child results', async () => {
    const fixture = setup('dlq-child-values');
    const child = await fixture.harness.manager.push(fixture.queueName, { data: { value: 2 } });
    const pulledChild = await fixture.harness.manager.pull(fixture.queueName);
    if (!pulledChild) throw new Error('Expected completed child');
    await fixture.harness.manager.ack(pulledChild.id, { answer: 42 });
    const parent = await fixture.harness.manager.push(fixture.queueName, {
      data: { value: 1 },
      childrenIds: [child.id],
      maxAttempts: 1,
    });
    const pulledParent = await fixture.harness.manager.pull(fixture.queueName);
    if (!pulledParent) throw new Error('Expected parent');
    await fixture.harness.manager.fail(pulledParent.id, 'parent failure');
    const entry = (await fixture.queue.getDlqAsync()).find((item) => item.job.id === parent.id);
    if (!entry) throw new Error('Expected parent in DLQ');
    expect(await entry.job.getChildrenValues()).toEqual({
      [`${fixture.queueName}:${child.id}`]: { answer: 42 },
    });
  });

  test('isWaiting observes a retried job', async () => {
    const fixture = setup('dlq-is-waiting');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    expect(await job.isWaiting()).toBe(true);
  });

  test('isActive observes a pulled job', async () => {
    const fixture = setup('dlq-is-active');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    expect(await job.isActive()).toBe(true);
  });

  test('isDelayed observes a delayed job', async () => {
    const fixture = setup('dlq-is-delayed');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    await fixture.harness.manager.changeDelay(jobId(id), 60_000);
    expect(await job.isDelayed()).toBe(true);
  });

  test('isCompleted observes a completed job', async () => {
    const fixture = setup('dlq-is-completed');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    await fixture.harness.manager.ack(jobId(id), 'done');
    expect(await job.isCompleted()).toBe(true);
  });

  test('isFailed observes the DLQ job', async () => {
    const fixture = setup('dlq-is-failed');
    const { job } = await createFailedDlqJob(fixture.harness, fixture.queue, fixture.queueName, {
      value: 1,
    });
    expect(await job.isFailed()).toBe(true);
  });

  test('isWaitingChildren observes a parked active job', async () => {
    const fixture = setup('dlq-is-waiting-children');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    await activateDirect(fixture.harness, fixture.queueName, id);
    await fixture.harness.manager.moveToWaitingChildren(jobId(id));
    expect(await job.isWaitingChildren()).toBe(true);
  });

  test('updateData replaces the persisted payload', async () => {
    const fixture = setup('dlq-update-data');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    await job.updateData({ value: 9 });
    expect((await fixture.queue.getJob(id))?.data).toEqual({ value: 9 });
  });

  test('promote moves a delayed job back to waiting', async () => {
    const fixture = setup('dlq-promote');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    await fixture.harness.manager.changeDelay(jobId(id), 60_000);
    await job.promote();
    expect(await fixture.queue.getJobState(id)).toBe('waiting');
  });

  test('changeDelay moves a retried job to delayed', async () => {
    const fixture = setup('dlq-change-delay');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    await job.changeDelay(60_000);
    expect(await fixture.queue.getJobState(id)).toBe('delayed');
  });

  test('changePriority updates the queued job priority', async () => {
    const fixture = setup('dlq-change-priority');
    const { job, id } = await createFailedDlqJob(
      fixture.harness,
      fixture.queue,
      fixture.queueName,
      { value: 1 }
    );
    retryDirect(fixture.harness, fixture.queueName, id);
    await job.changePriority({ priority: 17, lifo: true });
    expect((await fixture.queue.getJob(id))?.priority).toBe(17);
  });
});
