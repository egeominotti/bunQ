import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { MAX_TIMER_DELAY_MS, timeoutTimerDelay } from '../src/application/background/timeouts';
import type { JobId } from '../src/domain/types/job';

let manager: QueueManager | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
});

function pendingTimeouts(value: QueueManager): number {
  return (
    value as unknown as {
      timeoutScheduler: { pendingCount: number };
    }
  ).timeoutScheduler.pendingCount;
}

async function waitForState(id: JobId, state: string, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await manager!.getJobState(id)) === state) return;
    await Bun.sleep(5);
  }
  expect(await manager!.getJobState(id)).toBe(state);
}

describe('job timeout deadline scheduler', () => {
  test('bounds timers above the signed 32-bit delay instead of overflowing', () => {
    const now = 1_000;
    expect(timeoutTimerDelay(now + MAX_TIMER_DELAY_MS + 60_000, now)).toBe(MAX_TIMER_DELAY_MS);
    expect(timeoutTimerDelay(now + 250, now)).toBe(250);
    expect(timeoutTimerDelay(now - 1, now)).toBe(1);
  });

  test('creates the lease before arming an immediate locked timeout', async () => {
    manager = new QueueManager();
    const job = await manager.push('timeout-lock-race', {
      data: {},
      maxAttempts: 1,
      timeout: 1,
    });
    const delivery = await manager.pullWithLock('timeout-lock-race', 'worker', 0, 60_000);

    expect(delivery.job?.id).toBe(job.id);
    expect(delivery.token).toBeString();
    expect(manager.getLockInfo(job.id)?.token).toBe(delivery.token);
    await waitForState(job.id, 'failed');
    expect(pendingTimeouts(manager)).toBe(0);
  });

  test('cancels long deadlines on ACK, manual requeue, and shutdown', async () => {
    manager = new QueueManager();
    const completed = await manager.push('timeout-cleanup', { data: {}, timeout: 86_400_000 });
    const first = await manager.pullWithLock('timeout-cleanup', 'worker-a');
    expect(pendingTimeouts(manager)).toBe(1);
    await manager.ack(completed.id, true, first.token!);
    expect(pendingTimeouts(manager)).toBe(0);

    const requeued = await manager.push('timeout-cleanup', { data: {}, timeout: 86_400_000 });
    const second = await manager.pullWithLock('timeout-cleanup', 'worker-b');
    expect(pendingTimeouts(manager)).toBe(1);
    expect(await manager.moveActiveToWait(requeued.id, second.token!)).toBe(true);
    expect(pendingTimeouts(manager)).toBe(0);

    const active = await manager.pull('timeout-cleanup');
    expect(active?.id).toBe(requeued.id);
    expect(pendingTimeouts(manager)).toBe(1);
    manager.shutdown();
    expect(pendingTimeouts(manager)).toBe(0);
    manager = null;
  });

  test('a recycled custom ID is not failed by the previous generation deadline', async () => {
    manager = new QueueManager();
    const first = await manager.push('timeout-generation', {
      customId: 'recycled-timeout-id',
      data: { generation: 1 },
      maxAttempts: 1,
      timeout: 30,
    });
    await manager.pull('timeout-generation');
    await waitForState(first.id, 'failed');

    const second = await manager.push('timeout-generation', {
      customId: 'recycled-timeout-id',
      data: { generation: 2 },
      maxAttempts: 1,
      timeout: 250,
    });
    expect(second.id).toBe(first.id);
    await manager.pull('timeout-generation');
    await Bun.sleep(80);

    expect(await manager.getJobState(second.id)).toBe('active');
    expect(pendingTimeouts(manager)).toBe(1);
  });
});
