import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { checkJobTimeouts } from '../src/application/backgroundTasks';
import { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext } from '../src/application/types';
import { FailureReason } from '../src/domain/types/dlq';

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

async function eventually<T>(
  read: () => T | Promise<T>,
  accept: (value: T) => boolean
): Promise<T> {
  const deadline = Date.now() + 2_000;
  let value = await read();
  while (!accept(value) && Date.now() < deadline) {
    await Bun.sleep(10);
    value = await read();
  }
  if (!accept(value)) throw new Error('Timed out waiting for the expected state');
  return value;
}

test('a timed-out terminal job keeps the timeout DLQ reason across SQLite restart', async () => {
  const tempDir = mkdtempSync(join(tmpdir(), 'bunqueue-timeout-reason-'));
  const dataPath = join(tempDir, 'queue.db');
  const queue = 'timeout-reason';
  const config = {
    dataPath,
    cleanupIntervalMs: 100_000,
    jobTimeoutCheckMs: 10,
    dependencyCheckMs: 100_000,
    stallCheckMs: 100_000,
    dlqMaintenanceMs: 100_000,
  };
  let manager: QueueManager | null = new QueueManager(config);

  try {
    const job = await manager.push(queue, {
      data: { value: 1 },
      maxAttempts: 1,
      timeout: 1,
      durable: true,
    });
    expect((await manager.pull(queue))?.id).toBe(job.id);

    const entries = await eventually(
      () => manager!.getDlqEntries(queue),
      (current) => current.length === 1
    );
    expect(await manager.getJobState(job.id)).toBe('failed');
    expect(manager.getStats().dlq).toBe(1);
    expect(entries).toHaveLength(1);
    expect(entries[0].reason).toBe(FailureReason.Timeout);
    expect(entries[0].attempts.at(-1)?.reason).toBe(FailureReason.Timeout);
    expect(entries[0].error).toBe('Job timeout exceeded');

    manager.shutdown();
    manager = new QueueManager(config);
    const restored = manager.getDlqEntries(queue);
    expect(restored).toHaveLength(1);
    expect(restored[0].reason).toBe(FailureReason.Timeout);
    expect(restored[0].attempts.at(-1)?.reason).toBe(FailureReason.Timeout);
    expect(await manager.getJobState(job.id)).toBe('failed');
    expect(manager.getStats().dlq).toBe(1);
  } finally {
    manager?.shutdown();
    rmSync(tempDir, { recursive: true, force: true });
  }
});

test('two timed-out attempts retain timeout for the full DLQ history', async () => {
  const manager = new QueueManager({ jobTimeoutCheckMs: 100_000 });
  const queue = 'two-timeouts';

  try {
    const job = await manager.push(queue, {
      data: { value: 2 },
      maxAttempts: 2,
      timeout: 1,
      backoff: 0,
    });

    for (let attempt = 0; attempt < 2; attempt++) {
      const pulled = await eventually(
        () => manager.pull(queue),
        (current) => current !== null
      );
      expect(pulled?.id).toBe(job.id);
      await Bun.sleep(5);
      checkJobTimeouts(backgroundContext(manager));
      const state = await eventually(
        () => manager.getJobState(job.id),
        (state) => (attempt === 0 ? state === 'waiting' : state === 'failed')
      );
      expect(state).toBe(attempt === 0 ? 'waiting' : 'failed');
    }

    const [entry] = manager.getDlqEntries(queue);
    expect(entry.reason).toBe(FailureReason.Timeout);
    expect(entry.attempts.map((attempt) => attempt.reason)).toEqual([
      FailureReason.Timeout,
      FailureReason.Timeout,
    ]);
  } finally {
    manager.shutdown();
  }
});

test('a later processor failure is not contaminated by an earlier timeout', async () => {
  const manager = new QueueManager({ jobTimeoutCheckMs: 100_000 });
  const queue = 'timeout-then-processor-failure';

  try {
    const job = await manager.push(queue, {
      data: { value: 3 },
      maxAttempts: 2,
      timeout: 1,
      backoff: 0,
    });
    expect((await manager.pull(queue))?.id).toBe(job.id);
    await Bun.sleep(5);
    checkJobTimeouts(backgroundContext(manager));
    const waitingState = await eventually(
      () => manager.getJobState(job.id),
      (state) => state === 'waiting'
    );
    expect(waitingState).toBe('waiting');

    expect((await manager.pull(queue))?.id).toBe(job.id);
    await manager.fail(job.id, 'processor failed');

    const [entry] = manager.getDlqEntries(queue);
    expect(entry.reason).toBe(FailureReason.MaxAttemptsExceeded);
    expect(entry.attempts.map((attempt) => attempt.reason)).toEqual([
      FailureReason.Timeout,
      FailureReason.MaxAttemptsExceeded,
    ]);
  } finally {
    manager.shutdown();
  }
});
