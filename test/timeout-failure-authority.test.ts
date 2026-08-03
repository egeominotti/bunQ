import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { FailureReason } from '../src/domain/types/dlq';
import type { JobId } from '../src/domain/types/job';
import { processingShardIndex } from '../src/shared/hash';
import type { RWLock } from '../src/shared/lock';

function pauseNextProcessingWrite(
  manager: QueueManager,
  id: JobId
): { entered: Promise<void>; resume(): void } {
  const locks = (manager as unknown as { processingLocks: RWLock[] }).processingLocks;
  const lock = locks[processingShardIndex(id)];
  const original = lock.acquireWrite.bind(lock);
  let markEntered!: () => void;
  let resume!: () => void;
  const entered = new Promise<void>((resolve) => {
    markEntered = resolve;
  });
  lock.acquireWrite = (timeoutMs?: number) => {
    lock.acquireWrite = original;
    markEntered();
    return new Promise((resolve, reject) => {
      resume = () => void original(timeoutMs).then(resolve, reject);
    });
  };
  return { entered, resume: () => resume() };
}

describe('timeout failure authority', () => {
  let manager: QueueManager | null = null;

  afterEach(() => {
    manager?.shutdown();
    manager = null;
  });

  test('a timeout claim that wins after FAIL validation is reported as ignored', async () => {
    manager = new QueueManager();
    const queued = await manager.push('timeout-fail-race', { data: {}, maxAttempts: 1 });
    const pulled = await manager.pullWithLock('timeout-fail-race', 'worker', 0, 30_000);
    const paused = pauseNextProcessingWrite(manager, queued.id);
    const failure = manager.fail(queued.id, 'late processor failure', pulled.token ?? undefined);
    await paused.entered;
    await manager.failWithReason(queued.id, 'Job timeout exceeded', FailureReason.Timeout);
    paused.resume();

    await expect(failure).resolves.toEqual({ applied: false, reason: 'already-finalized' });
    expect(await manager.getJobState(queued.id)).toBe('failed');
    expect(manager.getStats().totalFailed).toBe(1n);
  });

  test('an old timeout token is ignored without weakening the current FAIL lease', async () => {
    manager = new QueueManager();
    const queued = await manager.push('timeout-fail-retry', {
      data: {},
      maxAttempts: 2,
      backoff: 0,
    });
    const retired = await manager.pullWithLock('timeout-fail-retry', 'retired', 0, 30_000);
    await manager.failWithReason(queued.id, 'Job timeout exceeded', FailureReason.Timeout);
    const current = await manager.pullWithLock('timeout-fail-retry', 'current', 0, 30_000);

    await expect(
      manager.fail(queued.id, 'late processor failure', retired.token ?? undefined)
    ).resolves.toEqual({ applied: false, reason: 'already-finalized' });
    expect(manager.getLockInfo(queued.id)?.token).toBe(current.token);
    await expect(manager.fail(queued.id, 'wrong', 'wrong-token')).rejects.toThrow(
      'Invalid or expired lock token'
    );
    await expect(manager.fail(queued.id, 'missing')).rejects.toThrow('Lock token required');

    await expect(
      manager.fail(queued.id, 'current processor failure', current.token ?? undefined)
    ).resolves.toBeUndefined();
    expect(await manager.getJobState(queued.id)).toBe('failed');
    expect(manager.getStats().totalFailed).toBe(1n);
  });
});
