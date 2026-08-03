import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { AckBatcher } from '../src/client/worker/ackBatcher';
import { ignoredAckIndices, outcomeWasApplied } from '../src/client/worker/ackOutcome';
import { FailureReason } from '../src/domain/types/dlq';
import type { JobId } from '../src/domain/types/job';
import { processingShardIndex } from '../src/shared/hash';
import type { RWLock } from '../src/shared/lock';

interface ManagerInternals {
  readonly processingLocks: RWLock[];
}

interface PausedWrite {
  readonly entered: Promise<void>;
  resume(): void;
}

/** Pause exactly the next processing-shard claim without holding the real lock. */
function pauseNextProcessingWrite(manager: QueueManager, id: JobId): PausedWrite {
  const lock = (manager as unknown as ManagerInternals).processingLocks[processingShardIndex(id)];
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

  return {
    entered,
    resume: () => resume(),
  };
}

describe('timeout outcome authority', () => {
  let manager: QueueManager | null = null;

  afterEach(() => {
    manager?.shutdown();
    manager = null;
  });

  test('a timeout claim that wins after ACK validation is reported as ignored', async () => {
    manager = new QueueManager();
    const queued = await manager.push('timeout-race', { data: { value: 1 }, maxAttempts: 1 });
    const pulled = await manager.pullWithLock('timeout-race', 'worker', 0, 30_000);
    expect(pulled.job?.id).toBe(queued.id);

    const paused = pauseNextProcessingWrite(manager, queued.id);
    const ack = manager.ack(queued.id, { late: true }, pulled.token ?? undefined);
    await paused.entered;
    await manager.failWithReason(queued.id, 'Job timeout exceeded', FailureReason.Timeout);
    paused.resume();

    await expect(ack).resolves.toEqual({ applied: false, reason: 'already-finalized' });
    expect(await manager.getJobState(queued.id)).toBe('failed');
    expect(manager.getResult(queued.id)).toBeUndefined();
  });

  test('a large mixed batch applies live generations and identifies the timeout winner', async () => {
    manager = new QueueManager();
    const deliveries: Array<{ id: JobId; token: string }> = [];
    for (let index = 0; index < 5; index++) {
      const queued = await manager.push('timeout-batch-race', {
        data: { index },
        maxAttempts: 1,
      });
      const pulled = await manager.pullWithLock('timeout-batch-race', 'worker', 0, 30_000);
      expect(pulled.job?.id).toBe(queued.id);
      expect(pulled.token).toBeString();
      deliveries.push({ id: queued.id, token: pulled.token as string });
    }

    const timedOut = deliveries[3];
    const paused = pauseNextProcessingWrite(manager, timedOut.id);
    const ack = manager.ackBatchWithResults(
      deliveries.map(({ id, token }, index) => ({ id, token, result: { index } }))
    );
    await paused.entered;
    await manager.failWithReason(timedOut.id, 'Job timeout exceeded', FailureReason.Timeout);
    paused.resume();

    await expect(ack).resolves.toEqual({ ignoredIds: [timedOut.id], ignoredIndices: [3] });
    expect(await manager.getJobState(timedOut.id)).toBe('failed');
    for (const delivery of deliveries.filter((entry) => entry !== timedOut)) {
      expect(await manager.getJobState(delivery.id)).toBe('completed');
    }
    expect(manager.getStats().totalCompleted).toBe(4n);
    expect(manager.getStats().totalFailed).toBe(1n);
  });

  test('old timeout tokens cannot disturb the current retry lease', async () => {
    manager = new QueueManager();
    const queued = await manager.push('timeout-retry-generation', {
      data: {},
      maxAttempts: 3,
      backoff: 0,
    });
    const first = await manager.pullWithLock('timeout-retry-generation', 'first', 0, 30_000);
    await manager.failWithReason(queued.id, 'first timeout', FailureReason.Timeout);
    const current = await manager.pullWithLock('timeout-retry-generation', 'current', 0, 30_000);
    expect(current.token).not.toBe(first.token);

    await expect(manager.ack(queued.id, 'late', first.token ?? undefined)).resolves.toEqual({
      applied: false,
      reason: 'already-finalized',
    });
    expect(manager.getLockInfo(queued.id)?.token).toBe(current.token);
    expect(await manager.getJobState(queued.id)).toBe('active');
    await expect(manager.ack(queued.id, 'wrong', 'wrong-token')).rejects.toThrow(
      'Invalid or expired lock token'
    );
    await expect(manager.ack(queued.id, 'missing')).rejects.toThrow('Lock token required');

    await expect(
      manager.ack(queued.id, 'current', current.token ?? undefined)
    ).resolves.toBeUndefined();
    expect(await manager.getJobState(queued.id)).toBe('completed');
    expect(manager.getResult(queued.id)).toBe('current');
  });

  test('multiple retired tokens for one ID remain generation-specific', async () => {
    manager = new QueueManager();
    const queued = await manager.push('timeout-token-history', {
      data: {},
      maxAttempts: 4,
      backoff: 0,
    });
    const first = await manager.pullWithLock('timeout-token-history', 'first', 0, 30_000);
    await manager.failWithReason(queued.id, 'first timeout', FailureReason.Timeout);
    const second = await manager.pullWithLock('timeout-token-history', 'second', 0, 30_000);
    await manager.failWithReason(queued.id, 'second timeout', FailureReason.Timeout);
    const current = await manager.pullWithLock('timeout-token-history', 'current', 0, 30_000);

    for (const token of [first.token, second.token]) {
      await expect(manager.ack(queued.id, 'late', token ?? undefined)).resolves.toEqual({
        applied: false,
        reason: 'already-finalized',
      });
      expect(manager.getLockInfo(queued.id)?.token).toBe(current.token);
    }
    await manager.ack(queued.id, 'accepted', current.token ?? undefined);
    expect(manager.getResult(queued.id)).toBe('accepted');
  });

  test('small batches preserve ignored positions and remove-on-complete evidence', async () => {
    manager = new QueueManager();
    const queue = 'timeout-small-batch';
    const jobs = await Promise.all(
      Array.from({ length: 3 }, (_, index) =>
        manager?.push(queue, { data: { index }, maxAttempts: 1 })
      )
    );
    const deliveries = [];
    for (let index = 0; index < jobs.length; index++) {
      deliveries.push(await manager.pullWithLock(queue, `worker-${index}`, 0, 30_000));
    }
    await manager.failWithReason(jobs[0]!.id, 'timeout zero', FailureReason.Timeout);
    await manager.failWithReason(jobs[2]!.id, 'timeout two', FailureReason.Timeout);

    const outcome = await manager.ackBatchWithResults([
      { id: jobs[0]!.id, result: 0, token: deliveries[0].token ?? undefined },
      {
        id: jobs[1]!.id,
        result: 1,
        token: deliveries[1].token ?? undefined,
        removeOnComplete: true,
      },
      { id: jobs[2]!.id, result: 2, token: deliveries[2].token ?? undefined },
    ]);

    expect(outcome).toEqual({
      ignoredIds: [jobs[0]!.id, jobs[2]!.id],
      ignoredIndices: [0, 2],
    });
    expect(await manager.getJobState(jobs[1]!.id)).toBe('unknown');
    expect(manager.getStats().totalCompleted).toBe(1n);
    expect(manager.getStats().totalFailed).toBe(2n);
  });

  test('the TCP ACK batcher resolves ignored and applied items without retrying', async () => {
    const batcher = new AckBatcher({
      batchSize: 2,
      interval: 1_000,
      embedded: false,
      maxRetries: 3,
      retryDelayMs: 1,
    });
    let sends = 0;
    batcher.setTcp({
      send: async () => {
        sends++;
        return { ok: true, data: { ignoredIds: ['retired'], ignoredIndices: [0] } };
      },
    });

    const retired = batcher.queue('retired', { late: true }, 'old-token');
    const current = batcher.queue('current', { accepted: true }, 'current-token');

    await expect(retired).resolves.toBe(false);
    await expect(current).resolves.toBe(true);
    expect(sends).toBe(1);
    batcher.stop();
  });

  test('duplicate IDs use ignored indices rather than suppressing the live generation', async () => {
    const batcher = new AckBatcher({ batchSize: 2, interval: 1_000, embedded: false });
    batcher.setTcp({
      send: async () => ({
        ok: true,
        data: { ignoredIds: ['same'], ignoredIndices: [0] },
      }),
    });

    const retired = batcher.queue('same', 'old', 'old-token');
    const current = batcher.queue('same', 'new', 'new-token');
    await expect(retired).resolves.toBe(false);
    await expect(current).resolves.toBe(true);
    batcher.stop();
  });

  test('malformed structured ACK evidence is rejected', () => {
    const batch = [{ id: 'known' }];
    expect(() => ignoredAckIndices({ ignoredIds: [1], ignoredIndices: [0] }, batch)).toThrow(
      'ignoredIds'
    );
    expect(() => ignoredAckIndices({ ignoredIds: ['known'], ignoredIndices: [2] }, batch)).toThrow(
      'ignoredIndices'
    );
    expect(() => ignoredAckIndices({ ignoredIds: ['other'], ignoredIndices: [0] }, batch)).toThrow(
      'Mismatched'
    );
    expect(outcomeWasApplied(undefined)).toBe(true);
    expect(outcomeWasApplied({ applied: false, reason: 'already-finalized' })).toBe(false);
    expect(() => outcomeWasApplied({ applied: 'false' })).toThrow('Invalid ACK response');
  });

  test('ACKB rejects ambiguous ignoredIds-only evidence', () => {
    expect(() => ignoredAckIndices({ ignoredIds: ['known'] }, [{ id: 'known' }])).toThrow(
      'ignoredIndices'
    );
  });
});
