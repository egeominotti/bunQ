import { afterEach, describe, expect, test } from 'bun:test';
import type { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';
import { DelayedError } from '../src/client/errors';
import { TcpConnectionPool } from '../src/client/tcpPool';
import {
  type CoreE2eHarness,
  MODES,
  type Mode,
  closeHarness,
  startHarness,
  waitForState,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

interface Lease {
  id: string;
  token: string;
}

interface TokenTransport {
  manager: QueueManager;
  pullLocked(queue: string): Promise<Lease>;
  pullBatchLocked(queue: string, count: number): Promise<Lease[]>;
  pullUnlocked(queue: string): Promise<string>;
  ack(id: string, token?: string): Promise<{ ok: boolean; error?: string }>;
  ackBatch(
    ids: string[],
    tokens?: string[],
    results?: unknown[]
  ): Promise<{ ok: boolean; error?: string }>;
  fail(id: string, token?: string): Promise<{ ok: boolean; error?: string }>;
  discard(id: string, token?: string): Promise<{ ok: boolean; error?: string }>;
}

function requiredLease(id: unknown, token: unknown): Lease {
  if (typeof id !== 'string' || typeof token !== 'string' || token.length === 0) {
    throw new Error('Expected a locked processing lease');
  }
  return { id, token };
}

async function tokenTransport(active: CoreE2eHarness, mode: Mode): Promise<TokenTransport> {
  const manager = active.brokerManager();
  if (mode === 'embedded') {
    return {
      manager,
      async pullLocked(queue) {
        const lease = await manager.pullWithLock(queue, 'token-owner', 0, 60_000);
        return requiredLease(lease.job?.id, lease.token);
      },
      async pullBatchLocked(queue, count) {
        const lease = await manager.pullBatchWithLock(queue, count, 'token-owner', 0, 60_000);
        return lease.jobs.map((job, index) => requiredLease(job.id, lease.tokens[index]));
      },
      async pullUnlocked(queue) {
        const job = await manager.pull(queue, 0);
        if (!job) throw new Error('Expected an unlocked active job');
        return String(job.id);
      },
      async ack(id, token) {
        try {
          await manager.ack(jobId(id), { accepted: true }, token);
          return { ok: true };
        } catch (error) {
          return { ok: false, error: String(error) };
        }
      },
      async ackBatch(ids, tokens, results) {
        try {
          if (results?.length === ids.length) {
            await manager.ackBatchWithResults(
              ids.map((id, index) => ({
                id: jobId(id),
                result: results[index],
                token: tokens?.[index],
              }))
            );
          } else {
            await manager.ackBatch(ids.map(jobId), tokens);
          }
          return { ok: true };
        } catch (error) {
          return { ok: false, error: String(error) };
        }
      },
      async fail(id, token) {
        try {
          await manager.fail(jobId(id), 'expected token test failure', token);
          return { ok: true };
        } catch (error) {
          return { ok: false, error: String(error) };
        }
      },
      async discard(id, token) {
        try {
          const discardWithToken = manager.discard as unknown as (
            target: ReturnType<typeof jobId>,
            leaseToken?: string
          ) => Promise<boolean>;
          return { ok: await discardWithToken.call(manager, jobId(id), token) };
        } catch (error) {
          return { ok: false, error: String(error) };
        }
      },
    };
  }

  const pool = new TcpConnectionPool(active.connection());
  active.addCleanup(() => pool.close());
  await pool.connect();
  return {
    manager,
    async pullLocked(queue) {
      const response = await pool.send({
        cmd: 'PULL',
        queue,
        owner: 'token-owner',
        lockTtl: 60_000,
        timeout: 0,
      });
      return requiredLease((response.job as { id?: unknown } | null)?.id, response.token);
    },
    async pullBatchLocked(queue, count) {
      const response = await pool.send({
        cmd: 'PULLB',
        queue,
        count,
        owner: 'token-owner',
        lockTtl: 60_000,
        timeout: 0,
      });
      const jobs = (response.jobs as Array<{ id?: unknown }> | undefined) ?? [];
      const tokens = (response.tokens as unknown[] | undefined) ?? [];
      return jobs.map((job, index) => requiredLease(job.id, tokens[index]));
    },
    async pullUnlocked(queue) {
      const response = await pool.send({ cmd: 'PULL', queue, timeout: 0, detach: true });
      const id = (response.job as { id?: unknown } | null)?.id;
      if (typeof id !== 'string') throw new Error('Expected an unlocked active job');
      return id;
    },
    async ack(id, token) {
      const response = await pool.send({
        cmd: 'ACK',
        id,
        result: { accepted: true },
        ...(token === undefined ? {} : { token }),
      });
      return { ok: response.ok === true, error: response.error as string | undefined };
    },
    async ackBatch(ids, tokens, results) {
      const response = await pool.send({
        cmd: 'ACKB',
        ids,
        ...(tokens === undefined ? {} : { tokens }),
        ...(results === undefined ? {} : { results }),
      });
      return { ok: response.ok === true, error: response.error as string | undefined };
    },
    async fail(id, token) {
      const response = await pool.send({
        cmd: 'FAIL',
        id,
        error: 'expected token test failure',
        ...(token === undefined ? {} : { token }),
      });
      return { ok: response.ok === true, error: response.error as string | undefined };
    },
    async discard(id, token) {
      const response = await pool.send({
        cmd: 'Discard',
        id,
        ...(token === undefined ? {} : { token }),
      } as Parameters<TcpConnectionPool['send']>[0]);
      return { ok: response.ok === true, error: response.error as string | undefined };
    },
  };
}

function expectTokenError(response: { ok: boolean; error?: string }): void {
  expect(response.ok).toBe(false);
  expect(response.error?.toLowerCase()).toContain('token');
}

async function expectActive(manager: QueueManager, id: string): Promise<void> {
  expect(await manager.getJobState(jobId(id))).toBe('active');
  expect(manager.getLockInfo(jobId(id))).not.toBeNull();
}

for (const mode of MODES) {
  describe(`lock token enforcement [${mode}]`, () => {
    test('locked transitions require the matching token; unlocked transitions do not', async () => {
      harness = await startHarness('lock-token', mode);
      const queue = harness.queue(`token-${mode}`);
      const transport = await tokenTransport(harness, mode);

      const ackJob = await queue.add('ack', {}, { durable: true });
      const ackLease = await transport.pullLocked(queue.name);
      expect(ackLease.id).toBe(ackJob.id);
      expectTokenError(await transport.ack(ackLease.id));
      await expectActive(transport.manager, ackLease.id);
      expectTokenError(await transport.ack(ackLease.id, 'wrong-token'));
      await expectActive(transport.manager, ackLease.id);
      expect((await transport.ack(ackLease.id, ackLease.token)).ok).toBe(true);
      expect(await queue.getJobState(ackLease.id)).toBe('completed');

      const failedJob = await queue.add('fail', {}, { attempts: 1, durable: true });
      const failedLease = await transport.pullLocked(queue.name);
      expect(failedLease.id).toBe(failedJob.id);
      expectTokenError(await transport.fail(failedLease.id));
      await expectActive(transport.manager, failedLease.id);
      expectTokenError(await transport.fail(failedLease.id, 'wrong-token'));
      await expectActive(transport.manager, failedLease.id);
      expect((await transport.fail(failedLease.id, failedLease.token)).ok).toBe(true);
      expect(await queue.getJobState(failedLease.id)).toBe('failed');

      const discardedJob = await queue.add('discard', {}, { attempts: 1, durable: true });
      const discardedLease = await transport.pullLocked(queue.name);
      expect(discardedLease.id).toBe(discardedJob.id);
      expectTokenError(await transport.discard(discardedLease.id));
      await expectActive(transport.manager, discardedLease.id);
      expectTokenError(await transport.discard(discardedLease.id, 'wrong-token'));
      await expectActive(transport.manager, discardedLease.id);
      expect((await transport.discard(discardedLease.id, discardedLease.token)).ok).toBe(true);
      expect(await queue.getJobState(discardedLease.id)).toBe('failed');

      const batchJobs = await queue.addBulk([
        { name: 'batch-a', data: {}, opts: { durable: true } },
        { name: 'batch-b', data: {}, opts: { durable: true } },
      ]);
      const batchLeases = await transport.pullBatchLocked(queue.name, 2);
      expect(batchLeases.map((lease) => lease.id)).toEqual(batchJobs.map((job) => job.id));
      expectTokenError(await transport.ackBatch(batchLeases.map((lease) => lease.id)));
      for (const lease of batchLeases) await expectActive(transport.manager, lease.id);
      expectTokenError(
        await transport.ackBatch(
          batchLeases.map((lease) => lease.id),
          [batchLeases[0].token, 'wrong-token']
        )
      );
      for (const lease of batchLeases) await expectActive(transport.manager, lease.id);
      expectTokenError(
        await transport.ackBatch(
          batchLeases.map((lease) => lease.id),
          undefined,
          [{ index: 0 }, { index: 1 }]
        )
      );
      for (const lease of batchLeases) await expectActive(transport.manager, lease.id);
      expect(
        (
          await transport.ackBatch(
            batchLeases.map((lease) => lease.id),
            batchLeases.map((lease) => lease.token),
            [{ index: 0 }, { index: 1 }]
          )
        ).ok
      ).toBe(true);
      expect(transport.manager.getResult(jobId(batchLeases[0].id))).toEqual({ index: 0 });
      expect(transport.manager.getResult(jobId(batchLeases[1].id))).toEqual({ index: 1 });

      const adminJob = await queue.add('administrative', {}, { durable: true });
      const unlockedId = await transport.pullUnlocked(queue.name);
      expect(unlockedId).toBe(adminJob.id);
      expect(transport.manager.getLockInfo(jobId(unlockedId))).toBeNull();
      expect((await transport.ack(unlockedId)).ok).toBe(true);
      expect(await queue.getJobState(unlockedId)).toBe('completed');

      const adminDiscard = await queue.add('administrative-discard', {}, { durable: true });
      const unlockedDiscardId = await transport.pullUnlocked(queue.name);
      expect(unlockedDiscardId).toBe(adminDiscard.id);
      expect((await transport.discard(unlockedDiscardId)).ok).toBe(true);
      expect(await queue.getJobState(unlockedDiscardId)).toBe('failed');
    }, 45_000);

    test('every public active-state move forwards and enforces its token', async () => {
      harness = await startHarness('lock-token-public-moves', mode);
      const transport = await tokenTransport(harness, mode);

      const completedQueue = harness.queue(`token-public-completed-${mode}`);
      const completed = await completedQueue.add('completed', {}, { durable: true });
      const completedLease = await transport.pullLocked(completedQueue.name);
      await expect(completed.moveToCompleted('done')).rejects.toThrow(/token/i);
      await expectActive(transport.manager, completed.id);
      await expect(completed.moveToCompleted('wrong', 'wrong-token')).rejects.toThrow(/token/i);
      await expectActive(transport.manager, completed.id);
      await expect(completed.moveToCompleted('done', completedLease.token)).resolves.toBeNull();
      expect(await completedQueue.getJobState(completed.id)).toBe('completed');
      expect(transport.manager.getLockInfo(jobId(completed.id))).toBeNull();

      const failedQueue = harness.queue(`token-public-failed-${mode}`);
      const failed = await failedQueue.add('failed', {}, { attempts: 1, durable: true });
      const failedLease = await transport.pullLocked(failedQueue.name);
      await expect(failed.moveToFailed(new Error('missing token'))).rejects.toThrow(/token/i);
      await expectActive(transport.manager, failed.id);
      await expect(failed.moveToFailed(new Error('wrong token'), 'wrong-token')).rejects.toThrow(
        /token/i
      );
      await expectActive(transport.manager, failed.id);
      await expect(
        failed.moveToFailed(new Error('expected'), failedLease.token)
      ).resolves.toBeUndefined();
      expect(await failedQueue.getJobState(failed.id)).toBe('failed');
      expect(transport.manager.getLockInfo(jobId(failed.id))).toBeNull();

      const waitingQueue = harness.queue(`token-public-waiting-${mode}`);
      const waiting = await waitingQueue.add('waiting', {}, { durable: true });
      const waitingLease = await transport.pullLocked(waitingQueue.name);
      await expect(waiting.moveToWait()).rejects.toThrow(/token/i);
      await expectActive(transport.manager, waiting.id);
      await expect(waiting.moveToWait('wrong-token')).rejects.toThrow(/token/i);
      await expectActive(transport.manager, waiting.id);
      await expect(waiting.moveToWait(waitingLease.token)).resolves.toBe(true);
      expect(await waitingQueue.getJobState(waiting.id)).toBe('waiting');
      expect(transport.manager.getLockInfo(jobId(waiting.id))).toBeNull();

      const delayedQueue = harness.queue(`token-public-delayed-${mode}`);
      const delayed = await delayedQueue.add('delayed', {}, { durable: true });
      const delayedLease = await transport.pullLocked(delayedQueue.name);
      const target = Date.now() + 60_000;
      await expect(delayed.moveToDelayed(target)).rejects.toThrow(/token/i);
      await expectActive(transport.manager, delayed.id);
      await expect(delayed.moveToDelayed(target, 'wrong-token')).rejects.toThrow(/token/i);
      await expectActive(transport.manager, delayed.id);
      await expect(delayed.moveToDelayed(target, delayedLease.token)).resolves.toBeUndefined();
      expect(await delayedQueue.getJobState(delayed.id)).toBe('delayed');
      expect(transport.manager.getLockInfo(jobId(delayed.id))).toBeNull();

      const waitingChildrenQueue = harness.queue(`token-public-waiting-children-${mode}`);
      const waitingChildren = await waitingChildrenQueue.add(
        'waiting-children',
        {},
        {
          durable: true,
        }
      );
      const waitingChildrenLease = await transport.pullLocked(waitingChildrenQueue.name);
      await expect(waitingChildren.moveToWaitingChildren()).rejects.toThrow(/token/i);
      await expectActive(transport.manager, waitingChildren.id);
      await expect(waitingChildren.moveToWaitingChildren('wrong-token')).rejects.toThrow(/token/i);
      await expectActive(transport.manager, waitingChildren.id);
      await expect(waitingChildren.moveToWaitingChildren(waitingChildrenLease.token)).resolves.toBe(
        true
      );
      expect(await waitingChildrenQueue.getJobState(waitingChildren.id)).toBe('waiting-children');
      expect(transport.manager.getLockInfo(jobId(waitingChildren.id))).toBeNull();
    }, 60_000);

    test('public active-state moves remain administrative when no lock exists', async () => {
      harness = await startHarness('lock-token-admin-moves', mode);
      const transport = await tokenTransport(harness, mode);

      const completedQueue = harness.queue(`token-admin-completed-${mode}`);
      const completed = await completedQueue.add('completed', {}, { durable: true });
      expect(await transport.pullUnlocked(completedQueue.name)).toBe(completed.id);
      await expect(completed.moveToCompleted('done')).resolves.toBeNull();

      const failedQueue = harness.queue(`token-admin-failed-${mode}`);
      const failed = await failedQueue.add('failed', {}, { attempts: 1, durable: true });
      expect(await transport.pullUnlocked(failedQueue.name)).toBe(failed.id);
      await expect(failed.moveToFailed(new Error('administrative'))).resolves.toBeUndefined();

      const waitingQueue = harness.queue(`token-admin-waiting-${mode}`);
      const waiting = await waitingQueue.add('waiting', {}, { durable: true });
      expect(await transport.pullUnlocked(waitingQueue.name)).toBe(waiting.id);
      await expect(waiting.moveToWait()).resolves.toBe(true);

      const delayedQueue = harness.queue(`token-admin-delayed-${mode}`);
      const delayed = await delayedQueue.add('delayed', {}, { durable: true });
      expect(await transport.pullUnlocked(delayedQueue.name)).toBe(delayed.id);
      await expect(delayed.moveToDelayed(Date.now() + 60_000)).resolves.toBeUndefined();

      const waitingChildrenQueue = harness.queue(`token-admin-waiting-children-${mode}`);
      const waitingChildren = await waitingChildrenQueue.add(
        'waiting-children',
        {},
        {
          durable: true,
        }
      );
      expect(await transport.pullUnlocked(waitingChildrenQueue.name)).toBe(waitingChildren.id);
      await expect(waitingChildren.moveToWaitingChildren()).resolves.toBe(true);
    }, 60_000);

    test('a stale delivery token cannot discard a newer active generation', async () => {
      harness = await startHarness('lock-token-stale-discard', mode);
      const queue = harness.queue(`token-stale-discard-${mode}`);
      const transport = await tokenTransport(harness, mode);

      const job = await queue.add('stale-discard', {}, { attempts: 1, durable: true });
      const firstLease = await transport.pullLocked(queue.name);
      expect(firstLease.id).toBe(job.id);
      expect(await transport.manager.moveActiveToWait(jobId(job.id), firstLease.token)).toBe(true);

      const currentLease = await transport.pullLocked(queue.name);
      expect(currentLease.id).toBe(job.id);
      expect(currentLease.token).not.toBe(firstLease.token);
      expectTokenError(await transport.discard(job.id, firstLease.token));
      await expectActive(transport.manager, job.id);
      expect(transport.manager.getLockInfo(jobId(job.id))?.token).toBe(currentLease.token);

      expect((await transport.discard(job.id, currentLease.token)).ok).toBe(true);
      expect(await queue.getJobState(job.id)).toBe('failed');
    }, 20_000);

    test('DelayedError forwards the worker lease token to the delayed transition', async () => {
      harness = await startHarness('lock-token-delayed-error', mode);
      const queue = harness.queue(`token-delayed-error-${mode}`);
      const worker = harness.worker(
        queue.name,
        () => {
          throw new DelayedError('retry later');
        },
        { concurrency: 1, lockDuration: 60_000 }
      );
      worker.on('error', () => {
        // The state assertion below remains authoritative for this recovery path.
      });

      const job = await queue.add('delayed-error', {}, { backoff: 60_000, durable: true });
      await waitForState(queue, job.id, 'delayed', 10_000);
      expect(harness.brokerManager().getLockInfo(jobId(job.id))).toBeNull();
    }, 20_000);
  });
}
