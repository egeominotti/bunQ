/**
 * Executable proof for /guide/dlq/auto-retry/ ("Automatic DLQ Retry").
 *
 * The maintenance cadence defaults to 60s, so the timing claims run against a
 * real QueueManager tuned with a short `dlqMaintenanceMs`; the client-visible
 * contract is checked through the public Queue API in both runtimes.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../../src/application/queueManager';
import { jobId } from '../../src/domain/types/job';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;
const scratch: string[] = [];
const managers: QueueManager[] = [];

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
  for (const manager of managers.splice(0)) manager.shutdown();
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tunedManager(
  label: string,
  dlqMaintenanceMs = 40
): { manager: QueueManager; path: string } {
  const dir = mkdtempSync(join(tmpdir(), `bunqueue-docs-${label}-`));
  scratch.push(dir);
  const path = join(dir, 'queue.db');
  const manager = new QueueManager({ dataPath: path, dlqMaintenanceMs });
  managers.push(manager);
  return { manager, path };
}

function shutdownTrackedManager(manager: QueueManager): void {
  const index = managers.indexOf(manager);
  if (index >= 0) managers.splice(index, 1);
  manager.shutdown();
}

/** Push a job, take it, and fail it terminally so it lands in the DLQ. */
async function failIntoDlq(manager: QueueManager, queue: string, data: unknown): Promise<string> {
  const job = await manager.push(queue, { data, maxAttempts: 1, durable: true });
  const pulled = await manager.pull(queue, 1_000);
  if (!pulled) throw new Error('job was not delivered');
  await manager.fail(pulled.id, 'boom');
  return String(job.id);
}

describe('dlq guide · automatic retry [broker timing]', () => {
  test('a new entry gets nextRetryAt = enteredAt + autoRetryInterval', async () => {
    const { manager } = tunedManager('auto-retry-stamp');
    const queue = 'emails';
    manager.setDlqConfig(queue, {
      autoRetry: true,
      autoRetryInterval: 60_000,
      maxAutoRetries: 3,
    });

    const id = await failIntoDlq(manager, queue, { key: 'value' });
    const entry = manager.getDlqEntries(queue).find((item) => String(item.job.id) === id);

    expect(entry).toBeDefined();
    expect(entry?.retryCount).toBe(0);
    expect(entry?.nextRetryAt).toBe((entry?.enteredAt ?? 0) + 60_000);
  }, 30_000);

  test('maintenance re-queues a due entry and advances its retry counter', async () => {
    const { manager } = tunedManager('auto-retry-dispatch');
    const queue = 'emails';
    manager.setDlqConfig(queue, { autoRetry: true, autoRetryInterval: 30, maxAutoRetries: 3 });

    const id = await failIntoDlq(manager, queue, { key: 'value' });
    await waitUntil(
      () => manager.getDlqEntries(queue).length === 0,
      'the due entry to leave the DLQ',
      20_000
    );

    // It is back in the queue, with its normal attempt counter reset.
    const requeued = await manager.pull(queue, 2_000);
    expect(requeued).not.toBeNull();
    expect(String(requeued?.id)).toBe(id);
    expect(requeued?.attempts).toBe(0);
    expect(requeued?.stallCount).toBe(0);

    await manager.fail(requeued!.id, 'boom again');
    await waitUntil(
      () => manager.getDlqEntries(queue).length === 1,
      'the second failure to be recorded',
      20_000
    );
    const entry = manager.getDlqEntries(queue)[0];
    expect(entry.retryCount).toBe(1);
    expect(entry.lastRetryAt).toBeNumber();
  }, 40_000);

  test('the failure history survives an automatic redelivery', async () => {
    const { manager } = tunedManager('auto-retry-history');
    const queue = 'emails';
    manager.setDlqConfig(queue, { autoRetry: true, autoRetryInterval: 30, maxAutoRetries: 3 });

    await failIntoDlq(manager, queue, { key: 'value' });
    await waitUntil(() => manager.getDlqEntries(queue).length === 0, 'the first dispatch', 20_000);
    const requeued = await manager.pull(queue, 2_000);
    await manager.fail(requeued!.id, 'second failure');
    await waitUntil(
      () => manager.getDlqEntries(queue).length === 1,
      'the entry to come back',
      20_000
    );

    const entry = manager.getDlqEntries(queue)[0];
    expect(entry.attempts.length).toBeGreaterThanOrEqual(2);
    expect(entry.attempts[0].error).toContain('boom');
    expect(entry.attempts.at(-1)?.error).toContain('second failure');
  }, 40_000);

  test('the retry delay doubles and stops at maxAutoRetries', async () => {
    const { manager } = tunedManager('auto-retry-backoff');
    const queue = 'emails';
    const base = 40;
    manager.setDlqConfig(queue, { autoRetry: true, autoRetryInterval: base, maxAutoRetries: 2 });

    await failIntoDlq(manager, queue, { key: 'value' });
    const delays: number[] = [];

    for (let round = 1; round <= 2; round++) {
      await waitUntil(() => manager.getDlqEntries(queue).length === 0, `dispatch ${round}`, 20_000);
      const requeued = await manager.pull(queue, 2_000);
      expect(requeued).not.toBeNull();
      await manager.fail(requeued!.id, `failure ${round}`);
      await waitUntil(() => manager.getDlqEntries(queue).length === 1, `re-entry ${round}`, 20_000);
      const entry = manager.getDlqEntries(queue)[0];
      expect(entry.retryCount).toBe(round);
      if (entry.nextRetryAt !== null) delays.push(entry.nextRetryAt - entry.lastRetryAt!);
    }

    const finalEntry = manager.getDlqEntries(queue)[0];
    // retryCount reached maxAutoRetries: automatic redelivery is over.
    expect(finalEntry.retryCount).toBe(2);
    expect(finalEntry.nextRetryAt).toBeNull();
    // The first re-scheduled delay follows autoRetryInterval * 2^(retryCount-1).
    expect(delays[0]).toBe(base);
    await Bun.sleep(300);
    expect(manager.getDlqEntries(queue).length).toBe(1);
  }, 60_000);

  test('a manual retry clears the automatic chain and the normal counters', async () => {
    const { manager } = tunedManager('auto-retry-manual', 60_000);
    const queue = 'emails';
    manager.setDlqConfig(queue, {
      autoRetry: true,
      autoRetryInterval: 3_600_000,
      maxAutoRetries: 3,
    });

    const id = await failIntoDlq(manager, queue, { key: 'value' });
    expect(manager.getDlqEntries(queue)[0].nextRetryAt).toBeGreaterThan(Date.now());

    expect(manager.retryDlq(queue, jobId(id))).toBe(1);

    const requeued = await manager.pull(queue, 2_000);
    expect(String(requeued?.id)).toBe(id);
    expect(requeued?.attempts).toBe(0);
    expect(requeued?.stallCount).toBe(0);
    await manager.fail(requeued!.id, 'boom again');
    await waitUntil(
      () => manager.getDlqEntries(queue).length === 1,
      'the entry to come back',
      20_000
    );
    // The operator-directed retry started a new generation, so the automatic
    // counter is back to zero.
    expect(manager.getDlqEntries(queue)[0].retryCount).toBe(0);
  }, 40_000);

  test('policy, counter, history and timestamps survive a broker restart', async () => {
    const { manager, path } = tunedManager('auto-retry-restart', 60_000);
    const queue = 'emails';
    manager.setDlqConfig(queue, {
      autoRetry: true,
      autoRetryInterval: 3_600_000,
      maxAutoRetries: 5,
    });
    const id = await failIntoDlq(manager, queue, { key: 'value' });
    const before = manager.getDlqEntries(queue).find((item) => String(item.job.id) === id);
    await Bun.sleep(150);
    shutdownTrackedManager(manager);

    const restarted = new QueueManager({ dataPath: path, dlqMaintenanceMs: 60_000 });
    managers.push(restarted);
    await waitUntil(
      () => restarted.getDlqEntries(queue).length === 1,
      'the DLQ entry to be restored',
      20_000
    );

    const after = restarted.getDlqEntries(queue)[0];
    expect(String(after.job.id)).toBe(id);
    expect(after.enteredAt).toBe(before?.enteredAt);
    expect(after.nextRetryAt).toBe(before?.nextRetryAt ?? null);
    expect(after.expiresAt).toBe(before?.expiresAt ?? null);
    expect(after.retryCount).toBe(before?.retryCount ?? 0);
    expect(after.attempts.length).toBe(before?.attempts.length ?? 0);
    expect(restarted.getDlqConfig(queue)).toMatchObject({
      autoRetry: true,
      autoRetryInterval: 3_600_000,
      maxAutoRetries: 5,
    });
  }, 40_000);
});

for (const mode of MODES) {
  describe(`dlq guide · automatic retry [client, ${mode}]`, () => {
    test('the auto-retry policy is visible through the Queue API', async () => {
      harness = await startHarness('dlq-auto-retry', mode);
      const queue = harness.queue('emails');

      await queue.setDlqConfigAsync({
        autoRetry: true,
        autoRetryInterval: 60_000,
        maxAutoRetries: 3,
      });
      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry?.retryCount).toBe(0);
      expect(entry?.nextRetryAt).toBe((entry?.enteredAt ?? 0) + 60_000);
      // pendingRetry counts entries whose retry time is already due.
      expect((await queue.getDlqStatsAsync()).pendingRetry).toBe(0);
    }, 40_000);

    test('pendingRetry counts an entry once its retry time is due', async () => {
      harness = await startHarness('dlq-auto-retry', mode);
      const queue = harness.queue('emails');

      await queue.setDlqConfigAsync({ autoRetry: true, autoRetryInterval: 50, maxAutoRetries: 3 });
      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      // Maintenance runs every 60s, so the due entry is still listed.
      await waitUntil(
        async () => (await queue.getDlqStatsAsync()).pendingRetry === 1,
        'the entry to become due for auto-retry',
        20_000
      );
      expect(await queue.getDlqAsync({ retriable: true })).toHaveLength(1);
    }, 40_000);

    test('autoRetry disabled leaves nextRetryAt null', async () => {
      harness = await startHarness('dlq-auto-retry', mode);
      const queue = harness.queue('emails');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', {}, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync()).find((item) => item.job.id === job.id);
      expect(entry?.nextRetryAt).toBeNull();
      expect((await queue.getDlqStatsAsync()).pendingRetry).toBe(0);
    }, 40_000);
  });
}
