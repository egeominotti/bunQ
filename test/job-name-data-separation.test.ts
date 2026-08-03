import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SandboxedWorker, type Job } from '../src/client';
import { TcpConnectionPool } from '../src/client/tcpPool';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from './docs-guide-support';

interface NamedData {
  name: string;
  marker: string;
}

let harness: CoreE2eHarness | null = null;
const sandboxedWorkers: SandboxedWorker[] = [];
const scratchDirs: string[] = [];

afterEach(async () => {
  for (const worker of sandboxedWorkers.splice(0)) {
    await worker.stop(true).catch(() => undefined);
  }
  await closeHarness(harness);
  harness = null;
  for (const directory of scratchDirs.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function expectPayload(
  job: Job<NamedData> | null | undefined,
  name: string,
  data: NamedData
): void {
  expect(job?.name).toBe(name);
  expect(job?.data).toEqual(data);
}

function processorFile(): string {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-name-data-'));
  scratchDirs.push(directory);
  const path = join(directory, 'processor.ts');
  writeFileSync(
    path,
    `export default async (job: { name: string; data: unknown }) => ({
      name: job.name,
      data: job.data,
    });`
  );
  return path;
}

for (const mode of MODES) {
  describe(`job name/data separation [${mode}]`, () => {
    test('add, addBulk and every pending-state query preserve a user data.name', async () => {
      harness = await startHarness('name-data-pending', mode);
      const queue = harness.queue<NamedData>('pending');
      const singleData = { name: 'user-single', marker: 'single' };
      const bulkData = { name: 'user-bulk', marker: 'bulk' };

      const single = await queue.add('single-job', singleData, { durable: true });
      const [bulk] = await queue.addBulk([
        { name: 'bulk-job', data: bulkData, opts: { durable: true } },
      ]);
      expectPayload(single, 'single-job', singleData);
      expectPayload(bulk, 'bulk-job', bulkData);
      expectPayload(await queue.getJob(single.id), 'single-job', singleData);

      const delayedData = { name: 'user-delayed', marker: 'delayed' };
      const delayedQueue = harness.queue<NamedData>('delayed');
      const delayed = await delayedQueue.add('delayed-job', delayedData, {
        delay: 60_000,
        durable: true,
      });
      expectPayload(
        (await delayedQueue.getDelayedAsync(0, -1)).find((job) => job.id === delayed.id),
        'delayed-job',
        delayedData
      );
      if (mode === 'embedded') {
        expectPayload(
          delayedQueue.getDelayed(0, -1).find((job) => job.id === delayed.id),
          'delayed-job',
          delayedData
        );
      }

      const priorityData = { name: 'user-priority', marker: 'priority' };
      const priorityQueue = harness.queue<NamedData>('priority');
      const priority = await priorityQueue.add('priority-job', priorityData, {
        priority: 10,
        durable: true,
      });
      expectPayload(
        (await priorityQueue.getPrioritized(0, -1)).find((job) => job.id === priority.id),
        'priority-job',
        priorityData
      );

      const all = await queue.getJobsAsync({ state: 'waiting', end: -1 });
      const waiting = await queue.getWaitingAsync(0, -1);
      expectPayload(
        all.find((job) => job.id === single.id),
        'single-job',
        singleData
      );
      expectPayload(
        waiting.find((job) => job.id === bulk.id),
        'bulk-job',
        bulkData
      );
      if (mode === 'embedded') {
        expectPayload(
          queue.getJobs({ state: 'waiting', end: -1 }).find((job) => job.id === single.id),
          'single-job',
          singleData
        );
        expectPayload(
          queue.getWaiting(0, -1).find((job) => job.id === bulk.id),
          'bulk-job',
          bulkData
        );
      }
    }, 30_000);

    test('Worker, active/completed/failed queries and DLQ preserve name and data', async () => {
      harness = await startHarness('name-data-lifecycle', mode);
      const activeQueue = harness.queue<NamedData>('active');
      const activeData = { name: 'user-active', marker: 'active' };
      let seen: Job<NamedData> | null = null;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      harness.worker<NamedData, boolean>(activeQueue.name, async (job) => {
        seen = job;
        await held;
        return true;
      });
      const active = await activeQueue.add('active-job', activeData, { durable: true });
      await waitForState(activeQueue, active.id, 'active', 10_000);
      expectPayload(seen, 'active-job', activeData);
      expectPayload(
        (await activeQueue.getActiveAsync(0, -1)).find((job) => job.id === active.id),
        'active-job',
        activeData
      );
      if (mode === 'embedded') {
        expectPayload(
          activeQueue.getActive(0, -1).find((job) => job.id === active.id),
          'active-job',
          activeData
        );
      }
      release();
      await waitForState(activeQueue, active.id, 'completed', 10_000);
      expectPayload(
        (await activeQueue.getCompletedAsync(0, -1)).find((job) => job.id === active.id),
        'active-job',
        activeData
      );
      if (mode === 'embedded') {
        expectPayload(
          activeQueue.getCompleted(0, -1).find((job) => job.id === active.id),
          'active-job',
          activeData
        );
      }

      const failedQueue = harness.queue<NamedData>('failed');
      const failedData = { name: 'user-failed', marker: 'failed' };
      harness.worker(failedQueue.name, async () => {
        throw new Error('expected failure');
      });
      const failed = await failedQueue.add('failed-job', failedData, {
        attempts: 1,
        durable: true,
      });
      await waitForState(failedQueue, failed.id, 'failed', 15_000);
      expectPayload(
        (await failedQueue.getFailedAsync(0, -1)).find((job) => job.id === failed.id),
        'failed-job',
        failedData
      );
      if (mode === 'embedded') {
        expectPayload(
          failedQueue.getFailed(0, -1).find((job) => job.id === failed.id),
          'failed-job',
          failedData
        );
      }
      const entry = (await failedQueue.getDlqAsync()).find((item) => item.job.id === failed.id);
      expectPayload(entry?.job, 'failed-job', failedData);
      expectPayload((await failedQueue.getDlqJobsAsync()).at(0), 'failed-job', failedData);
    }, 40_000);

    test('FlowProducer and waiting-children reads preserve a user data.name', async () => {
      harness = await startHarness('name-data-flow', mode);
      const queue = harness.queue<NamedData>('flow');
      const flow = harness.flow();
      const parentData = { name: 'user-parent', marker: 'parent' };
      const childData = { name: 'user-child', marker: 'child' };
      const root = await flow.add<NamedData>({
        name: 'parent-job',
        queueName: queue.name,
        data: parentData,
        children: [{ name: 'child-job', queueName: queue.name, data: childData }],
      });

      expectPayload(root.job, 'parent-job', parentData);
      const stored = await flow.getFlow<NamedData>({
        id: root.job.id,
        queueName: queue.name,
        depth: 2,
      });
      expectPayload(stored?.job, 'parent-job', parentData);
      expectPayload(stored?.children?.[0]?.job, 'child-job', childData);
      const childId = root.children?.[0]?.job.id ?? '';
      const listedParent = (await queue.getWaitingChildren(0, -1)).find(
        (job) => job.id === root.job.id
      );
      expect(listedParent?.name).toBe('parent-job');
      expect(listedParent?.data).toEqual({ ...parentData, __childrenIds: [childId] });

      const queriedChild = await queue.getJob(childId);
      expect(queriedChild?.name).toBe('child-job');
      expect(queriedChild?.data).toEqual({
        ...childData,
        __parentId: root.job.id,
        __parentQueue: queue.name,
      });
      const listedChild = (await queue.getWaitingAsync(0, -1)).find((job) => job.id === childId);
      expect(listedChild?.data).toEqual(queriedChild?.data);
    }, 30_000);

    test('updateData and repeat successors keep the authoritative job name', async () => {
      harness = await startHarness('name-data-mutations', mode);
      const updateQueue = harness.queue<NamedData>('update');
      const original = { name: 'user-before', marker: 'before' };
      const updated = { name: 'user-after', marker: 'after' };
      const job = await updateQueue.add('update-job', original, { durable: true });
      await job.updateData(updated);
      expectPayload(await updateQueue.getJob(job.id), 'update-job', updated);

      const repeatQueue = harness.queue<NamedData>('repeat');
      const repeatData = { name: 'user-repeat', marker: 'repeat' };
      const executions: Job<NamedData>[] = [];
      harness.worker(repeatQueue.name, async (current) => {
        executions.push(current);
        return true;
      });
      await repeatQueue.add('repeat-job', repeatData, {
        repeat: { every: 30, limit: 1 },
        durable: true,
      });
      await waitUntil(() => executions.length >= 2, 'repeat successor', 20_000);
      for (const execution of executions.slice(0, 2)) {
        expectPayload(execution, 'repeat-job', repeatData);
      }
    }, 30_000);

    test('primitive and undefined data stay independent from the job name', async () => {
      harness = await startHarness('name-data-values', mode);
      const queue = harness.queue<unknown>('values');
      const primitive = await queue.add('primitive-job', 42, { durable: true });
      const missing = await queue.add('undefined-job', undefined, { durable: true });

      expect(primitive.name).toBe('primitive-job');
      expect(primitive.data).toBe(42);
      expect(missing.name).toBe('undefined-job');
      expect(missing.data).toBeUndefined();
      expect((await queue.getJob(primitive.id))?.data).toBe(42);
      expect((await queue.getJob(missing.id))?.data).toBeUndefined();
    }, 30_000);

    test('invalid names are rejected before any addBulk mutation', async () => {
      harness = await startHarness('name-data-validation', mode);
      const queue = harness.queue<Record<string, boolean>>('validation');

      await expect(queue.add('', { invalid: true })).rejects.toThrow(/job name/i);
      await expect(queue.add('x'.repeat(257), { invalid: true })).rejects.toThrow(/job name/i);
      await expect(queue.add(42 as unknown as string, { invalid: true })).rejects.toThrow(
        /job name/i
      );
      await expect(
        queue.addBulk([
          { name: 'would-be-valid', data: { invalid: false } },
          { name: '', data: { invalid: true } },
        ])
      ).rejects.toThrow(/job name/i);
      expect(await queue.getWaitingAsync(0, -1)).toHaveLength(0);
    }, 30_000);

    test('SandboxedWorker receives the separate name and untouched data', async () => {
      harness = await startHarness('name-data-sandboxed', mode);
      const queue = harness.queue<NamedData>('sandboxed');
      const data = { name: 'user-sandboxed', marker: 'sandboxed' };
      const results: Array<{ name: string; data: NamedData }> = [];
      const eventJobs: Job<NamedData>[] = [];
      const worker = new SandboxedWorker(queue.name, {
        processor: processorFile(),
        concurrency: 1,
        timeout: 30_000,
        ...(mode === 'tcp' ? { connection: harness.connection() } : {}),
      });
      sandboxedWorkers.push(worker);
      worker.on('error', () => undefined);
      worker.on('completed', (eventJob, result) => {
        eventJobs.push(eventJob as Job<NamedData>);
        results.push(result as { name: string; data: NamedData });
      });
      await worker.start();
      const added = await queue.add('sandboxed-job', data, { durable: true });
      await waitUntil(() => results.length === 1, 'sandboxed result', 30_000);
      expect(results[0]).toEqual({ name: 'sandboxed-job', data });
      expectPayload(eventJobs[0], 'sandboxed-job', data);
      await waitForState(queue, added.id, 'completed', 20_000);
    }, 60_000);

    if (mode === 'tcp') {
      test('Hello advertises the separate job name wire format', async () => {
        harness = await startHarness('name-data-protocol', mode);
        const pool = new TcpConnectionPool(harness.connection());
        harness.addCleanup(() => pool.close());
        await pool.connect();

        const hello = await pool.send({
          cmd: 'Hello',
          protocolVersion: 3,
          capabilities: ['pipelining', 'separate-job-name'],
        });

        expect(hello.protocolVersion).toBe(3);
        expect(hello.capabilities).toContain('separate-job-name');
      }, 30_000);

      test('raw PUSH, PULL and GetJob expose top-level name with legacy fallback', async () => {
        harness = await startHarness('name-data-wire', mode);
        const pool = new TcpConnectionPool(harness.connection());
        harness.addCleanup(() => pool.close());
        await pool.connect();
        const queue = harness.unique('raw');
        const data = { name: 'user-wire', marker: 'wire' };
        const pushed = await pool.send({ cmd: 'PUSH', queue, name: 'wire-job', data });
        const fetched = await pool.send({ cmd: 'GetJob', id: pushed.id });
        expect(fetched.job).toMatchObject({ name: 'wire-job', data });
        const pulled = await pool.send({ cmd: 'PULL', queue, timeout: 0 });
        expect(pulled.job).toMatchObject({ name: 'wire-job', data });
        await pool.send({ cmd: 'ACK', id: pushed.id });

        const legacy = await pool.send({
          cmd: 'PUSH',
          queue,
          data: { name: 'legacy-job', marker: 'legacy' },
        });
        const legacyFetched = await pool.send({ cmd: 'GetJob', id: legacy.id });
        expect(legacyFetched.job).toMatchObject({
          name: 'legacy-job',
          data: { marker: 'legacy' },
        });
      }, 30_000);
    }
  });
}
