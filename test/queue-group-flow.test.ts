/**
 * QueueGroup + Flow Tests (Embedded Mode)
 *
 * Tests interaction between QueueGroup namespace isolation and
 * FlowProducer job chains, including pause/resume and cleanup.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, QueueGroup, FlowProducer, shutdownManager } from '../src/client';

describe('QueueGroup + Flow - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('queue group manages multiple queues with flow jobs', async () => {
    const group = new QueueGroup('flow-grp');
    const flow = new FlowProducer({ embedded: true });

    const q1 = group.getQueue<{ task: string }>('tasks-a');
    const q2 = group.getQueue<{ task: string }>('tasks-b');
    q1.obliterate();
    q2.obliterate();

    const processed: Array<{ queue: string; task: string }> = [];
    let completedCount = 0;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const w1 = group.getWorker<{ task: string }>('tasks-a', async (job) => {
      processed.push({ queue: 'tasks-a', task: job.data.task });
      completedCount++;
      if (completedCount >= 4) resolve!();
      return { ok: true };
    });

    const w2 = group.getWorker<{ task: string }>('tasks-b', async (job) => {
      processed.push({ queue: 'tasks-b', task: job.data.task });
      completedCount++;
      if (completedCount >= 4) resolve!();
      return { ok: true };
    });

    // Add chain within group queue A
    await flow.addChain([
      { name: 'step-1', queueName: 'flow-grp:tasks-a', data: { task: 'a-step-1' } },
      { name: 'step-2', queueName: 'flow-grp:tasks-a', data: { task: 'a-step-2' } },
    ]);

    // Add chain within group queue B
    await flow.addChain([
      { name: 'step-1', queueName: 'flow-grp:tasks-b', data: { task: 'b-step-1' } },
      { name: 'step-2', queueName: 'flow-grp:tasks-b', data: { task: 'b-step-2' } },
    ]);

    await done;
    await Bun.sleep(300);

    // All 4 jobs should have been processed
    expect(processed).toHaveLength(4);

    const tasksA = processed.filter((p) => p.queue === 'tasks-a').map((p) => p.task);
    const tasksB = processed.filter((p) => p.queue === 'tasks-b').map((p) => p.task);

    expect(tasksA).toContain('a-step-1');
    expect(tasksA).toContain('a-step-2');
    expect(tasksB).toContain('b-step-1');
    expect(tasksB).toContain('b-step-2');

    // Verify execution order within each chain
    const aIdx1 = processed.findIndex((p) => p.task === 'a-step-1');
    const aIdx2 = processed.findIndex((p) => p.task === 'a-step-2');
    expect(aIdx1).toBeLessThan(aIdx2);

    const bIdx1 = processed.findIndex((p) => p.task === 'b-step-1');
    const bIdx2 = processed.findIndex((p) => p.task === 'b-step-2');
    expect(bIdx1).toBeLessThan(bIdx2);

    await w1.close();
    await w2.close();
    flow.close();
  }, 20000);

  test('flow spanning multiple queues in a group', async () => {
    const group = new QueueGroup('cross-q');
    const flow = new FlowProducer({ embedded: true });

    const qIngest = group.getQueue<{ step: string }>('ingest');
    const qProcess = group.getQueue<{ step: string }>('process');
    const qStore = group.getQueue<{ step: string }>('store');
    qIngest.obliterate();
    qProcess.obliterate();
    qStore.obliterate();

    const results: Array<{ queue: string; step: string }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const wIngest = group.getWorker<{ step: string }>('ingest', async (job) => {
      results.push({ queue: 'ingest', step: job.data.step });
      return { done: true };
    });

    const wProcess = group.getWorker<{ step: string }>('process', async (job) => {
      results.push({ queue: 'process', step: job.data.step });
      return { done: true };
    });

    const wStore = group.getWorker<{ step: string }>('store', async (job) => {
      results.push({ queue: 'store', step: job.data.step });
      if (job.data.step === 'save') resolve!();
      return { done: true };
    });

    // Chain across different queues within the group
    await flow.addChain([
      { name: 'fetch', queueName: 'cross-q:ingest', data: { step: 'fetch' } },
      { name: 'transform', queueName: 'cross-q:process', data: { step: 'transform' } },
      { name: 'save', queueName: 'cross-q:store', data: { step: 'save' } },
    ]);

    await done;
    await Bun.sleep(300);

    expect(results).toHaveLength(3);
    expect(results[0]).toEqual({ queue: 'ingest', step: 'fetch' });
    expect(results[1]).toEqual({ queue: 'process', step: 'transform' });
    expect(results[2]).toEqual({ queue: 'store', step: 'save' });

    await wIngest.close();
    await wProcess.close();
    await wStore.close();
    flow.close();
  }, 20000);

  test('queue group pause/resume affects flow processing', async () => {
    const group = new QueueGroup('pause-flow');
    const flow = new FlowProducer({ embedded: true });

    const q = group.getQueue<{ idx: number }>('work');
    q.obliterate();

    const processed: number[] = [];

    // Add a chain before pausing
    await flow.addChain([
      { name: 'step-0', queueName: 'pause-flow:work', data: { idx: 0 } },
      { name: 'step-1', queueName: 'pause-flow:work', data: { idx: 1 } },
    ]);

    // Pause the group before starting workers
    group.pauseAll();
    expect(q.isPaused()).toBe(true);

    const w = group.getWorker<{ idx: number }>('work', async (job) => {
      processed.push(job.data.idx);
      return { ok: true };
    });

    // Wait and verify nothing processes while paused
    for (let i = 0; i < 15; i++) await Bun.sleep(100);
    expect(processed.length).toBe(0);

    // Resume and verify processing resumes
    group.resumeAll();
    expect(q.isPaused()).toBe(false);

    for (let i = 0; i < 50 && processed.length < 2; i++) await Bun.sleep(100);

    expect(processed.length).toBe(2);
    // Chain ordering: step-0 before step-1
    expect(processed[0]).toBe(0);
    expect(processed[1]).toBe(1);

    await w.close();
    flow.close();
  }, 20000);

  test('queue group close cleans up flow resources', async () => {
    const group = new QueueGroup('cleanup-flow');
    const flow = new FlowProducer({ embedded: true });

    const q1 = group.getQueue<{ v: number }>('alpha');
    const q2 = group.getQueue<{ v: number }>('beta');
    q1.obliterate();
    q2.obliterate();

    // Add flow jobs to both queues
    await flow.addChain([
      { name: 's1', queueName: 'cleanup-flow:alpha', data: { v: 1 } },
      { name: 's2', queueName: 'cleanup-flow:alpha', data: { v: 2 } },
    ]);

    await flow.addChain([
      { name: 's1', queueName: 'cleanup-flow:beta', data: { v: 3 } },
      { name: 's2', queueName: 'cleanup-flow:beta', data: { v: 4 } },
    ]);

    // Verify jobs exist
    const counts1Before = q1.getJobCounts();
    const counts2Before = q2.getJobCounts();
    // First step of each chain should be waiting; second step depends on first
    expect(counts1Before.waiting).toBeGreaterThanOrEqual(1);
    expect(counts2Before.waiting).toBeGreaterThanOrEqual(1);

    // Obliterate all queues in the group
    group.obliterateAll();

    // Verify all queues are cleaned up
    const counts1After = q1.getJobCounts();
    const counts2After = q2.getJobCounts();
    expect(counts1After.waiting).toBe(0);
    expect(counts2After.waiting).toBe(0);

    // No queues should remain in the group
    const remaining = group.listQueues();
    expect(remaining.length).toBe(0);

    flow.close();
  }, 20000);
});
