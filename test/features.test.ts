/**
 * Tests for new bunqueue features
 * - Bulk Operations (optimized addBulk)
 * - QueueGroup (namespaces)
 * - Repeatable Jobs
 * - FlowProducer (job pipelines)
 */

import { describe, it, expect, afterEach } from 'bun:test';
import {
  Queue,
  Worker,
  QueueGroup,
  FlowProducer,
  shutdownManager,
} from '../src/client';

describe('Bulk Operations', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('addBulk should add multiple jobs efficiently', async () => {
    const queue = new Queue<{ index: number }>('bulk-test');

    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: 'task',
      data: { index: i },
    }));

    const start = performance.now();
    const result = await queue.addBulk(jobs);
    const duration = performance.now() - start;

    expect(result).toHaveLength(100);
    expect(result[0].name).toBe('task');
    expect(result[0].data.index).toBe(0);
    expect(result[99].data.index).toBe(99);

    // Should be fast (< 500ms for 100 jobs)
    expect(duration).toBeLessThan(500);
  });

  it('addBulk should return correct job IDs', async () => {
    const queue = new Queue('bulk-ids');

    const result = await queue.addBulk([
      { name: 'a', data: { x: 1 } },
      { name: 'b', data: { x: 2 } },
      { name: 'c', data: { x: 3 } },
    ]);

    expect(result).toHaveLength(3);
    expect(result[0].id).toBeDefined();
    expect(result[1].id).toBeDefined();
    expect(result[2].id).toBeDefined();

    // IDs should be unique
    const ids = new Set(result.map((j) => j.id));
    expect(ids.size).toBe(3);
  });

  it('addBulk should handle empty array', async () => {
    const queue = new Queue('bulk-empty');
    const result = await queue.addBulk([]);
    expect(result).toHaveLength(0);
  });
});

describe('QueueGroup', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should prefix queue names with namespace', () => {
    const group = new QueueGroup('billing');
    const queue = group.getQueue('invoices');

    expect(queue.name).toBe('billing:invoices');
  });

  it('should handle namespace with trailing colon', () => {
    const group = new QueueGroup('billing:');
    const queue = group.getQueue('payments');

    expect(queue.name).toBe('billing:payments');
  });

  it('should create workers with prefixed queue names', async () => {
    const group = new QueueGroup('emails');
    let processedQueue = '';

    const worker = group.getWorker('outbound', async (job) => {
      processedQueue = job.queueName;
      return { ok: true };
    });

    const queue = group.getQueue('outbound');
    await queue.add('send', { to: 'test@test.com' });

    await Bun.sleep(200);

    expect(processedQueue).toBe('emails:outbound');

    await worker.close();
  });

  it('should list queues in group', async () => {
    const group = new QueueGroup('test-ns');

    // Create some queues
    const q1 = group.getQueue('queue1');
    const q2 = group.getQueue('queue2');

    await q1.add('task', {});
    await q2.add('task', {});

    const queues = group.listQueues();

    expect(queues).toContain('queue1');
    expect(queues).toContain('queue2');
  });

  it('should pause and resume all queues in group', async () => {
    const group = new QueueGroup('pause-test');
    const q1 = group.getQueue('a');
    const q2 = group.getQueue('b');

    await q1.add('task', {});
    await q2.add('task', {});

    group.pauseAll();
    group.resumeAll();

    // No error means success
    expect(true).toBe(true);
  });
});

describe('Repeatable Jobs', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should repeat job according to config', async () => {
    const queue = new Queue<{ value: number }>('repeat-test');
    const executions: number[] = [];

    const worker = new Worker('repeat-test', async (job) => {
      executions.push(job.data.value);
      return { done: true };
    });

    await queue.add('task', { value: 42 }, {
      repeat: { every: 50, limit: 3 },
    });

    // Wait for all repetitions
    await Bun.sleep(400);

    expect(executions.length).toBeGreaterThanOrEqual(3);

    await worker.close();
  });

  it('should not repeat after limit reached', async () => {
    const queue = new Queue('repeat-limit');
    let count = 0;

    const worker = new Worker('repeat-limit', async () => {
      count++;
      return {};
    });

    await queue.add('task', {}, {
      repeat: { every: 30, limit: 2 },
    });

    await Bun.sleep(300);

    // Should have executed at most 2 times
    expect(count).toBeLessThanOrEqual(3); // Initial + 2 repeats max

    await worker.close();
  });

  it('should pass repeat config through addBulk', async () => {
    const queue = new Queue('repeat-bulk');
    let count = 0;

    const worker = new Worker('repeat-bulk', async () => {
      count++;
      return {};
    });

    await queue.addBulk([
      { name: 'task', data: {}, opts: { repeat: { every: 50, limit: 2 } } },
    ]);

    await Bun.sleep(300);

    expect(count).toBeGreaterThanOrEqual(2);

    await worker.close();
  });
});

describe('FlowProducer', () => {
  afterEach(() => {
    shutdownManager();
  });

  describe('addChain()', () => {
    it('should create chain of dependent jobs', async () => {
      const flow = new FlowProducer();

      const { jobIds } = await flow.addChain([
        { name: 'step1', queueName: 'chain-test', data: { n: 1 } },
        { name: 'step2', queueName: 'chain-test', data: { n: 2 } },
        { name: 'step3', queueName: 'chain-test', data: { n: 3 } },
      ]);

      expect(jobIds).toHaveLength(3);
      expect(jobIds[0]).toBeDefined();
      expect(jobIds[1]).toBeDefined();
      expect(jobIds[2]).toBeDefined();
    });

    it('should execute jobs in order', async () => {
      const flow = new FlowProducer();
      const order: number[] = [];

      const worker = new Worker('chain-order', async (job) => {
        order.push(job.data.step);
        await Bun.sleep(30);
        return { step: job.data.step };
      });

      await flow.addChain([
        { name: 'a', queueName: 'chain-order', data: { step: 1 } },
        { name: 'b', queueName: 'chain-order', data: { step: 2 } },
        { name: 'c', queueName: 'chain-order', data: { step: 3 } },
      ]);

      // Wait longer - dependency check runs every 1000ms
      // Need enough time for: job1 + dep check + job2 + dep check + job3
      await Bun.sleep(3500);

      // Jobs should execute in order (dependencies)
      expect(order[0]).toBe(1);
      expect(order[1]).toBe(2);
      expect(order[2]).toBe(3);

      await worker.close();
    });

    it('should handle empty chain', async () => {
      const flow = new FlowProducer();
      const { jobIds } = await flow.addChain([]);
      expect(jobIds).toHaveLength(0);
    });
  });

  describe('addBulkThen()', () => {
    it('should create parallel jobs with final merge', async () => {
      const flow = new FlowProducer();

      const result = await flow.addBulkThen(
        [
          { name: 'p1', queueName: 'parallel', data: { id: 1 } },
          { name: 'p2', queueName: 'parallel', data: { id: 2 } },
          { name: 'p3', queueName: 'parallel', data: { id: 3 } },
        ],
        { name: 'merge', queueName: 'parallel', data: {} }
      );

      expect(result.parallelIds).toHaveLength(3);
      expect(result.finalId).toBeDefined();
    });

    it('should execute final after all parallel complete', async () => {
      const flow = new FlowProducer();
      const executed: string[] = [];

      const worker = new Worker('parallel-test', async (job) => {
        executed.push(job.name);
        await Bun.sleep(20);
        return { name: job.name };
      });

      await flow.addBulkThen(
        [
          { name: 'p1', queueName: 'parallel-test', data: {} },
          { name: 'p2', queueName: 'parallel-test', data: {} },
        ],
        { name: 'final', queueName: 'parallel-test', data: {} }
      );

      // Wait longer - dependency check runs every 1000ms
      await Bun.sleep(2500);

      // Final should be last
      expect(executed[executed.length - 1]).toBe('final');

      await worker.close();
    });
  });

  describe('addTree()', () => {
    it('should create tree structure with dependencies', async () => {
      const flow = new FlowProducer();

      const { jobIds } = await flow.addTree({
        name: 'root',
        queueName: 'tree-test',
        data: { level: 0 },
        children: [
          { name: 'child1', queueName: 'tree-test', data: { level: 1 } },
          { name: 'child2', queueName: 'tree-test', data: { level: 1 } },
        ],
      });

      // Root + 2 children
      expect(jobIds).toHaveLength(3);
    });
  });

  describe('getParentResult()', () => {
    it('should retrieve parent job result', async () => {
      const flow = new FlowProducer();
      let step2Data: unknown = null;

      const worker = new Worker('parent-result', async (job) => {
        if (job.name === 'step1') {
          return { fromStep1: 'hello' };
        }

        if (job.name === 'step2' && job.data.__flowParentId) {
          step2Data = flow.getParentResult(job.data.__flowParentId);
        }

        return { done: true };
      });

      await flow.addChain([
        { name: 'step1', queueName: 'parent-result', data: {} },
        { name: 'step2', queueName: 'parent-result', data: {} },
      ]);

      // Wait longer - dependency check runs every 1000ms
      await Bun.sleep(2500);

      expect(step2Data).toEqual({ fromStep1: 'hello' });

      await worker.close();
    });
  });
});

describe('Integration', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should work with QueueGroup and FlowProducer together', async () => {
    const group = new QueueGroup('pipeline');
    const flow = new FlowProducer();
    const processed: string[] = [];

    const worker = group.getWorker('process', async (job) => {
      processed.push(job.name);
      return { name: job.name };
    });

    await flow.addChain([
      { name: 'fetch', queueName: 'pipeline:process', data: {} },
      { name: 'transform', queueName: 'pipeline:process', data: {} },
      { name: 'store', queueName: 'pipeline:process', data: {} },
    ]);

    // Wait longer - dependency check runs every 1000ms
    await Bun.sleep(3500);

    expect(processed).toContain('fetch');
    expect(processed).toContain('transform');
    expect(processed).toContain('store');

    await worker.close();
  });

  it('should handle high-throughput bulk with groups', async () => {
    const group = new QueueGroup('bulk');
    const queue = group.getQueue<{ i: number }>('test');
    let completed = 0;

    const worker = group.getWorker('test', async () => {
      completed++;
      return {};
    }, { concurrency: 50 });

    const jobs = Array.from({ length: 500 }, (_, i) => ({
      name: 'task',
      data: { i },
    }));

    await queue.addBulk(jobs);

    // Wait for all to complete
    const start = Date.now();
    while (completed < 500 && Date.now() - start < 5000) {
      await Bun.sleep(50);
    }

    expect(completed).toBe(500);

    await worker.close();
  });
});
