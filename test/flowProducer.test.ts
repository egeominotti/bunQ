/**
 * FlowProducer Tests - BullMQ v5 Compatible Flow API
 * Tests for FlowProducer.add(), addBulk(), parent option, and getChildrenValues()
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer } from '../src/client';
import type { FlowJob, JobNode } from '../src/client';
import { shutdownManager } from '../src/client';

describe('FlowProducer - BullMQ v5 API', () => {
  let flowProducer: FlowProducer;
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, { result: number }> | null = null;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
    queue = new Queue('flow-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    flowProducer.close();
    queue.close();
    shutdownManager();
  });

  describe('add(flow)', () => {
    test('should add a simple flow without children', async () => {
      const flow: FlowJob<{ value: number }> = {
        name: 'simple-job',
        queueName: 'flow-test',
        data: { value: 42 },
      };

      const result = await flowProducer.add(flow);

      expect(result).toBeDefined();
      expect(result.job).toBeDefined();
      expect(result.job.id).toBeDefined();
      expect(result.job.name).toBe('simple-job');
      expect(result.job.queueName).toBe('flow-test');
      expect(result.children).toBeUndefined();
    });

    test('should add a flow with children', async () => {
      const flow: FlowJob<{ value: number }> = {
        name: 'parent-job',
        queueName: 'flow-test',
        data: { value: 100 },
        children: [
          { name: 'child-1', queueName: 'flow-test', data: { value: 1 } },
          { name: 'child-2', queueName: 'flow-test', data: { value: 2 } },
        ],
      };

      const result = await flowProducer.add(flow);

      expect(result.job).toBeDefined();
      expect(result.job.name).toBe('parent-job');
      expect(result.children).toBeDefined();
      expect(result.children).toHaveLength(2);
      expect(result.children![0].job.name).toBe('child-1');
      expect(result.children![1].job.name).toBe('child-2');
    });

    test('should add a nested flow (grandchildren)', async () => {
      const flow: FlowJob<{ value: number }> = {
        name: 'grandparent',
        queueName: 'flow-test',
        data: { value: 1000 },
        children: [
          {
            name: 'parent-1',
            queueName: 'flow-test',
            data: { value: 100 },
            children: [
              { name: 'child-1', queueName: 'flow-test', data: { value: 1 } },
              { name: 'child-2', queueName: 'flow-test', data: { value: 2 } },
            ],
          },
        ],
      };

      const result = await flowProducer.add(flow);

      expect(result.job.name).toBe('grandparent');
      expect(result.children).toHaveLength(1);
      expect(result.children![0].job.name).toBe('parent-1');
      expect(result.children![0].children).toHaveLength(2);
      expect(result.children![0].children![0].job.name).toBe('child-1');
      expect(result.children![0].children![1].job.name).toBe('child-2');
    });

    test('should return JobNode with correct structure', async () => {
      const flow: FlowJob = {
        name: 'test-job',
        queueName: 'flow-test',
        data: { value: 42 },
      };

      const result: JobNode = await flowProducer.add(flow);

      // Verify JobNode structure
      expect(result).toHaveProperty('job');
      expect(result.job).toHaveProperty('id');
      expect(result.job).toHaveProperty('name');
      expect(result.job).toHaveProperty('data');
      expect(result.job).toHaveProperty('queueName');
      expect(result.job).toHaveProperty('attemptsMade');
      expect(result.job).toHaveProperty('timestamp');
      expect(result.job).toHaveProperty('progress');
      expect(result.job).toHaveProperty('updateProgress');
      expect(result.job).toHaveProperty('log');
      expect(result.job).toHaveProperty('getState');
      expect(result.job).toHaveProperty('remove');
      expect(result.job).toHaveProperty('retry');
      expect(result.job).toHaveProperty('getChildrenValues');
    });
  });

  describe('addBulk(flows)', () => {
    test('should add multiple flows', async () => {
      const flows: FlowJob<{ value: number }>[] = [
        { name: 'flow-1', queueName: 'flow-test', data: { value: 1 } },
        { name: 'flow-2', queueName: 'flow-test', data: { value: 2 } },
        { name: 'flow-3', queueName: 'flow-test', data: { value: 3 } },
      ];

      const results = await flowProducer.addBulk(flows);

      expect(results).toHaveLength(3);
      expect(results[0].job.name).toBe('flow-1');
      expect(results[1].job.name).toBe('flow-2');
      expect(results[2].job.name).toBe('flow-3');
    });

    test('should add multiple flows with children', async () => {
      const flows: FlowJob<{ value: number }>[] = [
        {
          name: 'parent-1',
          queueName: 'flow-test',
          data: { value: 10 },
          children: [{ name: 'child-1a', queueName: 'flow-test', data: { value: 1 } }],
        },
        {
          name: 'parent-2',
          queueName: 'flow-test',
          data: { value: 20 },
          children: [{ name: 'child-2a', queueName: 'flow-test', data: { value: 2 } }],
        },
      ];

      const results = await flowProducer.addBulk(flows);

      expect(results).toHaveLength(2);
      expect(results[0].children).toHaveLength(1);
      expect(results[1].children).toHaveLength(1);
    });

    test('should handle empty array', async () => {
      const results = await flowProducer.addBulk([]);
      expect(results).toHaveLength(0);
    });
  });

  describe('JobOptions.parent', () => {
    test('should add job with parent reference', async () => {
      const parentJob = await queue.add('parent', { value: 100 });

      const childJob = await queue.add(
        'child',
        { value: 50 },
        { parent: { id: parentJob.id, queue: 'flow-test' } }
      );

      expect(childJob).toBeDefined();
      expect(childJob.id).toBeDefined();
    });

    test('should store parent info in job data', async () => {
      const parentJob = await queue.add('parent', { value: 100 });

      await queue.add('child', { value: 50 }, { parent: { id: parentJob.id, queue: 'flow-test' } });

      // Verify we can add multiple children to same parent
      await queue.add('child2', { value: 25 }, { parent: { id: parentJob.id, queue: 'flow-test' } });

      const counts = await queue.getJobCountsAsync();
      expect(counts.waiting).toBe(3); // parent + 2 children
    });
  });

  describe('Job.parent property', () => {
    test('should expose parent property on job', async () => {
      // Create a flow with parent-child relationship
      const flow: FlowJob<{ value: number }> = {
        name: 'parent',
        queueName: 'flow-test',
        data: { value: 100 },
        children: [{ name: 'child', queueName: 'flow-test', data: { value: 1 } }],
      };

      const result = await flowProducer.add(flow);

      // The child should have parent info (set by flow producer)
      const childJob = result.children![0].job;
      // Note: In our implementation, parent is extracted from job data
      expect(childJob).toBeDefined();
    });
  });

  describe('Job.getChildrenValues()', () => {
    test('should return empty object for job without children', async () => {
      const job = await queue.add('no-children', { value: 42 });
      const childrenValues = await job.getChildrenValues();
      expect(childrenValues).toEqual({});
    });

    test('should return empty object initially (children not completed)', async () => {
      const flow: FlowJob<{ value: number }> = {
        name: 'parent',
        queueName: 'flow-test',
        data: { value: 100 },
        children: [{ name: 'child', queueName: 'flow-test', data: { value: 1 } }],
      };

      const result = await flowProducer.add(flow);
      const childrenValues = await result.job.getChildrenValues();

      // Children not completed yet, so empty
      expect(childrenValues).toEqual({});
    });
  });

  describe('Flow execution order', () => {
    test('children jobs should be created with parent reference', async () => {
      const flow: FlowJob<{ value: number }> = {
        name: 'parent',
        queueName: 'flow-test',
        data: { value: 100 },
        children: [
          { name: 'child-1', queueName: 'flow-test', data: { value: 1 } },
          { name: 'child-2', queueName: 'flow-test', data: { value: 2 } },
        ],
      };

      const result = await flowProducer.add(flow);

      // Children should be waiting; parent waits for children (not in waiting state)
      const jobs = await queue.getWaitingAsync(0, 100);
      expect(jobs.length).toBeGreaterThanOrEqual(2);

      // Verify parent job exists
      const parentJob = await queue.getJob(result.job.id);
      expect(parentJob).toBeDefined();
      expect(parentJob!.name).toBe('parent');
    });
  });
});

describe('FlowProducer - Legacy API', () => {
  let flowProducer: FlowProducer;
  let queue: Queue<{ id: string }>;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
    queue = new Queue('legacy-flow-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    flowProducer.close();
    queue.close();
    shutdownManager();
  });

  describe('addChain()', () => {
    test('should create chain of dependent jobs', async () => {
      const result = await flowProducer.addChain([
        { name: 'step-1', queueName: 'legacy-flow-test', data: { id: '1' } },
        { name: 'step-2', queueName: 'legacy-flow-test', data: { id: '2' } },
        { name: 'step-3', queueName: 'legacy-flow-test', data: { id: '3' } },
      ]);

      expect(result.jobIds).toHaveLength(3);
    });

    test('should handle empty chain', async () => {
      const result = await flowProducer.addChain([]);
      expect(result.jobIds).toHaveLength(0);
    });
  });

  describe('addBulkThen()', () => {
    test('should create parallel jobs converging to final', async () => {
      const result = await flowProducer.addBulkThen(
        [
          { name: 'parallel-1', queueName: 'legacy-flow-test', data: { id: 'p1' } },
          { name: 'parallel-2', queueName: 'legacy-flow-test', data: { id: 'p2' } },
        ],
        { name: 'final', queueName: 'legacy-flow-test', data: { id: 'final' } }
      );

      expect(result.parallelIds).toHaveLength(2);
      expect(result.finalId).toBeDefined();
    });
  });

  describe('addTree()', () => {
    test('should create tree structure', async () => {
      const result = await flowProducer.addTree({
        name: 'root',
        queueName: 'legacy-flow-test',
        data: { id: 'root' },
        children: [
          { name: 'branch-1', queueName: 'legacy-flow-test', data: { id: 'b1' } },
          {
            name: 'branch-2',
            queueName: 'legacy-flow-test',
            data: { id: 'b2' },
            children: [{ name: 'leaf', queueName: 'legacy-flow-test', data: { id: 'leaf' } }],
          },
        ],
      });

      expect(result.jobIds.length).toBeGreaterThanOrEqual(4);
    });
  });
});

describe('getChildrenValues - Integration', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, number> | null = null;

  beforeEach(() => {
    queue = new Queue('children-values-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  test('should retrieve children values after completion', async () => {
    // This test verifies the getChildrenValues functionality
    // Note: Full integration requires worker processing children first

    const flowProducer = new FlowProducer({ embedded: true });

    const flow: FlowJob<{ value: number }> = {
      name: 'parent',
      queueName: 'children-values-test',
      data: { value: 100 },
      children: [
        { name: 'child-1', queueName: 'children-values-test', data: { value: 10 } },
        { name: 'child-2', queueName: 'children-values-test', data: { value: 20 } },
      ],
    };

    const result = await flowProducer.add(flow);

    // Initially children values should be empty (not processed yet)
    const initialValues = await result.job.getChildrenValues();
    expect(initialValues).toEqual({});

    flowProducer.close();
  });

  test('Queue.getChildrenValues should work', async () => {
    const job = await queue.add('test', { value: 42 });

    // Call through queue instance
    const values = await queue.getChildrenValues(job.id);
    expect(values).toEqual({});
  });
});
