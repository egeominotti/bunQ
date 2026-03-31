/**
 * ETL Pipeline Tests (Embedded Mode)
 *
 * Tests real-world ETL (Extract, Transform, Load) pipeline scenarios
 * using flow chains, fan-out/fan-in, multi-record processing, validation,
 * cascading steps, parallel pipelines, and retry logic.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('ETL Pipeline - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('simple ETL chain: extract -> transform -> load', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('etl-simple', { embedded: true });
    queue.obliterate();

    let loadedData: unknown = null;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'etl-simple',
      async (job) => {
        const data = job.data as { stage: string; __flowParentId?: string };

        if (data.stage === 'extract') {
          return { records: [10, 20, 30, 40, 50] };
        }

        if (data.stage === 'transform') {
          const parentResult = flow.getParentResult<{ records: number[] }>(data.__flowParentId!);
          const doubled = parentResult!.records.map((v) => v * 2);
          return { records: doubled };
        }

        if (data.stage === 'load') {
          const parentResult = flow.getParentResult<{ records: number[] }>(data.__flowParentId!);
          loadedData = { records: parentResult!.records, count: parentResult!.records.length };
          resolve!();
          return loadedData;
        }

        return {};
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'extract', queueName: 'etl-simple', data: { stage: 'extract' } },
      { name: 'transform', queueName: 'etl-simple', data: { stage: 'transform' } },
      { name: 'load', queueName: 'etl-simple', data: { stage: 'load' } },
    ]);

    await done;
    await Bun.sleep(200);

    // [10,20,30,40,50] * 2 = [20,40,60,80,100]
    expect(loadedData).toEqual({
      records: [20, 40, 60, 80, 100],
      count: 5,
    });

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('fan-out ETL: 1 extract -> 3 parallel transforms -> 1 load', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('etl-fanout', { embedded: true });
    queue.obliterate();

    const executionOrder: string[] = [];
    let finalExecuted = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'etl-fanout',
      async (job) => {
        const data = job.data as { task: string; value?: number };
        executionOrder.push(data.task);

        if (data.task === 'extract') {
          await Bun.sleep(30);
          return { raw: data.value ?? 0 };
        }

        if (data.task.startsWith('transform-')) {
          await Bun.sleep(30);
          return { transformed: (data.value ?? 0) * 2 };
        }

        if (data.task === 'load') {
          finalExecuted = true;
          resolve!();
          return { loaded: true };
        }

        return {};
      },
      { embedded: true, concurrency: 5 }
    );

    await flow.addBulkThen(
      [
        { name: 'transform-a', queueName: 'etl-fanout', data: { task: 'transform-a', value: 10 } },
        { name: 'transform-b', queueName: 'etl-fanout', data: { task: 'transform-b', value: 20 } },
        { name: 'transform-c', queueName: 'etl-fanout', data: { task: 'transform-c', value: 30 } },
      ],
      { name: 'load', queueName: 'etl-fanout', data: { task: 'load' } }
    );

    await done;
    await Bun.sleep(200);

    expect(finalExecuted).toBe(true);
    // Load must be last
    expect(executionOrder.indexOf('load')).toBe(executionOrder.length - 1);
    // All 3 transform steps must have executed before load
    const transforms = executionOrder.filter((t) => t.startsWith('transform-'));
    expect(transforms).toHaveLength(3);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('multi-record pipeline: 20 records through 3-step chain', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('etl-multi-record', { embedded: true });
    queue.obliterate();

    const RECORD_COUNT = 20;
    const completedRecords: Array<{ id: number; tags: string[] }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'etl-multi-record',
      async (job) => {
        const data = job.data as { recordId: number; tags: string[]; stage: string };

        if (data.stage === 'ingest') {
          return { recordId: data.recordId, tags: [...data.tags, 'ingested'] };
        }

        if (data.stage === 'enrich') {
          const parentResult = flow.getParentResult<{ recordId: number; tags: string[] }>(
            (job.data as any).__flowParentId
          );
          return {
            recordId: parentResult!.recordId,
            tags: [...parentResult!.tags, 'enriched'],
          };
        }

        if (data.stage === 'store') {
          const parentResult = flow.getParentResult<{ recordId: number; tags: string[] }>(
            (job.data as any).__flowParentId
          );
          const finalRecord = {
            id: parentResult!.recordId,
            tags: [...parentResult!.tags, 'stored'],
          };
          completedRecords.push(finalRecord);
          if (completedRecords.length === RECORD_COUNT) resolve!();
          return finalRecord;
        }

        return {};
      },
      { embedded: true, concurrency: 5 }
    );

    // Push 20 independent 3-step chains
    for (let i = 0; i < RECORD_COUNT; i++) {
      await flow.addChain([
        { name: `ingest-${i}`, queueName: 'etl-multi-record', data: { recordId: i, tags: [], stage: 'ingest' } },
        { name: `enrich-${i}`, queueName: 'etl-multi-record', data: { recordId: i, tags: [], stage: 'enrich' } },
        { name: `store-${i}`, queueName: 'etl-multi-record', data: { recordId: i, tags: [], stage: 'store' } },
      ]);
    }

    // Wait for completion with timeout
    for (let i = 0; i < 200; i++) {
      if (completedRecords.length >= RECORD_COUNT) break;
      await Bun.sleep(100);
    }

    expect(completedRecords).toHaveLength(RECORD_COUNT);

    // Verify all records have all 3 tags
    const sortedRecords = completedRecords.sort((a, b) => a.id - b.id);
    for (let i = 0; i < RECORD_COUNT; i++) {
      expect(sortedRecords[i].id).toBe(i);
      expect(sortedRecords[i].tags).toEqual(['ingested', 'enriched', 'stored']);
    }

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('ETL with validation: 7 valid + 3 invalid records', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queueExtract = new Queue('etl-validate-extract', { embedded: true });
    const queueTransform = new Queue('etl-validate-transform', { embedded: true });
    queueExtract.obliterate();
    queueTransform.obliterate();

    const completedIds: number[] = [];
    const failedIds: number[] = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const extractWorker = new Worker(
      'etl-validate-extract',
      async (job) => {
        const data = job.data as { recordId: number; value: number };
        return { recordId: data.recordId, value: data.value };
      },
      { embedded: true, concurrency: 5 }
    );

    const transformWorker = new Worker(
      'etl-validate-transform',
      async (job) => {
        const data = job.data as { recordId: number; value: number };
        // Invalid records have negative values
        if (data.value < 0) {
          throw new Error(`Validation failed: record ${data.recordId} has negative value`);
        }
        return { recordId: data.recordId, result: data.value * 10 };
      },
      { embedded: true, concurrency: 5 }
    );

    transformWorker.on('completed', (job) => {
      const data = job.data as { recordId: number };
      completedIds.push(data.recordId);
      if (completedIds.length + failedIds.length === 10) resolve!();
    });

    transformWorker.on('failed', (job) => {
      const data = job.data as { recordId: number };
      failedIds.push(data.recordId);
      if (completedIds.length + failedIds.length === 10) resolve!();
    });

    // Push 10 records: 7 valid (positive), 3 invalid (negative)
    const records = [
      { recordId: 0, value: 100 },
      { recordId: 1, value: -1 },   // invalid
      { recordId: 2, value: 200 },
      { recordId: 3, value: 300 },
      { recordId: 4, value: -5 },   // invalid
      { recordId: 5, value: 400 },
      { recordId: 6, value: 500 },
      { recordId: 7, value: -10 },  // invalid
      { recordId: 8, value: 600 },
      { recordId: 9, value: 700 },
    ];

    for (const rec of records) {
      await queueTransform.add(`validate-${rec.recordId}`, rec, {
        attempts: 1, // No retry for validation failures
      });
    }

    // Wait for all to complete or fail
    for (let i = 0; i < 100; i++) {
      if (completedIds.length + failedIds.length >= 10) break;
      await Bun.sleep(100);
    }

    expect(completedIds.sort((a, b) => a - b)).toEqual([0, 2, 3, 5, 6, 8, 9]);
    expect(failedIds.sort((a, b) => a - b)).toEqual([1, 4, 7]);

    await extractWorker.close();
    await transformWorker.close();
    flow.close();
    queueExtract.close();
    queueTransform.close();
  }, 30000);

  test('cascading ETL: 5-step chain ingest -> validate -> enrich -> transform -> load', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('etl-cascade', { embedded: true });
    queue.obliterate();

    const executionOrder: string[] = [];
    let finalResult: unknown = null;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'etl-cascade',
      async (job) => {
        const data = job.data as { stage: string; __flowParentId?: string };
        executionOrder.push(data.stage);

        if (data.stage === 'ingest') {
          return { value: 10, steps: ['ingest'] };
        }

        const parentResult = flow.getParentResult<{ value: number; steps: string[] }>(data.__flowParentId!);

        if (data.stage === 'validate') {
          return { value: parentResult!.value, steps: [...parentResult!.steps, 'validate'], valid: true };
        }

        if (data.stage === 'enrich') {
          return {
            value: parentResult!.value + 5,
            steps: [...(parentResult as any).steps, 'enrich'],
            metadata: { source: 'api' },
          };
        }

        if (data.stage === 'transform') {
          return {
            value: (parentResult as any).value * 3,
            steps: [...(parentResult as any).steps, 'transform'],
          };
        }

        if (data.stage === 'load') {
          finalResult = {
            value: (parentResult as any).value,
            steps: [...(parentResult as any).steps, 'load'],
          };
          resolve!();
          return finalResult;
        }

        return {};
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'ingest', queueName: 'etl-cascade', data: { stage: 'ingest' } },
      { name: 'validate', queueName: 'etl-cascade', data: { stage: 'validate' } },
      { name: 'enrich', queueName: 'etl-cascade', data: { stage: 'enrich' } },
      { name: 'transform', queueName: 'etl-cascade', data: { stage: 'transform' } },
      { name: 'load', queueName: 'etl-cascade', data: { stage: 'load' } },
    ]);

    await done;
    await Bun.sleep(200);

    // Verify execution order
    expect(executionOrder).toEqual(['ingest', 'validate', 'enrich', 'transform', 'load']);

    // Verify data enrichment: (10 + 5) * 3 = 45
    expect(finalResult).toEqual({
      value: 45,
      steps: ['ingest', 'validate', 'enrich', 'transform', 'load'],
    });

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('parallel ETL pipelines: 5 independent 3-step chains', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('etl-parallel-pipes', { embedded: true });
    queue.obliterate();

    const pipelineResults = new Map<number, string[]>();
    let completedPipelines = 0;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const PIPELINE_COUNT = 5;

    const worker = new Worker(
      'etl-parallel-pipes',
      async (job) => {
        const data = job.data as { pipelineId: number; stage: string };
        if (!pipelineResults.has(data.pipelineId)) {
          pipelineResults.set(data.pipelineId, []);
        }
        pipelineResults.get(data.pipelineId)!.push(data.stage);

        await Bun.sleep(20);

        if (data.stage === 'load') {
          completedPipelines++;
          if (completedPipelines === PIPELINE_COUNT) resolve!();
        }

        return { pipelineId: data.pipelineId, stage: data.stage };
      },
      { embedded: true, concurrency: 10 }
    );

    // Launch 5 independent chains concurrently
    await Promise.all(
      Array.from({ length: PIPELINE_COUNT }, (_, i) =>
        flow.addChain([
          { name: `extract-${i}`, queueName: 'etl-parallel-pipes', data: { pipelineId: i, stage: 'extract' } },
          { name: `transform-${i}`, queueName: 'etl-parallel-pipes', data: { pipelineId: i, stage: 'transform' } },
          { name: `load-${i}`, queueName: 'etl-parallel-pipes', data: { pipelineId: i, stage: 'load' } },
        ])
      )
    );

    // Wait for all pipelines to complete
    for (let i = 0; i < 200; i++) {
      if (completedPipelines >= PIPELINE_COUNT) break;
      await Bun.sleep(100);
    }

    expect(completedPipelines).toBe(PIPELINE_COUNT);

    // Each pipeline should have executed all 3 stages
    for (let i = 0; i < PIPELINE_COUNT; i++) {
      const stages = pipelineResults.get(i);
      expect(stages).toBeDefined();
      expect(stages).toHaveLength(3);
      expect(stages).toContain('extract');
      expect(stages).toContain('transform');
      expect(stages).toContain('load');
    }

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('ETL with retry: transform fails first attempt, succeeds on retry', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('etl-retry', { embedded: true });
    queue.obliterate();

    let transformAttempts = 0;
    let chainCompleted = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'etl-retry',
      async (job) => {
        const data = job.data as { stage: string; __flowParentId?: string };

        if (data.stage === 'extract') {
          return { records: [1, 2, 3] };
        }

        if (data.stage === 'transform') {
          transformAttempts++;
          if (transformAttempts < 2) {
            throw new Error(`Transient DB error on attempt ${transformAttempts}`);
          }
          const parentResult = flow.getParentResult<{ records: number[] }>(data.__flowParentId!);
          return { records: parentResult!.records.map((v) => v * 10) };
        }

        if (data.stage === 'load') {
          const parentResult = flow.getParentResult<{ records: number[] }>(data.__flowParentId!);
          chainCompleted = true;
          resolve!();
          return { loaded: parentResult!.records };
        }

        return {};
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'extract', queueName: 'etl-retry', data: { stage: 'extract' } },
      {
        name: 'transform',
        queueName: 'etl-retry',
        data: { stage: 'transform' },
        opts: { attempts: 3, backoff: 100 },
      },
      { name: 'load', queueName: 'etl-retry', data: { stage: 'load' } },
    ]);

    await done;
    await Bun.sleep(200);

    expect(transformAttempts).toBe(2);
    expect(chainCompleted).toBe(true);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);
});
