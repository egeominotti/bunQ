#!/usr/bin/env bun
/**
 * ETL Pipeline Tests (TCP Mode)
 *
 * Tests real-world ETL pipeline scenarios using flow chains, fan-out/fan-in,
 * multi-record processing, validation, cascading steps, parallel pipelines,
 * and retry logic. These tests verify execution order and completion;
 * the shared runtime-results contract covers direct FlowProducer result reads.
 */

import { Queue, Worker, FlowProducer } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function main() {
  console.log('=== ETL Pipeline Tests (TCP) ===\n');

  const flow = new FlowProducer({ connection: connOpts, useLocks: false });

  // ─────────────────────────────────────────────────
  // Test 1: Simple ETL chain (extract -> transform -> load)
  // ─────────────────────────────────────────────────
  console.log('1. Testing SIMPLE ETL CHAIN...');
  {
    const q = makeQueue('tcp-etl-simple');
    q.obliterate();
    await Bun.sleep(100);

    const executionOrder: string[] = [];

    const worker = new Worker(
      'tcp-etl-simple',
      async (job) => {
        const data = job.data as { stage: string };
        executionOrder.push(data.stage);
        await Bun.sleep(30);
        return { stage: data.stage };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    await flow.addChain([
      { name: 'extract', queueName: 'tcp-etl-simple', data: { stage: 'extract' } },
      { name: 'transform', queueName: 'tcp-etl-simple', data: { stage: 'transform' } },
      { name: 'load', queueName: 'tcp-etl-simple', data: { stage: 'load' } },
    ]);

    for (let i = 0; i < 80; i++) {
      if (executionOrder.length >= 3) break;
      await Bun.sleep(100);
    }

    if (
      executionOrder.length === 3 &&
      executionOrder[0] === 'extract' &&
      executionOrder[1] === 'transform' &&
      executionOrder[2] === 'load'
    ) {
      ok(`Simple ETL chain: ${executionOrder.join(' -> ')}`);
    } else {
      fail(`Expected [extract, transform, load], got [${executionOrder.join(', ')}]`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 2: Fan-out ETL (3 parallel transforms -> 1 load)
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing FAN-OUT ETL...');
  {
    const q = makeQueue('tcp-etl-fanout');
    q.obliterate();
    await Bun.sleep(100);

    const executionOrder: string[] = [];

    const worker = new Worker(
      'tcp-etl-fanout',
      async (job) => {
        const data = job.data as { task: string };
        executionOrder.push(data.task);
        await Bun.sleep(30);
        return { task: data.task };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    const result = await flow.addBulkThen(
      [
        { name: 'transform-a', queueName: 'tcp-etl-fanout', data: { task: 'transform-a' } },
        { name: 'transform-b', queueName: 'tcp-etl-fanout', data: { task: 'transform-b' } },
        { name: 'transform-c', queueName: 'tcp-etl-fanout', data: { task: 'transform-c' } },
      ],
      { name: 'load', queueName: 'tcp-etl-fanout', data: { task: 'load' } }
    );

    if (!result.finalId || result.parallelIds.length !== 3) {
      fail(`Bad structure: ${result.parallelIds.length} parallel, final=${!!result.finalId}`);
    } else {
      for (let i = 0; i < 80; i++) {
        if (executionOrder.length >= 4) break;
        await Bun.sleep(100);
      }

      const transforms = executionOrder.filter((t) => t.startsWith('transform-'));
      const loadIdx = executionOrder.indexOf('load');

      if (transforms.length === 3 && loadIdx === executionOrder.length - 1) {
        ok(`Fan-out ETL: ${executionOrder.join(', ')} (load last)`);
      } else {
        fail(`Order wrong: [${executionOrder.join(', ')}]`);
      }
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 3: Multi-record pipeline (20 records x 3 steps)
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing MULTI-RECORD PIPELINE (20 records)...');
  {
    const q = makeQueue('tcp-etl-multi');
    q.obliterate();
    await Bun.sleep(100);

    const RECORD_COUNT = 20;
    const completedRecords = new Set<number>();

    const worker = new Worker(
      'tcp-etl-multi',
      async (job) => {
        const data = job.data as { recordId: number; stage: string };
        if (data.stage === 'store') {
          completedRecords.add(data.recordId);
        }
        return { recordId: data.recordId, stage: data.stage };
      },
      { concurrency: 10, connection: connOpts, useLocks: false }
    );

    for (let i = 0; i < RECORD_COUNT; i++) {
      await flow.addChain([
        { name: `ingest-${i}`, queueName: 'tcp-etl-multi', data: { recordId: i, stage: 'ingest' } },
        { name: `enrich-${i}`, queueName: 'tcp-etl-multi', data: { recordId: i, stage: 'enrich' } },
        { name: `store-${i}`, queueName: 'tcp-etl-multi', data: { recordId: i, stage: 'store' } },
      ]);
    }

    for (let i = 0; i < 200; i++) {
      if (completedRecords.size >= RECORD_COUNT) break;
      await Bun.sleep(100);
    }

    if (completedRecords.size === RECORD_COUNT) {
      ok(`All ${RECORD_COUNT} records completed through 3-step pipeline`);
    } else {
      fail(`Only ${completedRecords.size}/${RECORD_COUNT} records completed`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 4: ETL with validation (7 valid, 3 invalid)
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing ETL WITH VALIDATION...');
  {
    const q = makeQueue('tcp-etl-validate');
    q.obliterate();
    await Bun.sleep(100);

    const completedIds: number[] = [];
    const failedIds: number[] = [];

    const worker = new Worker(
      'tcp-etl-validate',
      async (job) => {
        const data = job.data as { recordId: number; value: number };
        if (data.value < 0) {
          throw new Error(`Validation failed: record ${data.recordId}`);
        }
        return { recordId: data.recordId, result: data.value * 10 };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('completed', (job) => completedIds.push((job.data as any).recordId));
    worker.on('failed', (job) => failedIds.push((job.data as any).recordId));

    const records = [
      { recordId: 0, value: 100 },
      { recordId: 1, value: -1 },
      { recordId: 2, value: 200 },
      { recordId: 3, value: 300 },
      { recordId: 4, value: -5 },
      { recordId: 5, value: 400 },
      { recordId: 6, value: 500 },
      { recordId: 7, value: -10 },
      { recordId: 8, value: 600 },
      { recordId: 9, value: 700 },
    ];

    for (const rec of records) {
      await q.add(`validate-${rec.recordId}`, rec, { attempts: 1 });
    }

    for (let i = 0; i < 100; i++) {
      if (completedIds.length + failedIds.length >= 10) break;
      await Bun.sleep(100);
    }

    if (completedIds.length === 7 && failedIds.length === 3) {
      ok(`Validation: 7 passed, 3 failed correctly`);
    } else {
      fail(`Expected 7/3, got ${completedIds.length}/${failedIds.length}`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 5: Cascading ETL (5-step deep chain)
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing CASCADING 5-STEP ETL...');
  {
    const q = makeQueue('tcp-etl-cascade');
    q.obliterate();
    await Bun.sleep(100);

    const executionOrder: string[] = [];

    const worker = new Worker(
      'tcp-etl-cascade',
      async (job) => {
        const data = job.data as { stage: string };
        executionOrder.push(data.stage);
        await Bun.sleep(20);
        return { stage: data.stage };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    await flow.addChain([
      { name: 'ingest', queueName: 'tcp-etl-cascade', data: { stage: 'ingest' } },
      { name: 'validate', queueName: 'tcp-etl-cascade', data: { stage: 'validate' } },
      { name: 'enrich', queueName: 'tcp-etl-cascade', data: { stage: 'enrich' } },
      { name: 'transform', queueName: 'tcp-etl-cascade', data: { stage: 'transform' } },
      { name: 'load', queueName: 'tcp-etl-cascade', data: { stage: 'load' } },
    ]);

    for (let i = 0; i < 100; i++) {
      if (executionOrder.length >= 5) break;
      await Bun.sleep(100);
    }

    if (
      executionOrder.length === 5 &&
      executionOrder[0] === 'ingest' &&
      executionOrder[1] === 'validate' &&
      executionOrder[2] === 'enrich' &&
      executionOrder[3] === 'transform' &&
      executionOrder[4] === 'load'
    ) {
      ok(`Cascading ETL: ${executionOrder.join(' -> ')}`);
    } else {
      fail(`Expected 5-step order, got [${executionOrder.join(', ')}]`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 6: Parallel ETL pipelines (5 independent chains)
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing PARALLEL ETL PIPELINES (5 chains)...');
  {
    const q = makeQueue('tcp-etl-parallel');
    q.obliterate();
    await Bun.sleep(100);

    const PIPELINE_COUNT = 5;
    const pipelineResults = new Map<number, string[]>();
    let completedPipelines = 0;

    const worker = new Worker(
      'tcp-etl-parallel',
      async (job) => {
        const data = job.data as { pipelineId: number; stage: string };
        if (!pipelineResults.has(data.pipelineId)) pipelineResults.set(data.pipelineId, []);
        pipelineResults.get(data.pipelineId)!.push(data.stage);
        if (data.stage === 'load') completedPipelines++;
        await Bun.sleep(20);
        return { ok: true };
      },
      { concurrency: 10, connection: connOpts, useLocks: false }
    );

    await Promise.all(
      Array.from({ length: PIPELINE_COUNT }, (_, i) =>
        flow.addChain([
          {
            name: `extract-${i}`,
            queueName: 'tcp-etl-parallel',
            data: { pipelineId: i, stage: 'extract' },
          },
          {
            name: `transform-${i}`,
            queueName: 'tcp-etl-parallel',
            data: { pipelineId: i, stage: 'transform' },
          },
          {
            name: `load-${i}`,
            queueName: 'tcp-etl-parallel',
            data: { pipelineId: i, stage: 'load' },
          },
        ])
      )
    );

    for (let i = 0; i < 200; i++) {
      if (completedPipelines >= PIPELINE_COUNT) break;
      await Bun.sleep(100);
    }

    if (completedPipelines === PIPELINE_COUNT) {
      ok(`All ${PIPELINE_COUNT} pipelines completed independently`);
    } else {
      fail(`Only ${completedPipelines}/${PIPELINE_COUNT} pipelines completed`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 7: ETL with retry (transform fails then succeeds)
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing ETL WITH RETRY...');
  {
    const q = makeQueue('tcp-etl-retry');
    q.obliterate();
    await Bun.sleep(100);

    let transformAttempts = 0;
    const executionOrder: string[] = [];

    const worker = new Worker(
      'tcp-etl-retry',
      async (job) => {
        const data = job.data as { stage: string };
        executionOrder.push(data.stage);

        if (data.stage === 'transform') {
          transformAttempts++;
          if (transformAttempts < 2) {
            throw new Error(`Transient error attempt ${transformAttempts}`);
          }
        }
        return { stage: data.stage };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    await flow.addChain([
      { name: 'extract', queueName: 'tcp-etl-retry', data: { stage: 'extract' } },
      {
        name: 'transform',
        queueName: 'tcp-etl-retry',
        data: { stage: 'transform' },
        opts: { attempts: 3, backoff: 100 },
      },
      { name: 'load', queueName: 'tcp-etl-retry', data: { stage: 'load' } },
    ]);

    for (let i = 0; i < 100; i++) {
      if (executionOrder.includes('load')) break;
      await Bun.sleep(100);
    }

    if (transformAttempts >= 2 && executionOrder.includes('load')) {
      ok(
        `Transform retried ${transformAttempts} times, chain completed: ${executionOrder.join(' -> ')}`
      );
    } else {
      fail(`Transform attempts: ${transformAttempts}, order: [${executionOrder.join(', ')}]`);
    }

    await worker.close();
  }

  // ─── Cleanup ───
  await flow.close();
  for (const q of queues) {
    q.obliterate();
    q.close();
  }

  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
