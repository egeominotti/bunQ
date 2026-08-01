#!/usr/bin/env bun
/**
 * Advanced namespace-isolation stress tests over TCP.
 * The QueueGroup API contract itself is exercised in test-queue-group.ts.
 */

import { Queue, Worker } from '../../src/client';

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

function makeQueue<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function test1_namespaceIsolation() {
  console.log('1. Namespace prefix isolation...');

  const billingInvoices = makeQueue<{ type: string }>('tcp-grp-billing:invoices');
  const shippingOrders = makeQueue<{ type: string }>('tcp-grp-shipping:orders');

  await Promise.all([billingInvoices.obliterateAsync(), shippingOrders.obliterateAsync()]);

  await billingInvoices.add('inv', { type: 'billing-invoice' });
  await billingInvoices.add('inv', { type: 'billing-invoice-2' });
  await shippingOrders.add('ord', { type: 'shipping-order' });
  await shippingOrders.add('ord', { type: 'shipping-order-2' });

  const billingReceived: string[] = [];
  const shippingReceived: string[] = [];

  const w1 = new Worker<{ type: string }>(
    'tcp-grp-billing:invoices',
    async (job) => {
      billingReceived.push(job.data.type);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  const w2 = new Worker<{ type: string }>(
    'tcp-grp-shipping:orders',
    async (job) => {
      shippingReceived.push(job.data.type);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  for (let i = 0; i < 30; i++) await Bun.sleep(100);

  await w1.close();
  await w2.close();

  if (
    billingReceived.length === 2 &&
    billingReceived.every((t) => t.startsWith('billing-')) &&
    shippingReceived.length === 2 &&
    shippingReceived.every((t) => t.startsWith('shipping-'))
  ) {
    ok(
      `Billing got ${billingReceived.length} jobs, shipping got ${shippingReceived.length} jobs - isolation verified`
    );
  } else {
    fail(`Billing: [${billingReceived.join(', ')}], Shipping: [${shippingReceived.join(', ')}]`);
  }
}

async function test2_multiplePrefixedQueues() {
  console.log('\n2. Multiple prefixed queues under same group...');

  const q1 = makeQueue<{ idx: number }>('tcp-grp-multi:alpha');
  const q2 = makeQueue<{ idx: number }>('tcp-grp-multi:beta');
  const q3 = makeQueue<{ idx: number }>('tcp-grp-multi:gamma');

  await Promise.all([q1.obliterateAsync(), q2.obliterateAsync(), q3.obliterateAsync()]);

  await q1.add('task', { idx: 1 });
  await q1.add('task', { idx: 2 });
  await q2.add('task', { idx: 10 });
  await q2.add('task', { idx: 20 });
  await q3.add('task', { idx: 100 });
  await q3.add('task', { idx: 200 });

  const alphaResults: number[] = [];
  const betaResults: number[] = [];
  const gammaResults: number[] = [];

  const w1 = new Worker<{ idx: number }>(
    'tcp-grp-multi:alpha',
    async (job) => {
      alphaResults.push(job.data.idx);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  const w2 = new Worker<{ idx: number }>(
    'tcp-grp-multi:beta',
    async (job) => {
      betaResults.push(job.data.idx);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  const w3 = new Worker<{ idx: number }>(
    'tcp-grp-multi:gamma',
    async (job) => {
      gammaResults.push(job.data.idx);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  for (let i = 0; i < 30; i++) await Bun.sleep(100);

  await w1.close();
  await w2.close();
  await w3.close();

  if (
    alphaResults.length === 2 &&
    betaResults.length === 2 &&
    gammaResults.length === 2 &&
    alphaResults.sort((a, b) => a - b).join(',') === '1,2' &&
    betaResults.sort((a, b) => a - b).join(',') === '10,20' &&
    gammaResults.sort((a, b) => a - b).join(',') === '100,200'
  ) {
    ok('All 3 prefixed queues processed independently');
  } else {
    fail(`Alpha: [${alphaResults}], Beta: [${betaResults}], Gamma: [${gammaResults}]`);
  }
}

async function test3_crossGroupNoInterference() {
  console.log('\n3. Cross-group no interference...');

  const qA = makeQueue<{ src: string }>('tcp-grp-a:q1');
  const qB = makeQueue<{ src: string }>('tcp-grp-b:q1');

  await Promise.all([qA.obliterateAsync(), qB.obliterateAsync()]);

  // Push only to group A
  await qA.add('task', { src: 'group-a-job-1' });
  await qA.add('task', { src: 'group-a-job-2' });

  const bReceived: string[] = [];

  // Worker only on group B - should NOT receive group A jobs
  const wB = new Worker<{ src: string }>(
    'tcp-grp-b:q1',
    async (job) => {
      bReceived.push(job.data.src);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  for (let i = 0; i < 20; i++) await Bun.sleep(100);

  await wB.close();

  // Group B worker should have received nothing
  if (bReceived.length === 0) {
    ok('Worker on grp-b:q1 did not receive grp-a:q1 jobs');
  } else {
    fail(`grp-b worker unexpectedly received: [${bReceived.join(', ')}]`);
  }

  // Verify group A jobs are untouched (waiting or still in queue)
  const countsA = await qA.getJobCountsAsync();
  if (countsA.waiting === 2 && bReceived.length === 0) {
    ok(`grp-a:q1 jobs not consumed by grp-b worker (waiting=${countsA.waiting})`);
  } else {
    fail(`Unexpected state: waiting=${countsA.waiting}, bReceived=${bReceived.length}`);
  }
}

async function test4_prefixedQueuePauseResume() {
  console.log('\n4. Prefixed queue pause/resume...');

  const q1 = makeQueue<{ idx: number }>('tcp-grp-pr:q1');
  const q2 = makeQueue<{ idx: number }>('tcp-grp-pr:q2');

  await Promise.all([q1.obliterateAsync(), q2.obliterateAsync()]);

  await q1.add('task', { idx: 1 });
  await q2.add('task', { idx: 2 });

  // Pause only q1
  await q1.pauseAsync();

  const q1Received: number[] = [];
  const q2Received: number[] = [];

  const w1 = new Worker<{ idx: number }>(
    'tcp-grp-pr:q1',
    async (job) => {
      q1Received.push(job.data.idx);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  const w2 = new Worker<{ idx: number }>(
    'tcp-grp-pr:q2',
    async (job) => {
      q2Received.push(job.data.idx);
      return {};
    },
    { connection: connOpts, useLocks: false }
  );

  // Wait for q2 to process while q1 stays paused
  for (let i = 0; i < 20; i++) await Bun.sleep(100);

  // q2 should have processed, q1 should not
  if (q2Received.length === 1 && q2Received[0] === 2) {
    ok('Unpaused queue (q2) processed its job');
  } else {
    fail(`q2 received: [${q2Received.join(', ')}]`);
  }

  if (q1Received.length === 0) {
    ok('Paused queue (q1) did not process');
  } else {
    fail(`q1 unexpectedly received: [${q1Received.join(', ')}]`);
  }

  // Resume q1 and verify it processes
  await q1.resumeAsync();

  for (let i = 0; i < 20; i++) await Bun.sleep(100);

  await w1.close();
  await w2.close();

  if (q1Received.length === 1 && q1Received[0] === 1) {
    ok('Resumed queue (q1) processed its job after resume');
  } else {
    fail(`After resume, q1 received: [${q1Received.join(', ')}]`);
  }
}

async function test5_highVolumeAcrossGroups() {
  console.log('\n5. High volume across groups (3 groups x 2 queues x 20 jobs = 120 jobs)...');

  const groupNames = ['tcp-grp-hv-x', 'tcp-grp-hv-y', 'tcp-grp-hv-z'];
  const queueSuffixes = ['fast', 'slow'];
  const allQueues: Queue<{ group: string; queue: string; idx: number }>[] = [];
  const allWorkers: Worker<{ group: string; queue: string; idx: number }>[] = [];

  // Track results per group:queue
  const results: Record<string, number[]> = {};

  // Create queues, obliterate, and add jobs
  for (const grp of groupNames) {
    for (const suf of queueSuffixes) {
      const fullName = `${grp}:${suf}`;
      results[fullName] = [];

      const q = makeQueue<{ group: string; queue: string; idx: number }>(fullName);
      allQueues.push(q);
      await q.obliterateAsync();
    }
  }

  await Bun.sleep(300);

  // Add 20 jobs to each queue (120 total)
  for (const grp of groupNames) {
    for (const suf of queueSuffixes) {
      const fullName = `${grp}:${suf}`;
      // Use a new reference to add
      const addQueue = makeQueue<{ group: string; queue: string; idx: number }>(fullName);
      for (let i = 0; i < 20; i++) {
        await addQueue.add('task', { group: grp, queue: suf, idx: i });
      }
    }
  }

  // Create workers
  for (const grp of groupNames) {
    for (const suf of queueSuffixes) {
      const fullName = `${grp}:${suf}`;
      const w = new Worker<{ group: string; queue: string; idx: number }>(
        fullName,
        async (job) => {
          results[fullName].push(job.data.idx);
          return {};
        },
        { connection: connOpts, concurrency: 5, useLocks: false }
      );
      allWorkers.push(w);
    }
  }

  // Wait for processing
  for (let i = 0; i < 50; i++) await Bun.sleep(100);

  // Close all workers
  for (const w of allWorkers) {
    await w.close();
  }

  // Verify results
  let totalProcessed = 0;
  let allCorrect = true;

  for (const grp of groupNames) {
    for (const suf of queueSuffixes) {
      const fullName = `${grp}:${suf}`;
      const count = results[fullName].length;
      totalProcessed += count;
      if (count !== 20) {
        allCorrect = false;
        fail(`${fullName} processed ${count}/20 jobs`);
      }
    }
  }

  if (allCorrect && totalProcessed === 120) {
    ok(`All 120 jobs processed correctly across 6 queues (${totalProcessed} total)`);
  } else if (allCorrect) {
    fail(`Total processed: ${totalProcessed}/120`);
  }
}

async function main() {
  console.log('=== QueueGroup Advanced Tests (TCP) ===\n');

  await test1_namespaceIsolation();
  await test2_multiplePrefixedQueues();
  await test3_crossGroupNoInterference();
  await test4_prefixedQueuePauseResume();
  await test5_highVolumeAcrossGroups();

  // Cleanup
  for (const q of queues) {
    await q.obliterateAsync();
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
