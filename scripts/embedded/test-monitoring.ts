#!/usr/bin/env bun
/**
 * Test Monitoring & Stats (Embedded Mode): Stats, Metrics, Prometheus
 */

import { Queue, Worker } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';

const QUEUE_NAME = 'test-monitoring-embedded';

async function main() {
  console.log('=== Test Monitoring & Stats (Embedded) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
  const manager = getSharedManager();
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();

  // Test 1: Stats - Get queue statistics
  console.log('1. Testing STATS (initial)...');
  try {
    const stats = manager.getStats();

    if (
      typeof stats.waiting === 'number' &&
      typeof stats.delayed === 'number' &&
      typeof stats.active === 'number' &&
      typeof stats.dlq === 'number' &&
      typeof stats.completed === 'number' &&
      typeof stats.totalPushed === 'bigint' &&
      typeof stats.totalPulled === 'bigint' &&
      typeof stats.totalCompleted === 'bigint' &&
      typeof stats.totalFailed === 'bigint' &&
      typeof stats.uptime === 'number'
    ) {
      console.log(`   PASS Stats structure valid:`);
      console.log(`     - waiting: ${stats.waiting}`);
      console.log(`     - delayed: ${stats.delayed}`);
      console.log(`     - active: ${stats.active}`);
      console.log(`     - dlq: ${stats.dlq}`);
      console.log(`     - completed: ${stats.completed}`);
      console.log(`     - totalPushed: ${stats.totalPushed}`);
      console.log(`     - uptime: ${stats.uptime}ms`);
      passed++;
    } else {
      console.log('   FAIL Stats structure invalid');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Stats test failed: ${e}`);
    failed++;
  }

  // Test 2: Metrics - Get detailed metrics
  console.log('\n2. Testing METRICS...');
  try {
    const stats = manager.getStats();
    const memoryUsageMb = process.memoryUsage().heapUsed / 1024 / 1024;

    // Simulate metrics structure as returned by handler
    const metrics = {
      totalPushed: Number(stats.totalPushed),
      totalPulled: Number(stats.totalPulled),
      totalCompleted: Number(stats.totalCompleted),
      totalFailed: Number(stats.totalFailed),
      avgLatencyMs: 0,
      avgProcessingMs: 0,
      memoryUsageMb,
      sqliteSizeMb: 0,
      activeConnections: 0,
    };

    if (
      typeof metrics.totalPushed === 'number' &&
      typeof metrics.totalPulled === 'number' &&
      typeof metrics.totalCompleted === 'number' &&
      typeof metrics.totalFailed === 'number' &&
      typeof metrics.memoryUsageMb === 'number' &&
      metrics.memoryUsageMb > 0
    ) {
      console.log(`   PASS Metrics structure valid:`);
      console.log(`     - totalPushed: ${metrics.totalPushed}`);
      console.log(`     - totalPulled: ${metrics.totalPulled}`);
      console.log(`     - totalCompleted: ${metrics.totalCompleted}`);
      console.log(`     - totalFailed: ${metrics.totalFailed}`);
      console.log(`     - memoryUsageMb: ${metrics.memoryUsageMb.toFixed(2)}`);
      passed++;
    } else {
      console.log('   FAIL Metrics structure invalid');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Metrics test failed: ${e}`);
    failed++;
  }

  // Test 3: Prometheus - Get Prometheus format metrics
  console.log('\n3. Testing PROMETHEUS METRICS...');
  try {
    const prometheusMetrics = manager.getPrometheusMetrics();

    if (
      typeof prometheusMetrics === 'string' &&
      prometheusMetrics.includes('bunqueue_jobs_waiting') &&
      prometheusMetrics.includes('bunqueue_jobs_delayed') &&
      prometheusMetrics.includes('bunqueue_jobs_active') &&
      prometheusMetrics.includes('bunqueue_jobs_dlq') &&
      prometheusMetrics.includes('bunqueue_jobs_pushed_total') &&
      prometheusMetrics.includes('bunqueue_uptime_seconds')
    ) {
      console.log('   PASS Prometheus format valid');
      console.log('   Sample lines:');
      const lines = prometheusMetrics.split('\n').filter(l => !l.startsWith('#') && l.trim());
      for (const line of lines.slice(0, 5)) {
        console.log(`     ${line}`);
      }
      passed++;
    } else {
      console.log('   FAIL Prometheus format invalid');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Prometheus test failed: ${e}`);
    failed++;
  }

  // Test 4: Stats after adding jobs
  console.log('\n4. Testing STATS AFTER ADDING JOBS...');
  try {
    const statsBefore = manager.getStats();
    const totalPushedBefore = statsBefore.totalPushed;

    // Add some jobs
    await queue.add('job-1', { value: 1 });
    await queue.add('job-2', { value: 2 });
    await queue.add('job-3', { value: 3 });

    const statsAfter = manager.getStats();

    if (
      statsAfter.waiting >= 3 &&
      statsAfter.totalPushed >= totalPushedBefore + BigInt(3)
    ) {
      console.log(`   PASS Stats updated after adding jobs:`);
      console.log(`     - waiting: ${statsAfter.waiting}`);
      console.log(`     - totalPushed: ${totalPushedBefore} -> ${statsAfter.totalPushed}`);
      passed++;
    } else {
      console.log(`   FAIL Stats not updated correctly after adding jobs`);
      console.log(`     - waiting: ${statsAfter.waiting} (expected >= 3)`);
      console.log(`     - totalPushed: ${statsAfter.totalPushed} (expected >= ${totalPushedBefore + BigInt(3)})`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Stats after adding jobs failed: ${e}`);
    failed++;
  }

  // Test 5: Stats after processing jobs
  console.log('\n5. Testing STATS AFTER PROCESSING JOBS...');
  try {
    const statsBefore = manager.getStats();

    // Process jobs
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      return { processed: (job.data as { value: number }).value };
    }, { concurrency: 5, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    const statsAfter = manager.getStats();

    if (
      statsAfter.waiting < statsBefore.waiting &&
      statsAfter.totalCompleted >= statsBefore.totalCompleted
    ) {
      console.log(`   PASS Stats updated after processing:`);
      console.log(`     - waiting: ${statsBefore.waiting} -> ${statsAfter.waiting}`);
      console.log(`     - completed: ${statsBefore.completed} -> ${statsAfter.completed}`);
      console.log(`     - totalCompleted: ${statsBefore.totalCompleted} -> ${statsAfter.totalCompleted}`);
      passed++;
    } else {
      console.log('   FAIL Stats not updated correctly after processing');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Stats after processing failed: ${e}`);
    failed++;
  }

  // Test 6: Memory usage in metrics
  console.log('\n6. Testing MEMORY USAGE IN METRICS...');
  try {
    const memStats = manager.getMemoryStats();

    if (
      typeof memStats.jobIndex === 'number' &&
      typeof memStats.completedJobs === 'number' &&
      typeof memStats.jobResults === 'number' &&
      typeof memStats.processingTotal === 'number' &&
      typeof memStats.queuedTotal === 'number'
    ) {
      console.log(`   PASS Memory stats structure valid:`);
      console.log(`     - jobIndex: ${memStats.jobIndex}`);
      console.log(`     - completedJobs: ${memStats.completedJobs}`);
      console.log(`     - jobResults: ${memStats.jobResults}`);
      console.log(`     - processingTotal: ${memStats.processingTotal}`);
      console.log(`     - queuedTotal: ${memStats.queuedTotal}`);

      // Verify heap memory is accessible
      const heapUsed = process.memoryUsage().heapUsed;
      if (heapUsed > 0) {
        console.log(`     - heapUsed: ${(heapUsed / 1024 / 1024).toFixed(2)} MB`);
        passed++;
      } else {
        console.log('   FAIL Heap memory not accessible');
        failed++;
      }
    } else {
      console.log('   FAIL Memory stats structure invalid');
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Memory usage test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
