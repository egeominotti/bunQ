#!/usr/bin/env bun
/**
 * Test Server-side Cron Jobs (Embedded Mode)
 * Tests the CronScheduler via getSharedManager()
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';
import { getSharedManager, shutdownManager } from '../../src/client/manager';

const QUEUE_NAME = 'test-cron-server';

async function main() {
  console.log('=== Test Server-side Cron Jobs (Embedded) ===\n');

  const queue = new Queue<{ type: string }>(QUEUE_NAME, { embedded: true });
  const manager = getSharedManager();
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();
  // Remove any existing crons from previous test runs
  for (const cron of manager.listCrons()) {
    manager.removeCron(cron.name);
  }

  // Test 1: Add cron job with cron expression (every 5 seconds)
  console.log('1. Testing CRON WITH CRON EXPRESSION...');
  try {
    const cron = manager.addCron({
      name: 'cron-expr-test',
      queue: QUEUE_NAME,
      data: { type: 'cron-expression' },
      schedule: '*/5 * * * * *', // Every 5 seconds (6-part cron with seconds)
    });

    if (cron.name === 'cron-expr-test' && cron.schedule === '*/5 * * * * *' && cron.nextRun > Date.now()) {
      console.log(`   ✅ Cron job added with schedule, nextRun: ${new Date(cron.nextRun).toISOString()}`);
      passed++;
    } else {
      console.log(`   ❌ Cron job not created correctly: ${JSON.stringify(cron)}`);
      failed++;
    }
    manager.removeCron('cron-expr-test');
  } catch (e) {
    console.log(`   ❌ Cron expression test failed: ${e}`);
    failed++;
  }

  // Test 2: Add cron job with repeatEvery (ms interval)
  console.log('\n2. Testing CRON WITH REPEAT EVERY...');
  try {
    const cron = manager.addCron({
      name: 'repeat-every-test',
      queue: QUEUE_NAME,
      data: { type: 'interval' },
      repeatEvery: 5000, // Every 5 seconds
    });

    if (cron.name === 'repeat-every-test' && cron.repeatEvery === 5000) {
      console.log(`   ✅ Cron job added with repeatEvery: ${cron.repeatEvery}ms`);
      passed++;
    } else {
      console.log(`   ❌ Cron job repeatEvery not set correctly: ${JSON.stringify(cron)}`);
      failed++;
    }
    manager.removeCron('repeat-every-test');
  } catch (e) {
    console.log(`   ❌ RepeatEvery test failed: ${e}`);
    failed++;
  }

  // Test 3: CronList - List all cron jobs
  console.log('\n3. Testing CRON LIST...');
  try {
    // Add a few cron jobs
    manager.addCron({
      name: 'list-test-1',
      queue: QUEUE_NAME,
      data: { type: 'list1' },
      repeatEvery: 10000,
    });
    manager.addCron({
      name: 'list-test-2',
      queue: QUEUE_NAME,
      data: { type: 'list2' },
      schedule: '0 * * * *', // Every hour
    });

    const cronList = manager.listCrons();
    const names = cronList.map(c => c.name);

    if (names.includes('list-test-1') && names.includes('list-test-2')) {
      console.log(`   ✅ CronList returned ${cronList.length} cron jobs: ${names.join(', ')}`);
      passed++;
    } else {
      console.log(`   ❌ CronList missing expected crons: ${names.join(', ')}`);
      failed++;
    }

    // Clean up
    manager.removeCron('list-test-1');
    manager.removeCron('list-test-2');
  } catch (e) {
    console.log(`   ❌ CronList test failed: ${e}`);
    failed++;
  }

  // Test 4: CronDelete - Delete a cron job
  console.log('\n4. Testing CRON DELETE...');
  try {
    manager.addCron({
      name: 'to-delete',
      queue: QUEUE_NAME,
      data: { type: 'delete' },
      repeatEvery: 10000,
    });

    // Verify it exists
    const existsBefore = manager.getCron('to-delete');
    if (!existsBefore) {
      throw new Error('Cron job was not created');
    }

    // Delete it
    const removed = manager.removeCron('to-delete');

    // Verify it's gone
    const existsAfter = manager.getCron('to-delete');

    if (removed && !existsAfter) {
      console.log('   ✅ Cron job deleted successfully');
      passed++;
    } else {
      console.log(`   ❌ Cron job not deleted: removed=${removed}, exists=${!!existsAfter}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ CronDelete test failed: ${e}`);
    failed++;
  }

  // Test 5: Cron with priority - Cron job creates jobs with specific priority
  console.log('\n5. Testing CRON WITH PRIORITY...');
  try {
    const cron = manager.addCron({
      name: 'priority-cron',
      queue: QUEUE_NAME,
      data: { type: 'high-priority' },
      repeatEvery: 200, // Fast interval for testing
      priority: 100,
    });

    if (cron.priority === 100) {
      console.log(`   ✅ Cron job added with priority: ${cron.priority}`);
      passed++;
    } else {
      console.log(`   ❌ Cron job priority not set: ${cron.priority}`);
      failed++;
    }

    manager.removeCron('priority-cron');
  } catch (e) {
    console.log(`   ❌ Priority cron test failed: ${e}`);
    failed++;
  }

  // Test 6: Cron with timezone - Test timezone parameter
  console.log('\n6. Testing CRON WITH TIMEZONE...');
  try {
    const cron = manager.addCron({
      name: 'timezone-cron',
      queue: QUEUE_NAME,
      data: { type: 'timezone' },
      schedule: '0 9 * * *', // 9am daily
      timezone: 'Europe/Rome',
    });

    if (cron.timezone === 'Europe/Rome') {
      console.log(`   ✅ Cron job added with timezone: ${cron.timezone}`);

      // Verify the nextRun is adjusted for timezone
      const nextRunDate = new Date(cron.nextRun);
      console.log(`      Next run: ${nextRunDate.toISOString()}`);
      passed++;
    } else {
      console.log(`   ❌ Cron job timezone not set: ${cron.timezone}`);
      failed++;
    }

    manager.removeCron('timezone-cron');
  } catch (e) {
    console.log(`   ❌ Timezone cron test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();
  for (const cron of manager.listCrons()) {
    if (cron.queue === QUEUE_NAME) {
      manager.removeCron(cron.name);
    }
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
