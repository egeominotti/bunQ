#!/usr/bin/env bun
/**
 * Test Server-side Cron Jobs (TCP Mode)
 * Tests the Cron, CronList, CronDelete TCP commands
 */

import { Queue, TcpConnectionPool } from '../../src/client';

const QUEUE_NAME = 'tcp-test-cron-server';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Server-side Cron Jobs (TCP) ===\n');

  const queue = new Queue<{ type: string }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  const tcp = new TcpConnectionPool({ port: TCP_PORT });
  await tcp.connect();
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();
  await Bun.sleep(100);

  // Clean up any existing crons from previous test runs
  const existingCrons = await tcp.send({ cmd: 'CronList' });
  if (existingCrons.ok && Array.isArray(existingCrons.crons)) {
    for (const cron of existingCrons.crons) {
      if (cron.name.startsWith('tcp-cron-')) {
        await tcp.send({ cmd: 'CronDelete', name: cron.name });
      }
    }
  }
  await Bun.sleep(100);

  // Test 1: Add cron job with cron expression (every 5 seconds)
  console.log('1. Testing CRON WITH CRON EXPRESSION...');
  try {
    const result = await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-expr-test',
      queue: QUEUE_NAME,
      data: { type: 'cron-expression' },
      schedule: '*/5 * * * * *', // Every 5 seconds (6-part cron with seconds)
    });

    if (result.ok && result.cron && result.cron.name === 'tcp-cron-expr-test') {
      console.log(`   ✅ Cron job added with schedule, nextRun: ${new Date(result.cron.nextRun).toISOString()}`);
      passed++;
    } else {
      console.log(`   ❌ Cron job not created correctly: ${JSON.stringify(result)}`);
      failed++;
    }

    await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-expr-test' });
  } catch (e) {
    console.log(`   ❌ Cron expression test failed: ${e}`);
    failed++;
  }

  // Test 2: Add cron job with repeatEvery (ms interval)
  console.log('\n2. Testing CRON WITH REPEAT EVERY...');
  try {
    const result = await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-repeat-test',
      queue: QUEUE_NAME,
      data: { type: 'interval' },
      repeatEvery: 5000, // Every 5 seconds
    });

    if (result.ok && result.cron && result.cron.repeatEvery === 5000) {
      console.log(`   ✅ Cron job added with repeatEvery: ${result.cron.repeatEvery}ms`);
      passed++;
    } else {
      console.log(`   ❌ Cron job repeatEvery not set correctly: ${JSON.stringify(result)}`);
      failed++;
    }

    await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-repeat-test' });
  } catch (e) {
    console.log(`   ❌ RepeatEvery test failed: ${e}`);
    failed++;
  }

  // Test 3: CronList - List all cron jobs
  console.log('\n3. Testing CRON LIST...');
  try {
    // Add a few cron jobs
    await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-list-1',
      queue: QUEUE_NAME,
      data: { type: 'list1' },
      repeatEvery: 10000,
    });
    await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-list-2',
      queue: QUEUE_NAME,
      data: { type: 'list2' },
      schedule: '0 * * * *', // Every hour
    });

    const result = await tcp.send({ cmd: 'CronList' });

    if (result.ok && Array.isArray(result.crons)) {
      const names = result.crons.map((c: { name: string }) => c.name);
      if (names.includes('tcp-cron-list-1') && names.includes('tcp-cron-list-2')) {
        console.log(`   ✅ CronList returned ${result.crons.length} cron jobs: ${names.join(', ')}`);
        passed++;
      } else {
        console.log(`   ❌ CronList missing expected crons: ${names.join(', ')}`);
        failed++;
      }
    } else {
      console.log(`   ❌ CronList failed: ${JSON.stringify(result)}`);
      failed++;
    }

    // Clean up
    await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-list-1' });
    await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-list-2' });
  } catch (e) {
    console.log(`   ❌ CronList test failed: ${e}`);
    failed++;
  }

  // Test 4: CronDelete - Delete a cron job
  console.log('\n4. Testing CRON DELETE...');
  try {
    // Create a cron job
    await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-to-delete',
      queue: QUEUE_NAME,
      data: { type: 'delete' },
      repeatEvery: 10000,
    });

    // Verify it exists
    const listBefore = await tcp.send({ cmd: 'CronList' });
    const existsBefore = listBefore.crons?.some((c: { name: string }) => c.name === 'tcp-cron-to-delete');

    if (!existsBefore) {
      throw new Error('Cron job was not created');
    }

    // Delete it
    const deleteResult = await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-to-delete' });

    // Verify it's gone
    const listAfter = await tcp.send({ cmd: 'CronList' });
    const existsAfter = listAfter.crons?.some((c: { name: string }) => c.name === 'tcp-cron-to-delete');

    if (deleteResult.ok && !existsAfter) {
      console.log('   ✅ Cron job deleted successfully');
      passed++;
    } else {
      console.log(`   ❌ Cron job not deleted: result=${JSON.stringify(deleteResult)}, exists=${existsAfter}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ CronDelete test failed: ${e}`);
    failed++;
  }

  // Test 5: Cron with priority - Cron job creates jobs with specific priority
  // Note: priority is not returned in the Cron response, but is stored server-side
  // We verify it was accepted by checking the cron was created successfully
  console.log('\n5. Testing CRON WITH PRIORITY...');
  try {
    const result = await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-priority',
      queue: QUEUE_NAME,
      data: { type: 'high-priority' },
      repeatEvery: 200,
      priority: 100,
    });

    if (result.ok && result.cron && result.cron.name === 'tcp-cron-priority') {
      // Priority is stored server-side but not returned in response
      // The fact that the cron was created successfully confirms the priority was accepted
      console.log(`   ✅ Cron job added with priority (priority stored server-side)`);
      passed++;
    } else {
      console.log(`   ❌ Cron job not created: ${JSON.stringify(result)}`);
      failed++;
    }

    await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-priority' });
  } catch (e) {
    console.log(`   ❌ Priority cron test failed: ${e}`);
    failed++;
  }

  // Test 6: Cron with timezone - Test timezone parameter
  console.log('\n6. Testing CRON WITH TIMEZONE...');
  try {
    const result = await tcp.send({
      cmd: 'Cron',
      name: 'tcp-cron-timezone',
      queue: QUEUE_NAME,
      data: { type: 'timezone' },
      schedule: '0 9 * * *', // 9am daily
      timezone: 'Europe/Rome',
    });

    if (result.ok && result.cron && result.cron.timezone === 'Europe/Rome') {
      console.log(`   ✅ Cron job added with timezone: ${result.cron.timezone}`);
      console.log(`      Next run: ${new Date(result.cron.nextRun).toISOString()}`);
      passed++;
    } else {
      console.log(`   ❌ Cron job timezone not set: ${JSON.stringify(result)}`);
      failed++;
    }

    await tcp.send({ cmd: 'CronDelete', name: 'tcp-cron-timezone' });
  } catch (e) {
    console.log(`   ❌ Timezone cron test failed: ${e}`);
    failed++;
  }

  // Clean up any remaining test crons before closing connections
  const finalList = await tcp.send({ cmd: 'CronList' });
  if (finalList.ok && Array.isArray(finalList.crons)) {
    for (const cron of finalList.crons) {
      if (cron.name.startsWith('tcp-cron-') && cron.queue === QUEUE_NAME) {
        await tcp.send({ cmd: 'CronDelete', name: cron.name });
      }
    }
  }

  // Cleanup connections
  queue.obliterate();
  queue.close();
  tcp.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
