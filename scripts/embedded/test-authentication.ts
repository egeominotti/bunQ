#!/usr/bin/env bun
/**
 * Test Authentication (Embedded Mode)
 *
 * In embedded mode, authentication is typically not enforced because
 * the client has direct access to the in-process queue manager.
 * These tests verify the authentication API exists and behaves correctly.
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-authentication';

async function main() {
  console.log('=== Test Authentication (Embedded Mode) ===\n');
  console.log('Note: Embedded mode bypasses TCP authentication.\n');
  console.log('These tests verify API compatibility and basic behavior.\n');

  let passed = 0;
  let failed = 0;

  // Test 1: Queue creation without token works
  console.log('1. Testing QUEUE WITHOUT TOKEN...');
  try {
    const queue = new Queue<{ message: string }>(QUEUE_NAME);
    queue.obliterate();

    const job = await queue.add('test', { message: 'no token needed' });

    if (job.id) {
      console.log(`   [PASS] Queue works without token: job ${job.id}`);
      passed++;
    } else {
      console.log('   [FAIL] Job not created');
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   [FAIL] Queue creation failed: ${e}`);
    failed++;
  }

  // Test 2: Queue creation with token works (token is ignored in embedded mode)
  console.log('\n2. Testing QUEUE WITH TOKEN (embedded ignores token)...');
  try {
    const queue = new Queue<{ message: string }>(QUEUE_NAME, {
      embedded: true,
      connection: { token: 'my-test-token' },
    });
    queue.obliterate();

    const job = await queue.add('test', { message: 'with token' });

    if (job.id) {
      console.log(`   [PASS] Queue works with token (ignored): job ${job.id}`);
      passed++;
    } else {
      console.log('   [FAIL] Job not created');
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   [FAIL] Queue with token failed: ${e}`);
    failed++;
  }

  // Test 3: Worker processes jobs without authentication
  console.log('\n3. Testing WORKER WITHOUT AUTH...');
  try {
    const queue = new Queue<{ message: string }>(QUEUE_NAME);
    queue.obliterate();

    await queue.add('process-test', { message: 'process me' });

    let processed = false;
    const worker = new Worker<{ message: string }>(QUEUE_NAME, async (job) => {
      processed = true;
      return { success: true };
    }, { concurrency: 1 });

    await Bun.sleep(500);
    await worker.close();

    if (processed) {
      console.log('   [PASS] Worker processed job without auth');
      passed++;
    } else {
      console.log('   [FAIL] Job was not processed');
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   [FAIL] Worker test failed: ${e}`);
    failed++;
  }

  // Test 4: Multiple queues with different "tokens" work in embedded mode
  console.log('\n4. Testing MULTIPLE QUEUES WITH DIFFERENT TOKENS...');
  try {
    const queue1 = new Queue<{ value: number }>(`${QUEUE_NAME}-1`, {
      embedded: true,
      connection: { token: 'token-1' },
    });
    const queue2 = new Queue<{ value: number }>(`${QUEUE_NAME}-2`, {
      embedded: true,
      connection: { token: 'token-2' },
    });

    queue1.obliterate();
    queue2.obliterate();

    const job1 = await queue1.add('job1', { value: 1 });
    const job2 = await queue2.add('job2', { value: 2 });

    if (job1.id && job2.id) {
      console.log(`   [PASS] Both queues work: job1=${job1.id}, job2=${job2.id}`);
      passed++;
    } else {
      console.log('   [FAIL] Jobs not created');
      failed++;
    }

    queue1.obliterate();
    queue2.obliterate();
  } catch (e) {
    console.log(`   [FAIL] Multiple queues test failed: ${e}`);
    failed++;
  }

  // Test 5: Connection options are preserved
  console.log('\n5. Testing CONNECTION OPTIONS PRESERVED...');
  try {
    const connectionOpts = {
      host: 'ignored-host',
      port: 12345,
      token: 'test-token-123',
    };

    const queue = new Queue<{ data: string }>(QUEUE_NAME, {
      embedded: true,
      connection: connectionOpts,
    });
    queue.obliterate();

    // In embedded mode, these connection options are ignored but shouldn't cause errors
    const job = await queue.add('options-test', { data: 'test' });

    if (job.id) {
      console.log('   [PASS] Connection options accepted (ignored in embedded mode)');
      passed++;
    } else {
      console.log('   [FAIL] Job not created');
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   [FAIL] Connection options test failed: ${e}`);
    failed++;
  }

  // Test 6: Empty token works (equivalent to no auth)
  console.log('\n6. Testing EMPTY TOKEN...');
  try {
    const queue = new Queue<{ message: string }>(QUEUE_NAME, {
      embedded: true,
      connection: { token: '' },
    });
    queue.obliterate();

    const job = await queue.add('empty-token', { message: 'empty token test' });

    if (job.id) {
      console.log(`   [PASS] Empty token works: job ${job.id}`);
      passed++;
    } else {
      console.log('   [FAIL] Job not created');
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   [FAIL] Empty token test failed: ${e}`);
    failed++;
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  console.log('\nNote: In embedded mode, authentication is bypassed.');
  console.log('For TCP authentication tests, see scripts/tcp/test-authentication.ts');

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
