#!/usr/bin/env bun
/**
 * Worker Advanced Methods Tests (TCP Mode)
 * Tests: rateLimit, isRateLimited, startStalledCheckTimer, delay
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

async function main() {
  console.log('=== Worker Advanced Methods Tests (TCP) ===\n');

  const queueName = 'tcp-worker-advanced';

  // ─────────────────────────────────────────────────
  // Test 1: Worker has rateLimit method
  // ─────────────────────────────────────────────────
  console.log('1. Testing Worker has rateLimit method...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    if (typeof worker.rateLimit === 'function') {
      ok('rateLimit method exists');
    } else {
      fail('rateLimit is not a function');
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 2: rateLimit applies rate limiting
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing rateLimit APPLIES limiting...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    worker.rateLimit(1000);

    if (worker.isRateLimited()) {
      ok('Worker is rate limited after rateLimit(1000)');
    } else {
      fail('Worker should be rate limited');
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 3: isRateLimited returns false initially
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing isRateLimited FALSE initially...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    if (!worker.isRateLimited()) {
      ok('Worker is not rate limited initially');
    } else {
      fail('Worker should not be rate limited initially');
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 4: isRateLimited returns false after expiry
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing isRateLimited EXPIRES...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    worker.rateLimit(50);

    if (!worker.isRateLimited()) {
      fail('Worker should be rate limited immediately');
    } else {
      await Bun.sleep(100);
      if (!worker.isRateLimited()) {
        ok('Rate limit expired after 100ms');
      } else {
        fail('Rate limit should have expired');
      }
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 5: Worker has startStalledCheckTimer method
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing Worker has startStalledCheckTimer...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    if (typeof worker.startStalledCheckTimer === 'function') {
      ok('startStalledCheckTimer method exists');
    } else {
      fail('startStalledCheckTimer is not a function');
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 6: startStalledCheckTimer does not throw
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing startStalledCheckTimer NO THROW...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    try {
      await worker.startStalledCheckTimer();
      ok('startStalledCheckTimer completed without error');
    } catch (err) {
      fail(`startStalledCheckTimer threw: ${err}`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 7: Worker has delay method
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing Worker has delay method...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    if (typeof worker.delay === 'function') {
      ok('delay method exists');
    } else {
      fail('delay is not a function');
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 8: delay with 0 resolves immediately
  // ─────────────────────────────────────────────────
  console.log('\n8. Testing delay(0) resolves immediately...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    const start = Date.now();
    await worker.delay(0);
    const elapsed = Date.now() - start;

    if (elapsed <= 50) {
      ok(`delay(0) resolved in ${elapsed}ms`);
    } else {
      fail(`delay(0) took too long: ${elapsed}ms`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 9: delay waits for specified time
  // ─────────────────────────────────────────────────
  console.log('\n9. Testing delay(100) waits...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    const start = Date.now();
    await worker.delay(100);
    const elapsed = Date.now() - start;

    if (elapsed >= 90) {
      ok(`delay(100) waited ${elapsed}ms`);
    } else {
      fail(`delay(100) was too short: ${elapsed}ms`);
    }

    await worker.close();
  }

  // ─────────────────────────────────────────────────
  // Test 10: delay can be aborted
  // ─────────────────────────────────────────────────
  console.log('\n10. Testing delay ABORT...');
  {
    const worker = new Worker(queueName, async () => ({ done: true }), {
      connection: connOpts, autorun: false, useLocks: false,
    });

    const abortController = new AbortController();
    const delayPromise = worker.delay(1000, abortController);

    setTimeout(() => abortController.abort(), 50);

    try {
      await delayPromise;
      fail('Delay should have been aborted');
    } catch (err) {
      if (err instanceof Error && err.message.includes('aborted')) {
        ok('Delay aborted successfully');
      } else {
        fail(`Expected abort error, got: ${err}`);
      }
    }

    await worker.close();
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
